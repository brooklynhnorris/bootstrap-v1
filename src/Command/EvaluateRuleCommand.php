<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:evaluate-rule', description: 'Multi-LLM rule evaluation with 3-round deliberation + output consensus for DDT team')]
class EvaluateRuleCommand extends Command
{
    private const MAX_ROUNDS   = 3;
    private const ASSET_FILTER = "url NOT LIKE '%.pdf' AND url NOT LIKE '%.doc' AND url NOT LIKE '%.docx' AND url NOT LIKE '%.xls' AND url NOT LIKE '%.xlsx' AND url NOT LIKE '%.jpg' AND url NOT LIKE '%.jpeg' AND url NOT LIKE '%.png' AND url NOT LIKE '%.zip'";
    private const TIER4_URLS   = "'/contact-us/','/get-quote/','/trailer-finder/','/book-a-video-call/','/join-our-mailing-list/','/freebook/','/horse-trailer-safety-webinars/','/virtual-horse-trailer-safety-inspection/'";

    // Team roster — used in output consensus prompts
    private const TEAM = [
        'Brook'  => 'SEO + Content — on-page fixes, FC rule violations, content rewrites',
        'Brad'   => 'Developer — schema deployment, redirects, canonicals, technical fixes',
        'Kalib'  => 'Design — UX, conversion path, page layout, CTA design',
        'Jeanne' => 'Owner — rule review/approval, classification decisions, QA of AI findings',
    ];

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('rule',        null, InputOption::VALUE_OPTIONAL, 'Specific rule ID (e.g. OPQ-001). Omit to evaluate all firing rules.')
            ->addOption('dry-run',     null, InputOption::VALUE_NONE,     'Show prompts without calling APIs')
            ->addOption('verbose-llm', null, InputOption::VALUE_NONE,     'Show full LLM responses per round')
            ->addOption('skip-output', null, InputOption::VALUE_NONE,     'Skip Stage 2 output consensus (run rule validation only)')
            ->addOption('skip-validation', null, InputOption::VALUE_NONE, 'Skip Stage 1 validation (rules are pre-validated, go straight to play briefs)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $ruleFilter      = $input->getOption('rule');
        $dryRun          = (bool) $input->getOption('dry-run');
        $verboseLlm      = (bool) $input->getOption('verbose-llm');
        $skipOutput      = (bool) $input->getOption('skip-output');
        $skipValidation  = (bool) $input->getOption('skip-validation');

        $this->ensureSchema();

        $rules = $this->loadRules();
        if (empty($rules)) {
            $output->writeln('[ERROR] Could not load rules from system-prompt.txt');
            return Command::FAILURE;
        }

        if ($ruleFilter) {
            $rules = array_filter($rules, fn($r) => $r['id'] === strtoupper($ruleFilter));
            if (empty($rules)) {
                $output->writeln("[ERROR] Rule {$ruleFilter} not found in system-prompt.txt");
                return Command::FAILURE;
            }
        }

        $output->writeln('');
        $output->writeln('+============================================+');
        $output->writeln('|      LOGIRI MULTI-LLM RULE EVALUATOR       |');
        if (!$skipValidation) {
            $output->writeln('|  Stage 1: Rule Validation                  |');
        }
        if (!$skipOutput) {
            $output->writeln('|  Stage 2: Play Brief Generation (1 round)  |');
        }
        $output->writeln('+============================================+');
        $output->writeln('');

        $totalEvaluated = 0;
        $totalFlagged   = 0;

        foreach ($rules as $rule) {
            $firingPages = $this->getFiringPages($rule);

            if (empty($firingPages)) {
                $output->writeln("[ ] {$rule['id']} -- no pages currently firing, skipping.");
                continue;
            }

            // Enrich firing pages with body text snippet for exact placement instructions
            foreach ($firingPages as &$page) {
                $url = $page['url'] ?? '';
                if ($url && !isset($page['body_text_snippet'])) {
                    try {
                        $snippet = $this->db->fetchOne(
                            "SELECT body_text_snippet
                             FROM page_crawl_snapshots
                             WHERE url = ?
                             ORDER BY crawled_at DESC, id DESC
                             LIMIT 1",
                            [$url]
                        );
                        if ($snippet) {
                            $page['body_text_snippet'] = $snippet;
                        }
                    } catch (\Exception $e) {
                        // Non-fatal
                    }
                }
            }
            unset($page);

            $output->writeln(">> {$rule['id']}: {$rule['name']}");
            $output->writeln("   Pages firing: " . count($firingPages));

            if ($dryRun) {
                $output->writeln("   [DRY RUN] Would send to LLMs. Skipping.");
                $output->writeln('');
                continue;
            }

            // ══════════════════════════════════════════
            //  STAGE 1 — RULE VALIDATION (skippable)
            // ══════════════════════════════════════════
            $finalVerdicts  = [];
            $finalConsensus = ['status' => 'VALIDATED', 'avg_conf' => 10, 'passes' => 5, 'flags' => 0];
            $allRounds      = [];
            $roundsRun      = 0;

            if (!$skipValidation) {
                $output->writeln("   -- Stage 1: Rule Validation --");

                $basePrompt     = $this->buildValidationPrompt($rule, $firingPages);
                $stage1Result   = $this->runDeliberation($basePrompt, $output, $verboseLlm, 'S1');
                $finalVerdicts  = $stage1Result['verdicts'];
                $finalConsensus = $stage1Result['consensus'];
                $allRounds      = $stage1Result['rounds'];
                $roundsRun      = $stage1Result['rounds_run'];

                $this->displayValidationResults($output, $finalVerdicts, $finalConsensus, $allRounds, $roundsRun);
            } else {
                $output->writeln("   [SKIP] Validation skipped -- rules are pre-validated");
            }

            $this->displayValidationResults($output, $finalVerdicts, $finalConsensus, $allRounds, $roundsRun);

            // ══════════════════════════════════════════
            //  STAGE 2 — OUTPUT CONSENSUS FOR DDT TEAM
            // ══════════════════════════════════════════
            $outputConsensus = null;
            $tasksCreated    = 0;
            $tasksSuppressed = 0;

            if (!$skipOutput) {
                $output->writeln('');
                $output->writeln("   -- Stage 2: Output Consensus --");

                $outputPrompt  = $this->buildOutputPrompt($rule, $firingPages, $finalConsensus, $finalVerdicts);
                $stage2Result  = $this->runDeliberation($outputPrompt, $output, $verboseLlm, 'S2', 8000, 1);
                $outputConsensus = $this->synthesiseOutput($stage2Result['verdicts'], $stage2Result['consensus'], $rule, $firingPages);
                $outputConsensus['rounds_run'] = $stage2Result['rounds_run'];

                $this->displayOutputConsensus($output, $outputConsensus, $stage2Result['consensus']);

                // ── CREATE TASKS FROM PLAY BRIEFS ──
                $briefs = $outputConsensus['briefs'] ?? [];
                $firingPageMap = [];
                foreach ($firingPages as $firingPage) {
                    if (!empty($firingPage['url'])) {
                        $firingPageMap[$this->normalizeUrl((string) $firingPage['url'])] = $firingPage;
                    }
                }
                foreach ($briefs as $brief) {
                    $title = $brief['title'] ?? '';
                    $url   = $this->normalizeUrl((string) ($brief['url'] ?? ''));
                    if (!$title || !$url) continue;
                    $brief['url'] = $url;

                    if (!isset($firingPageMap[$url])) {
                        $tasksSuppressed++;
                        $output->writeln("     [suppressed] {$title} â€” {$url} is not in the current firing set");
                        continue;
                    }

                    // Skip meta-commentary briefs (LLM explaining its process instead of an actual task)
                    if (str_contains(strtolower($title), 'maintaining my response') || str_contains(strtolower($title), 'peer summaries')) continue;

                    if ($this->shouldSuppressBriefFromBoard($brief, $outputConsensus, $finalConsensus)) {
                        $tasksSuppressed++;
                        $output->writeln("     [suppressed] {$title} — held back from board due to low confidence / review-only status");
                        continue;
                    }

                    // Check for duplicate — don't create if same rule+url task already exists and is not done
                    $existing = $this->db->fetchAssociative(
                        "SELECT id FROM tasks WHERE rule_id = :rule AND title LIKE :url AND status NOT IN ('done','closed')",
                        ['rule' => $rule['id'], 'url' => '%' . $url . '%']
                    );
                    if ($existing) continue;

                    $strictActionFamily = $this->inferActionFamily($title, (string) ($brief['your_move'] ?? ''), (string) ($brief['done_when'] ?? ''));
                    if ($strictActionFamily && $url) {
                        $strictCrossDup = $this->findExistingTaskByUrlAndActionFamily($url, $strictActionFamily, $rule['id']);
                        if ($strictCrossDup) {
                            $output->writeln("     âš¡ STRICT DEDUP: Skipping {$rule['id']} task for {$url} â€” overlaps with {$strictCrossDup['rule_id']} task #{$strictCrossDup['id']} ({$strictActionFamily})");
                            continue;
                        }
                    }

                    // Cross-rule dedup — detect overlapping work across different rules for the same URL
                    // Group rules by action type to identify semantic duplicates
                    $actionType = match(true) {
                        str_contains(strtolower($title), 'proprietary') || str_contains(strtolower($title), 'z-frame') || str_contains(strtolower($title), 'brand term') => 'proprietary_terms',
                        str_contains(strtolower($title), 'internal link') || str_contains(strtolower($title), 'link') && str_contains(strtolower($title), 'add') => 'internal_links',
                        str_contains(strtolower($title), 'schema') || str_contains(strtolower($title), 'json-ld') || str_contains(strtolower($title), 'structured data') => 'schema',
                        str_contains(strtolower($title), 'alt text') || str_contains(strtolower($title), 'image') => 'images',
                        str_contains(strtolower($title), 'title tag') || str_contains(strtolower($title), 'h1') || str_contains(strtolower($title), 'heading') => 'headings',
                        str_contains(strtolower($title), 'word count') || str_contains(strtolower($title), 'thin content') || str_contains(strtolower($title), 'expand') => 'content_length',
                        str_contains(strtolower($title), 'meta description') => 'meta_description',
                        default => null,
                    };
                    if ($actionType && $url) {
                        $crossDup = $this->db->fetchAssociative(
                            "SELECT id, rule_id, title FROM tasks WHERE title LIKE :url AND status NOT IN ('done','closed') AND rule_id != :rule LIMIT 1",
                            ['url' => '%' . $url . '%', 'rule' => $rule['id']]
                        );
                        if ($crossDup) {
                            $existingTitle = strtolower($crossDup['title'] ?? '');
                            $existingFamily = $this->inferActionFamily($existingTitle);
                            $overlap = $strictActionFamily !== null && $existingFamily === $strictActionFamily;
                            if (!$overlap) {
                                $overlap = match($actionType) {
                                'proprietary_terms' => str_contains($existingTitle, 'proprietary') || str_contains($existingTitle, 'z-frame') || str_contains($existingTitle, 'brand'),
                                'internal_links' => str_contains($existingTitle, 'link'),
                                'schema' => str_contains($existingTitle, 'schema') || str_contains($existingTitle, 'json-ld'),
                                'images' => str_contains($existingTitle, 'alt') || str_contains($existingTitle, 'image'),
                                'headings' => str_contains($existingTitle, 'h1') || str_contains($existingTitle, 'title') || str_contains($existingTitle, 'heading'),
                                'content_length' => str_contains($existingTitle, 'word count') || str_contains($existingTitle, 'thin') || str_contains($existingTitle, 'expand'),
                                'meta_description' => str_contains($existingTitle, 'meta description'),
                                default => false,
                                };
                            }
                            if ($overlap) {
                                $output->writeln("     ⚡ CROSS-RULE DEDUP: Skipping {$rule['id']} task for {$url} — overlaps with {$crossDup['rule_id']} task #{$crossDup['id']}");
                                continue;
                            }
                        }
                    }

                    // Determine priority — normalize LLM output
                    $rawPriority = strtolower(trim($brief['priority'] ?? $rule['priority'] ?? 'high'));
                    $priority = match(true) {
                        str_contains($rawPriority, 'critical'), str_contains($rawPriority, 'urgent') => 'critical',
                        str_contains($rawPriority, 'high') => 'high',
                        str_contains($rawPriority, 'medium') => 'medium',
                        str_contains($rawPriority, 'low') => 'low',
                        default => 'high',
                    };

                    // Determine assignee from brief or rule
                    $assigned = '';
                    if (!empty($brief['assigned'])) {
                        $assigned = $brief['assigned'];
                    } elseif (!empty($rule['assigned'])) {
                        $assigned = $rule['assigned'];
                    }
                    // Extract first name if multiple (e.g., "Brook (content), Brad (schema)")
                    if (preg_match('/^(Brook|Brad|Kalib|Jeanne)/i', $assigned, $am)) {
                        $assigned = ucfirst(strtolower($am[1]));
                    }

                    // Build description from play brief fields
                    $descParts = [];
                    if ($brief['current_state']) $descParts[] = "CURRENT STATE:\n" . $brief['current_state'];
                    if ($brief['your_move'])     $descParts[] = "YOUR MOVE:\n" . $brief['your_move'];
                    if ($brief['done_when'])     $descParts[] = "DONE WHEN: " . $brief['done_when'];
                    if ($brief['caveat'] && strtolower($brief['caveat']) !== 'none') {
                        $descParts[] = "CAVEAT: " . $brief['caveat'];
                    }
                    $description = implode("\n\n", $descParts);

                    // Estimate hours based on priority
                    $hours = match($priority) {
                        'critical' => 4,
                        'high'     => 2,
                        'medium'   => 1,
                        default    => 1,
                    };

                    // Determine recheck type from rule ID prefix
                    $recheckType = match(true) {
                        str_starts_with($rule['id'], 'OPQ')  => 'on_page_fix',
                        str_starts_with($rule['id'], 'TECH') => 'schema_fix',
                        str_starts_with($rule['id'], 'AIS')  => 'on_page_fix',
                        default => 'on_page_fix',
                    };

                    // Parse recheck days
                    $recheckDays = 14;
                    if ($brief['recheck']) {
                        if (preg_match('/(\d+)/', $brief['recheck'], $rm)) {
                            $recheckDays = (int) $rm[1];
                        }
                    }

                    // Task title format: [Rule ID] Brief title — URL
                    $taskTitle = "[{$rule['id']}] {$title}";
                    if ($url && !str_contains($taskTitle, $url)) {
                        $taskTitle .= " — {$url}";
                    }

                    try {
                        $this->db->insert('tasks', [
                            'title'           => substr($taskTitle, 0, 500),
                            'description'     => $description,
                            'rule_id'         => $rule['id'],
                            'assigned_to'     => $assigned ?: null,
                            'assigned_role'   => null,
                            'status'          => 'pending',
                            'priority'        => $priority,
                            'estimated_hours' => $hours,
                            'logged_hours'    => 0,
                            'recheck_type'    => $recheckType,
                            'created_at'      => date('Y-m-d H:i:s'),
                        ]);
                        $tasksCreated++;
                    } catch (\Exception $e) {
                        // Non-fatal — task creation failure doesn't block evaluation
                    }
                }

                if ($tasksCreated > 0) {
                    $output->writeln("  >> {$tasksCreated} task(s) added to Playbook Board");
                }
                if ($tasksSuppressed > 0) {
                    $output->writeln("  >> {$tasksSuppressed} low-confidence/review brief(s) suppressed from the board");
                }
            }

            // Store both stages
            $this->storeEvaluation($rule, $firingPages, $finalVerdicts, $finalConsensus, $allRounds, $roundsRun, $outputConsensus);

            $totalEvaluated++;
            if (in_array($finalConsensus['status'], ['FLAGGED', 'NEEDS_HUMAN_REVIEW'])) {
                $totalFlagged++;
            }

            $output->writeln('');
            $output->writeln(str_repeat('-', 50));
            $output->writeln('');
        }

        $output->writeln('==============================================');
        $output->writeln("SUMMARY: {$totalEvaluated} rules evaluated | {$totalFlagged} flagged");
        $output->writeln('');
        $output->writeln("  View evaluations: SELECT * FROM rule_evaluations ORDER BY evaluated_at DESC;");
        $output->writeln("  View outputs:     SELECT rule_id, output_finding, output_priority FROM rule_evaluations ORDER BY evaluated_at DESC;");

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  RUN DELIBERATION LOOP (shared by both stages)
    // ─────────────────────────────────────────────

    private function runDeliberation(string $basePrompt, OutputInterface $output, bool $verboseLlm, string $stagePrefix, int $maxTokens = 1500, int $maxRounds = 3): array
    {
        $allRounds     = [];
        $finalVerdicts = [];
        $finalConsensus = null;
        $roundsRun     = 0;

        for ($round = 1; $round <= $maxRounds; $round++) {
            $roundsRun = $round;
            $output->writeln("   [{$stagePrefix}] Round {$round} of {$maxRounds}...");

            $prompt = ($round === 1)
                ? $basePrompt
                : $this->buildDeliberationPrompt($basePrompt, $allRounds, $round);

            $responses     = $this->callAllLLMs($prompt, $maxTokens);
            $roundVerdicts = [];

            foreach ($responses as $llm => $response) {
                if (isset($response['error'])) {
                    $output->writeln("   [!] {$llm}: API error -- {$response['error']}");
                    if (isset($allRounds[$round - 1][$llm])) {
                        $roundVerdicts[$llm] = $allRounds[$round - 1][$llm];
                    }
                    continue;
                }
                $roundVerdicts[$llm] = $this->parseVerdict($response['text']);
                if ($verboseLlm) {
                    $output->writeln("   [{$stagePrefix}:R{$round}:{$llm}] " . substr($response['text'], 0, 150));
                }
            }

            $allRounds[$round] = $roundVerdicts;
            $consensus         = $this->determineConsensus($roundVerdicts);
            $passes            = $consensus['passes'];
            $flags             = $consensus['flags'];
            $total             = count($roundVerdicts);

            $output->writeln("   [{$stagePrefix}] Round {$round}: {$consensus['status']} (passes:{$passes} flags:{$flags} of {$total})");

            if ($passes === $total || $flags === $total) {
                $output->writeln("   [{$stagePrefix}] >> Unanimous -- stopping.");
                $finalVerdicts  = $roundVerdicts;
                $finalConsensus = $consensus;
                break;
            }

            if ($round === $maxRounds) {
                $output->writeln("   [{$stagePrefix}] >> Max rounds -- majority vote.");
                $finalVerdicts  = $roundVerdicts;
                $finalConsensus = $this->determineMajority($roundVerdicts, $allRounds);
                break;
            }

            $output->writeln("   [{$stagePrefix}] >> No consensus -- Round " . ($round + 1) . " with peer review.");
        }

        return ['verdicts' => $finalVerdicts, 'consensus' => $finalConsensus, 'rounds' => $allRounds, 'rounds_run' => $roundsRun];
    }

    // ─────────────────────────────────────────────
    //  BUILD STAGE 1 — VALIDATION PROMPT
    // ─────────────────────────────────────────────

    private function buildValidationPrompt(array $rule, array $firingPages): string
    {
        $pageList = '';
        foreach (array_slice($firingPages, 0, 5) as $page) {
            $pageList .= "\n- URL: " . ($page['url'] ?? 'n/a');
            foreach ($page as $key => $val) {
                if ($key === 'url' || in_array($key, ['internal_links', 'crawled_at'])) continue;
                // Boolean fields must display TRUE/FALSE explicitly (PHP false = empty string)
                $boolFields = ['has_central_entity', 'has_core_link', 'h1_matches_title', 'is_noindex', 'is_utility'];
                if (is_null($val)) { $display = 'NULL'; }
                elseif (in_array($key, $boolFields)) { $display = ($val && $val !== 'f' && $val !== '0') ? 'TRUE' : 'FALSE'; }
                elseif (is_bool($val)) { $display = $val ? 'TRUE' : 'FALSE'; }
                else { $display = (string) $val; }
                $pageList .= " | {$key}: " . $display;
            }
        }

        $total = count($firingPages);

        // Pull Jeanne's past reviews for this rule (feedback learning loop)
        $ownerFeedback = $this->getOwnerFeedback($rule['id']);
        $feedbackSection = '';
        if ($ownerFeedback) {
            $feedbackSection = "\n\nOWNER FEEDBACK HISTORY (from Jeanne, the business owner — take this seriously):\n{$ownerFeedback}\n";
        }

        // Brand glossary
        $brandGlossary = $this->getBrandGlossary();

        return <<<PROMPT
You are an expert SEO architect evaluating whether an SEO rule is firing correctly.

SITE CONTEXT:
- Domain: doubledtrailers.com
- Business: Custom horse trailer manufacturer (Double D Trailers)
- Central entity: horse trailer

BRAND TERMINOLOGY (use ONLY these terms):
{$brandGlossary}

RULE BEING EVALUATED:
ID: {$rule['id']}
Name: {$rule['name']}
Trigger condition: {$rule['trigger_condition']}
Diagnosis: {$rule['diagnosis']}

Full rule text:
{$rule['full_text']}
{$feedbackSection}
CURRENT FIRING DATA ({$total} pages triggering this rule):
{$pageList}

YOUR EVALUATION TASK:
1. Is this rule firing correctly given the data above? (yes/no)
2. Are there false positives -- pages flagged that shouldn't be? (yes/no, explain)
3. Are there false negatives -- pages NOT flagged that should be? (yes/no, explain)
4. Is the diagnosis accurate for the pages shown? (yes/no)
5. Does the rule need adjustment? If yes, what specific change?
6. Confidence score: how confident are you in this rule's accuracy? (1-10)
7. Overall verdict: PASS (rule is working correctly) or FLAG (rule needs review)

Respond in this exact format:
FIRING_CORRECTLY: yes/no
FALSE_POSITIVES: yes/no -- [explanation]
FALSE_NEGATIVES: yes/no -- [explanation]
DIAGNOSIS_ACCURATE: yes/no
NEEDS_ADJUSTMENT: yes/no -- [specific suggested change or "none"]
CONFIDENCE: [1-10]
VERDICT: PASS/FLAG
SUMMARY: [one sentence]
PROMPT;
    }

    // ─────────────────────────────────────────────
    //  BUILD STAGE 2 — OUTPUT CONSENSUS PROMPT
    // ─────────────────────────────────────────────

    private function buildOutputPrompt(array $rule, array $firingPages, array $stage1Consensus, array $stage1Verdicts): string
    {
        $pageDetails = '';
        foreach (array_slice($firingPages, 0, 5) as $page) {
            $pageDetails .= "\n\nPAGE: " . ($page['url'] ?? 'n/a');
            $bodySnippet = '';
            foreach ($page as $key => $val) {
                if ($key === 'url' || in_array($key, ['internal_links', 'crawled_at'])) continue;
                // Save body_text_snippet separately — don't inline it with metadata
                if ($key === 'body_text_snippet') {
                    $bodySnippet = (string) $val;
                    continue;
                }
                $boolFields = ['has_central_entity', 'has_core_link', 'h1_matches_title', 'is_noindex', 'is_utility'];
                if (is_null($val)) { $display = 'NULL'; }
                elseif (in_array($key, $boolFields)) { $display = ($val && $val !== 'f' && $val !== '0') ? 'TRUE' : 'FALSE'; }
                elseif (is_bool($val)) { $display = $val ? 'TRUE' : 'FALSE'; }
                else { $display = (string) $val; }
                $pageDetails .= "\n  {$key}: {$display}";
            }
            if ($bodySnippet) {
                // Truncate to ~5000 chars for token efficiency but enough for full-page placement context
                $truncated = strlen($bodySnippet) > 5000 ? substr($bodySnippet, 0, 5000) . '...[truncated]' : $bodySnippet;
                $pageDetails .= "\n  PAGE_BODY_TEXT (use for exact placement):\n  " . str_replace("\n", "\n  ", $truncated);
            }
        }

        $total       = count($firingPages);
        $ruleStatus  = $stage1Consensus['status'];

        $s1Summary = '';
        foreach ($stage1Verdicts as $llm => $v) {
            $s1Summary .= "\n- " . strtoupper($llm) . ": {$v['verdict']} ({$v['confidence']}/10) — {$v['summary']}";
        }

        $ruleNote = ($ruleStatus === 'VALIDATED')
            ? "Rule validation: VALIDATED. The rule is firing correctly."
            : "Rule validation: {$ruleStatus}. Note any caveats.";

        // Pull actual Core page URLs from the database so LLMs reference real pages
        $corePages = $this->getCorePageList();
        $coreList  = implode("\n", array_map(fn($p) => "- {$p['url']} | {$p['title_tag']}", $corePages));

        // Brand glossary
        $brandGlossary = $this->getBrandGlossary();

        // Owner feedback history
        $ownerFeedback = $this->getOwnerFeedback($rule['id']);
        $feedbackSection = '';
        if ($ownerFeedback) {
            $feedbackSection = "\nPAST REVIEWER FEEDBACK ON THIS RULE (incorporate corrections into your output):\n{$ownerFeedback}\n";
        }

        // Outcome feedback — what worked and what didn't from past verifications
        $outcomeFeedback = $this->getOutcomeFeedback($rule['id']);
        $outcomeSection = '';
        if ($outcomeFeedback) {
            $outcomeSection = "\n{$outcomeFeedback}\nUSE THIS TO IMPROVE YOUR RECOMMENDATIONS. If past fixes for this rule failed, propose a different approach. If they succeeded, replicate the winning pattern.\n";
        }

        $structuredRejections = $this->getStructuredRejectionContext((string) ($rule['id'] ?? ''), $firingPages);
        $structuredRejectionSection = '';
        if ($structuredRejections) {
            $structuredRejectionSection = "\n{$structuredRejections}\nUSE THIS TO AVOID REPEATING REJECTED TASK PATTERNS. If a rejection says the task is irrelevant, missing prerequisites, or misclassified, do not generate the same type of task again for similar pages.\n";
        }

        // Competitor SERP context — what top-ranking competitors look like for each page's target query
        $competitorContext = '';
        try {
            $pageUrls = array_column(array_slice($firingPages, 0, 5), 'url');
            $seenQueries = [];
            foreach ($pageUrls as $pUrl) {
                $tq = null;
                foreach ($firingPages as $fp) {
                    if (($fp['url'] ?? '') === $pUrl && !empty($fp['target_query'])) {
                        $tq = $fp['target_query'];
                        break;
                    }
                }
                if (!$tq || isset($seenQueries[$tq])) continue;
                $seenQueries[$tq] = true;

                $serp = $this->db->fetchAssociative(
                    "SELECT top_3_json, paa_json FROM live_serp_checks WHERE query = :q ORDER BY checked_at DESC LIMIT 1",
                    ['q' => $tq]
                );
                if ($serp && !empty($serp['top_3_json'])) {
                    $top3 = json_decode($serp['top_3_json'], true) ?: [];
                    if (!empty($top3)) {
                        $competitorContext .= "\nSERP COMPETITORS for \"{$tq}\":\n";
                        foreach ($top3 as $comp) {
                            $competitorContext .= "  #{$comp['position']} {$comp['domain']} — \"{$comp['title']}\"\n";
                            if (!empty($comp['description'])) {
                                $competitorContext .= "     Snippet: " . substr($comp['description'], 0, 200) . "\n";
                            }
                        }
                    }
                    $paa = json_decode($serp['paa_json'] ?? '[]', true) ?: [];
                    if (!empty($paa)) {
                        $competitorContext .= "  People Also Ask: " . implode(' | ', array_slice($paa, 0, 3)) . "\n";
                    }
                }
            }
        } catch (\Exception $e) {
            // Non-fatal — SERP data not available
        }

        // Load learnings from chat_learnings for memory injection
        $memorySection = '';
        try {
            $learnings = $this->db->fetchAllAssociative(
                "SELECT learning, category FROM chat_learnings WHERE is_active = TRUE ORDER BY confidence DESC LIMIT 20"
            );
            if (!empty($learnings)) {
                $memorySection = "\nYOUR MEMORY (learnings from past conversations — apply these to all output):\n";
                foreach ($learnings as $l) {
                    $memorySection .= "- [{$l['category']}] {$l['learning']}\n";
                }
                $memorySection .= "\n";
            }
        } catch (\Exception $e) {}

        return <<<PROMPT
You are Logiri, an SEO intelligence engine for doubledtrailers.com (Double D Trailers — custom horse trailer manufacturer).

Your job: produce ONE PLAY BRIEF per affected page. A play brief is a task ticket — specific, actionable, copy-paste ready.
{$memorySection}
BRAND TERMINOLOGY (use ONLY these terms — do NOT invent product names):
{$brandGlossary}

RULE THAT FIRED:
ID: {$rule['id']}
Name: {$rule['name']}
Trigger: {$rule['trigger_condition']}
Diagnosis: {$rule['diagnosis']}

Full rule context:
{$rule['full_text']}

VALIDATION: {$ruleNote}
LLM assessments:{$s1Summary}
{$feedbackSection}{$outcomeSection}{$structuredRejectionSection}
REAL CORE PAGES ON THIS SITE (use ONLY these URLs when suggesting Core link targets):
{$coreList}
{$competitorContext}
DATA FOR AFFECTED PAGES ({$total} total):
{$pageDetails}

INSTRUCTIONS:
Write one PLAY_BRIEF block per page, per action. If a page needs two different fixes (e.g., add a definition AND add an internal link), those are TWO SEPARATE PLAY_BRIEF blocks — never combine unrelated actions into one brief.

Each brief must include:
1. CURRENT STATE — the exact data fields from the crawl that triggered this rule. Use the actual values above. Format as bullet points.
2. YOUR MOVE — ONE specific action only. Not two. Not a compound task. ONE thing to do.
   - If the fix involves adding or editing text: use the PAGE_BODY_TEXT provided above to identify the EXACT sentence or paragraph where the change goes. Say "Insert after the sentence that begins '[first 10 words of the sentence]'" or "Replace the paragraph that starts '[first 10 words]' with [new text]". Never say "find the mention" — YOU find it in the body text and tell them exactly where.
   - If the fix involves code (schema, meta tags, HTML): include the actual code snippet ready to paste.
   - If the fix involves a content rewrite: provide the actual before/after text.
3. DONE WHEN — the specific crawl field check that confirms the fix worked, plus any manual verification step.
4. RECHECK — number of days until recheck.

CRITICAL RULES FOR OUTPUT:
- TARGET QUERY IS THE NORTH STAR. Each page has a `target_query` field from GSC — this is the query the page is trying to rank for. ALL optimization decisions must align with this query. If the H1 matches the target query but the title tag doesn't, fix the TITLE TAG, not the H1. If the target query is "3 horse trailer with living quarters" and the H1 says "3 Horse Trailer with Living Quarters for Sale", the H1 is CORRECT — do not change it to match a branded title. Always state the target query in CURRENT_STATE.
- ONE ACTION PER BRIEF. Adding a Z-Frame definition is one brief. Adding an internal link is a separate brief. Adding schema is a separate brief. NEVER combine these.
- USE THE PAGE BODY TEXT to give exact placement. You have the page content — reference it. "Insert before the sentence that begins 'Our trailers feature...'" not "find the first mention and insert before it."
- Do NOT write a report or analysis. Write task tickets.
- Do NOT split by team role. One unified brief per page per action.
- Include actual code snippets where relevant (JSON-LD, meta tags, HTML).
- Include actual copy rewrites where relevant (before/after with exact existing text quoted).
- If the brief says to add schema, include the FULL schema block ready to paste. Never say "from the play brief", "exactly as written", or "use the block below" unless the block is actually included in YOUR_MOVE.
- If the brief says to insert exact copy, include the FULL replacement copy in YOUR_MOVE. Never require the user to reconstruct, infer, or request the missing text.
- Do not ask the user to confirm crawl facts that are already present above (word count, H1, title, schema types, internal link count, canonical, video presence, target URL existence, GSC fields). Use the provided data as ground truth.
- If a required payload is missing (schema block, exact copy, verified video metadata, etc.), do NOT create an executable implementation task. Convert it into a readiness-review brief with a clear CAVEAT explaining what data is missing.
- Reference the EXACT data values from the crawl data above.
- When suggesting Core page link targets, use ONLY URLs from the REAL CORE PAGES list above. Do not invent URLs.
- Keep each brief under 200 words.
- If a page is a false positive or edge case, say so in a CAVEAT line and suggest skipping instead of fixing.
- Product pages: body text must NOT exceed 500 words. MSE elements carry the page.
- Outer pages: minimum 1000 words. Below that = thin content.
- Max 3 internal links per page. Zero external links.
- Never claim crawl data is unavailable if page fields are present above. Use the provided fields as ground truth.
- Do not create contradictory instructions. Never say both "do not change H1" and "update H1" in the same brief.
- Do not create generic schema prerequisite steps unless the actual page data shows the prerequisite is failing.
- For DDT-SD-002 specifically: if the page is not the homepage and has word_count < 400 or target_query_position > 30, do not generate a direct Organization schema deployment Play. Generate a prerequisite readiness review instead.

FORMAT (repeat for each page, per action):

PLAY_BRIEF: [Short title — verb + what + where]
URL: [exact url path]
TARGET_QUERY: [the target_query from crawl data — this is what the page is optimizing for]
PRIORITY: [Critical / High / Medium / Low]
ASSIGNED: [Brook / Brad / Kalib / Jeanne]
CURRENT_STATE:
- target_query: [query]
- target_query_position: [position]
- target_query_impressions: [count]
- [other relevant fields]: [values]
YOUR_MOVE:
1. [Step]
2. [Step]
3. [Step]
DONE_WHEN: [crawl field check] + [manual verification step]
RECHECK: [X] days
CAVEAT: [Any edge case note, or "None"]
PROMPT;
    }

    // ─────────────────────────────────────────────
    //  SYNTHESISE OUTPUT CONSENSUS
    //  Merges 3 LLM outputs into a single agreed output
    // ─────────────────────────────────────────────

    private function synthesiseOutput(array $verdicts, array $consensus, array $rule, array $firingPages): array
    {
        // Use the highest-confidence LLM's output as the base
        $best     = null;
        $bestConf = -1;

        foreach ($verdicts as $llm => $v) {
            if ($v['confidence'] > $bestConf) {
                $bestConf = $v['confidence'];
                $best     = $v;
            }
        }

        if (!$best) {
            return ['status' => 'NO_OUTPUT', 'raw' => '', 'briefs' => []];
        }

        $raw = $best['raw'] ?? '';

        // Parse PLAY_BRIEF blocks
        $briefs = $this->parsePlayBriefs($raw);

        // If best LLM produced no parseable briefs, try others
        if (empty($briefs)) {
            foreach ($verdicts as $llm => $v) {
                $briefs = $this->parsePlayBriefs($v['raw'] ?? '');
                if (!empty($briefs)) { $raw = $v['raw']; break; }
            }
        }

        $briefs = $this->sanitizePlayBriefs($briefs, $rule, $firingPages);

        // Merge caveats from all LLMs
        $allCaveats = [];
        foreach ($verdicts as $llm => $v) {
            if (preg_match_all('/CAVEAT:\s*(.+)/i', $v['raw'] ?? '', $m)) {
                foreach ($m[1] as $c) {
                    $c = trim($c);
                    if ($c && strtolower($c) !== 'none' && !in_array($c, $allCaveats)) {
                        $allCaveats[] = $c;
                    }
                }
            }
        }

        return [
            'status'     => $consensus['status'],
            'briefs'     => $briefs,
            'caveats'    => $allCaveats,
            'rounds_run' => $consensus['rounds_run'] ?? 1,
            'avg_conf'   => $consensus['avg_conf'],
            'raw'        => $raw,
            // Legacy fields for DB storage — flatten first brief for backwards compat
            'finding'    => !empty($briefs) ? ($briefs[0]['title'] ?? '') : '',
            'diagnosis'  => !empty($briefs) ? ($briefs[0]['current_state'] ?? '') : '',
            'pages'      => array_map(fn($b) => ($b['url'] ?? '') . ' | ' . ($b['title'] ?? ''), $briefs),
            'priority'   => !empty($briefs) ? ($briefs[0]['priority'] ?? 'High') : ($rule['priority'] ?? 'High'),
            'verify_in'  => !empty($briefs) ? ($briefs[0]['recheck'] ?? '14') : '14',
            'role_brook'  => null,
            'role_brad'   => null,
            'role_kalib'  => null,
            'role_jeanne' => null,
            'caveat'     => !empty($allCaveats) ? implode(' | ', $allCaveats) : 'None',
        ];
    }

    private function parsePlayBriefs(string $text): array
    {
        $briefs = [];
        // Split on PLAY_BRIEF: headers
        $blocks = preg_split('/\nPLAY_BRIEF:\s*/i', $text);

        foreach ($blocks as $block) {
            $block = trim($block);
            if (empty($block)) continue;

            $brief = [
                'title'         => '',
                'url'           => '',
                'priority'      => '',
                'assigned'      => '',
                'current_state' => '',
                'your_move'     => '',
                'done_when'     => '',
                'recheck'       => '',
                'caveat'        => '',
            ];

            // Title is the first line — strip any residual PLAY_BRIEF: prefix
            $lines = explode("\n", $block, 2);
            $brief['title'] = trim(preg_replace('/^PLAY_BRIEF:\s*/i', '', trim($lines[0])));
            $rest = $lines[1] ?? '';

            $brief['url']           = $this->extractField($rest, 'URL') ?: $this->extractField($block, 'URL');
            $brief['priority']      = $this->extractField($rest, 'PRIORITY') ?: $this->extractField($block, 'PRIORITY');
            $brief['assigned']      = $this->extractField($rest, 'ASSIGNED') ?: $this->extractField($block, 'ASSIGNED');
            $brief['current_state'] = $this->extractField($rest, 'CURRENT_STATE') ?: $this->extractField($block, 'CURRENT_STATE');
            $brief['your_move']     = $this->extractField($rest, 'YOUR_MOVE') ?: $this->extractField($block, 'YOUR_MOVE');
            $brief['done_when']     = $this->extractField($rest, 'DONE_WHEN') ?: $this->extractField($block, 'DONE_WHEN');
            $brief['recheck']       = $this->extractField($rest, 'RECHECK') ?: $this->extractField($block, 'RECHECK');
            $brief['caveat']        = $this->extractField($rest, 'CAVEAT') ?: $this->extractField($block, 'CAVEAT');

            // Only include if we got at minimum a URL or title
            if ($brief['url'] || $brief['title']) {
                $briefs[] = $brief;
            }
        }

        return $briefs;
    }

    private function sanitizePlayBriefs(array $briefs, array $rule, array $firingPages): array
    {
        $pageMap = [];
        foreach ($firingPages as $page) {
            if (!empty($page['url'])) {
                $pageMap[$this->normalizeUrl((string) $page['url'])] = $page;
            }
        }

        $sanitized = [];
        foreach ($briefs as $brief) {
            $rawUrl = trim((string) ($brief['url'] ?? ''));
            $url = $this->normalizeUrl($rawUrl);
            $page = $pageMap[$url] ?? null;

            if ($this->shouldDropBriefForUrl($rawUrl, $url, $page)) {
                continue;
            }

            $brief['rule_id'] = (string) ($rule['id'] ?? ($brief['rule_id'] ?? ''));

            if ($page) {
                $brief['url'] = $url;
                $brief['page_type'] = (string) ($page['page_type'] ?? ($brief['page_type'] ?? ''));
                $brief = $this->repairMissingCrawlBoilerplate($brief, $page);
                $brief = $this->removeContradictoryInstructions($brief, $page);
                $brief = $this->enforceSelfContainedBrief($brief, $page);
            }

            if (($rule['id'] ?? '') === 'DDT-SD-002' && $page) {
                $brief = $this->applyOrganizationSchemaGate($brief, $page);
            }

            $sanitized[] = $brief;
        }

        return $sanitized;
    }

    private function repairMissingCrawlBoilerplate(array $brief, array $page): array
    {
        $combined = strtolower(($brief['current_state'] ?? '') . "\n" . ($brief['your_move'] ?? ''));
        if (!str_contains($combined, 'no crawl data is available')) {
            return $brief;
        }

        $brief['current_state'] = $this->buildCurrentStateFromPage($page);

        $yourMove = preg_replace('/.*no crawl data is available.*(?:\r?\n)?/i', '', (string) ($brief['your_move'] ?? ''));
        $yourMove = preg_replace('/.*run php bin\/console app:crawl-pages.*(?:\r?\n)?/i', '', (string) $yourMove);
        $brief['your_move'] = trim((string) $yourMove);

        if ($brief['your_move'] === '') {
            $brief['your_move'] = '1. Use the current crawl data already present for this URL. 2. Perform the schema or content fix required by the rule. 3. Re-crawl the page after publishing to verify the target fields changed.';
        }

        return $brief;
    }

    private function removeContradictoryInstructions(array $brief, array $page): array
    {
        $yourMove = (string) ($brief['your_move'] ?? '');
        $lines = preg_split('/\r\n|\r|\n/', $yourMove) ?: [];
        $filtered = [];

        $h1Aligned = $this->toBool($page['h1_matches_title'] ?? false);
        $canonicalUrl = (string) ($page['canonical_url'] ?? '');
        $selfCanonical = $canonicalUrl !== '' && rtrim($canonicalUrl, '/') === rtrim('https://www.doubledtrailers.com' . $this->normalizeUrl((string) ($page['url'] ?? '')), '/');

        foreach ($lines as $line) {
            $lineLower = strtolower(trim($line));
            if ($h1Aligned && str_contains($lineLower, 'update h1')) {
                continue;
            }
            if ($selfCanonical && (str_contains($lineLower, 'confirm canonical') || str_contains($lineLower, 'canonical integrity before any schema'))) {
                continue;
            }
            $filtered[] = $line;
        }

        $brief['your_move'] = trim(implode("\n", $filtered));

        if ($h1Aligned && str_contains(strtolower((string) ($brief['done_when'] ?? '')), 'h1') && str_contains(strtolower((string) ($brief['done_when'] ?? '')), 'change')) {
            $brief['done_when'] = preg_replace('/\s*\+\s*.*h1.*$/i', '', (string) $brief['done_when']) ?: $brief['done_when'];
        }

        return $brief;
    }

    private function enforceSelfContainedBrief(array $brief, array $page): array
    {
        $yourMove = (string) ($brief['your_move'] ?? '');
        $combined = strtolower(trim(($brief['title'] ?? '') . "\n" . $yourMove . "\n" . ($brief['done_when'] ?? '')));
        $hasVideo = trim((string) ($page['video_urls'] ?? '')) !== '' || trim((string) ($page['video_title'] ?? '')) !== '';

        $brief['your_move'] = $this->stripRedundantValidationSteps($yourMove, $page);

        if ($this->requiresVideoPayload($combined) && !$hasVideo) {
            return $this->convertBriefToReview(
                $brief,
                $page,
                'Skip video/schema implementation for this page',
                'The current crawl data does not show a main-content video on this URL, so a VideoObject task is not executable and should not be assigned.'
            );
        }

        if ($this->requiresSchemaPayload($combined) && !$this->containsSchemaPayload($yourMove)) {
            return $this->convertBriefToReview(
                $brief,
                $page,
                'Review schema prerequisites before implementation',
                'The brief requests schema deployment but does not include a paste-ready JSON-LD block. Generate the schema payload first or skip this task.'
            );
        }

        if ($this->requiresExactCopyPayload($combined) && !$this->containsExactCopyPayload($yourMove)) {
            return $this->convertBriefToReview(
                $brief,
                $page,
                'Review missing copy payload before implementation',
                'The brief tells the user to insert exact or verbatim copy, but the actual replacement text is missing. Generate the copy first or skip this task.'
            );
        }

        if ($this->hasVaguePlacementInstruction($yourMove) && !$this->containsQuotedPlacementAnchor($yourMove)) {
            return $this->convertBriefToReview(
                $brief,
                $page,
                'Review vague placement instructions before implementation',
                'The brief does not quote the exact paragraph, heading, or sentence to replace or insert around. Regenerate it with a precise placement anchor.'
            );
        }

        if ($this->isMultiActionImplementationBrief($brief)) {
            return $this->convertBriefToReview(
                $brief,
                $page,
                'Split multi-action play before implementation',
                'The brief bundles multiple implementation actions into one task. Split it into separate plays so each task has one clear change and one verification target.'
            );
        }

        if ($this->hasFeatureCompatibilityMismatch($combined, $page)) {
            return $this->convertBriefToReview(
                $brief,
                $page,
                'Review feature-to-page mismatch before implementation',
                'The brief introduces feature language that does not match the current product/page context. Regenerate the play using only compatible features already supported by this page.'
            );
        }

        if ($this->violatesPageTypePolicy($combined, $brief, $page)) {
            return $this->convertBriefToReview(
                $brief,
                $page,
                'Review page-type policy before implementation',
                $this->buildPageTypePolicyCaveat($combined, $page)
            );
        }

        if ($this->hasInvalidSuccessCriteria($brief, $page)) {
            return $this->convertBriefToReview(
                $brief,
                $page,
                'Review success criteria before implementation',
                $this->buildSuccessCriteriaCaveat($brief, $page)
            );
        }

        return $brief;
    }

    private function stripRedundantValidationSteps(string $yourMove, array $page): string
    {
        $lines = preg_split('/\r\n|\r|\n/', $yourMove) ?: [];
        $filtered = [];

        foreach ($lines as $line) {
            $lineLower = strtolower(trim($line));

            if ($lineLower === '') {
                continue;
            }

            if (
                str_contains($lineLower, 'run php bin/console app:crawl-pages')
                || str_contains($lineLower, 'run the crawl')
                || str_contains($lineLower, 'pull live canonical data')
                || str_contains($lineLower, 'confirm current word count')
                || str_contains($lineLower, 'verify current word count')
                || str_contains($lineLower, 'confirm the target url exists in crawl data')
                || str_contains($lineLower, 'pull gsc average position')
                || str_contains($lineLower, 'confirm current in-body link count')
            ) {
                continue;
            }

            if (($page['canonical_url'] ?? '') !== '' && str_contains($lineLower, 'confirm canonical')) {
                continue;
            }

            $filtered[] = $line;
        }

        $result = trim(implode("\n", $filtered));

        return $result !== '' ? $result : $yourMove;
    }

    private function requiresVideoPayload(string $combined): bool
    {
        return str_contains($combined, 'videoobject')
            || str_contains($combined, 'youtube video')
            || str_contains($combined, 'video id')
            || str_contains($combined, 'thumbnail url')
            || str_contains($combined, 'upload date');
    }

    private function requiresSchemaPayload(string $combined): bool
    {
        return str_contains($combined, 'json-ld')
            || str_contains($combined, 'schema block')
            || str_contains($combined, 'organization schema')
            || str_contains($combined, 'videoobject schema')
            || str_contains($combined, 'product schema')
            || str_contains($combined, 'itemlist schema')
            || str_contains($combined, 'faqpage')
            || str_contains($combined, 'schema');
    }

    private function containsSchemaPayload(string $text): bool
    {
        $lower = strtolower($text);

        return str_contains($lower, '<script type="application/ld+json">')
            || str_contains($lower, '"@context"')
            || str_contains($lower, "'@context'")
            || str_contains($lower, '{')
            && (
                str_contains($lower, '"@type"')
                || str_contains($lower, "'@type'")
            );
    }

    private function requiresExactCopyPayload(string $combined): bool
    {
        return str_contains($combined, 'verbatim')
            || str_contains($combined, 'exactly as written')
            || str_contains($combined, 'exactly this')
            || str_contains($combined, 'from the play brief')
            || str_contains($combined, 'replace it with exactly this')
            || str_contains($combined, 'insert the passage');
    }

    private function containsExactCopyPayload(string $text): bool
    {
        return preg_match('/["\'].{80,}["\']/s', $text) === 1
            || str_contains($text, 'Replace it with exactly this:')
            || str_contains($text, "Replace it with exactly this:\n")
            || str_contains($text, "Insert this copy:\n")
            || str_contains($text, "```");
    }

    private function hasVaguePlacementInstruction(string $text): bool
    {
        $lower = strtolower($text);

        return str_contains($lower, 'find the generic')
            || str_contains($lower, 'find the paragraph')
            || str_contains($lower, 'find the first paragraph')
            || str_contains($lower, 'replace the existing h2')
            || str_contains($lower, 'replace the existing heading')
            || str_contains($lower, 'insert before the configurator')
            || str_contains($lower, 'insert near the top')
            || str_contains($lower, 'inside body copy')
            || str_contains($lower, 'in the body copy')
            || str_contains($lower, 'under both h2s');
    }

    private function containsQuotedPlacementAnchor(string $text): bool
    {
        $lower = strtolower($text);

        if (
            str_contains($lower, 'replace this paragraph:')
            || str_contains($lower, 'replace the paragraph that begins')
            || str_contains($lower, 'insert after the sentence that begins')
            || str_contains($lower, 'replace this heading:')
            || str_contains($lower, 'replace the paragraph beginning')
        ) {
            return true;
        }

        return preg_match('/["\'][^"\']{20,}["\']/', $text) === 1;
    }

    private function isMultiActionImplementationBrief(array $brief): bool
    {
        $text = strtolower(trim(($brief['title'] ?? '') . "\n" . ($brief['your_move'] ?? '') . "\n" . ($brief['done_when'] ?? '')));
        $actionTypes = 0;

        $actionPatterns = [
            '/\btitle\b|\btitle tag\b|\bmeta\b/',
            '/\bh1\b|\bh2\b|\bheading\b/',
            '/\bparagraph\b|\bbody copy\b|\bopening copy\b|\bword count\b|\badd \d+\+? words\b/',
            '/\bschema\b|\bjson-ld\b|\bfaqpage\b|\bvideoobject\b|\borganization\b|\bitemlist\b|\bproduct schema\b/',
            '/\binternal link\b|\blinks?\b/',
            '/\bcwv\b|\bcore web vitals\b|\brich results test\b/'
        ];

        foreach ($actionPatterns as $pattern) {
            if (preg_match($pattern, $text) === 1) {
                $actionTypes++;
            }
        }

        return $actionTypes >= 4;
    }

    private function hasFeatureCompatibilityMismatch(string $combined, array $page): bool
    {
        $pageText = strtolower(
            trim(
                implode("\n", array_filter([
                    (string) ($page['url'] ?? ''),
                    (string) ($page['title_tag'] ?? ''),
                    (string) ($page['h1'] ?? ''),
                    (string) ($page['body_text_snippet'] ?? ''),
                ]))
            )
        );

        if (str_contains($combined, 'safetack reverse') && !str_contains($pageText, 'safetack reverse') && !str_contains($pageText, 'reverse load')) {
            return true;
        }

        if (str_contains($combined, 'videoobject') && !str_contains($pageText, 'video') && trim((string) ($page['video_urls'] ?? '')) === '') {
            return true;
        }

        if (str_contains($combined, 'faqpage') && !$this->looksLikeFaqCandidate($pageText)) {
            return true;
        }

        return false;
    }

    private function looksLikeFaqCandidate(string $pageText): bool
    {
        return str_contains($pageText, 'frequently asked questions')
            || str_contains($pageText, 'faq')
            || substr_count($pageText, '?') >= 3;
    }

    private function violatesPageTypePolicy(string $combined, array $brief, array $page): bool
    {
        $policy = $this->classifyPagePolicy($page);
        $text = $combined . "\n" . strtolower((string) ($brief['current_state'] ?? ''));

        if ($policy === 'core_hub') {
            if (
                preg_match('/\b<=\s*500\b/', $text) === 1
                || str_contains($text, 'cut living quarters core page')
                || str_contains($text, 'offload everything else')
                || str_contains($text, 'mse takes over')
                || str_contains($text, 'one intro sentence')
            ) {
                return true;
            }
        }

        if ($policy === 'testimonial_review') {
            if (
                str_contains($text, '1000+ words')
                || preg_match('/\badd\s+5\d{2}\+?\s+words\b/', $text) === 1
                || preg_match('/\badd\s+\d{3,}\+?\s+words\b/', $text) === 1
                || str_contains($text, 'expand body to 1,000+ words')
                || str_contains($text, 'editorial context around the video')
                || str_contains($text, 'merge or expand')
            ) {
                return true;
            }
        }

        if ($policy === 'informational_article') {
            if (
                str_contains($text, 'safe trailer solutions')
                || str_contains($text, 'bridge content to ddt products')
                || str_contains($text, 'bridge to ddt products')
                || str_contains($text, 'product-bridge')
                || str_contains($text, 'factory-direct from')
            ) {
                return true;
            }
        }

        return false;
    }

    private function buildPageTypePolicyCaveat(string $combined, array $page): string
    {
        return match ($this->classifyPagePolicy($page)) {
            'core_hub' => 'This URL behaves like a core hub/category page. Do not compress it to product-page length or offload core differentiator content just to satisfy a word-cap rule.',
            'testimonial_review' => 'This URL behaves like a testimonial/review page. Do not inflate it into an article-length page; prefer authenticity-preserving edits or a merge/redirect decision.',
            'informational_article' => 'This URL behaves like an informational article. Do not force aggressive product-bridge messaging unless the page is already clearly commercial-adjacent.',
            default => 'This brief conflicts with the page-type policy for this URL. Regenerate it with a strategy that matches the page’s actual role on the site.',
        };
    }

    private function classifyPagePolicy(array $page): string
    {
        $url = strtolower((string) ($page['url'] ?? ''));
        $pageType = strtolower((string) ($page['page_type'] ?? ''));
        $title = strtolower((string) ($page['title_tag'] ?? ''));
        $h1 = strtolower((string) ($page['h1'] ?? ''));
        $body = strtolower((string) ($page['body_text_snippet'] ?? ''));
        $wordCount = (int) ($page['word_count'] ?? 0);

        $haystack = implode("\n", [$url, $pageType, $title, $h1, $body]);

        if (
            str_contains($haystack, 'review posted')
            || str_contains($haystack, 'customer review')
            || str_contains($haystack, 'double d trailer review')
            || str_contains($haystack, 'would you recommend double d trailers')
            || preg_match('#/(horse-trailer-reviews|reviews?)/#', $url) === 1
        ) {
            return 'testimonial_review';
        }

        if (
            $pageType === 'core'
            || preg_match('#^/(horse-trailers|gooseneck-horse-trailers|bumper-pull-horse-trailers|living-quarters-horse-trailers)/?$#', $url) === 1
            || (str_contains($haystack, 'for sale') && $wordCount > 1000)
        ) {
            return 'core_hub';
        }

        if (
            $pageType === 'outer'
            || str_contains($url, '/articles/')
            || str_contains($url, '/resources/')
            || str_contains($haystack, 'what sets our customer service apart')
            || str_contains($haystack, 'horse jockeys')
        ) {
            return 'informational_article';
        }

        return 'default';
    }

    private function hasInvalidSuccessCriteria(array $brief, array $page): bool
    {
        $doneWhen = strtolower(trim((string) ($brief['done_when'] ?? '')));
        $yourMove = strtolower(trim((string) ($brief['your_move'] ?? '')));

        if ($doneWhen === '') {
            return false;
        }

        if ($this->isOutcomeOnlySuccessCriteria($doneWhen)) {
            return true;
        }

        if ($this->hasContradictoryHeadingSuccessCriteria($doneWhen, $yourMove)) {
            return true;
        }

        if ($this->hasEditMismatchSuccessCriteria($doneWhen, $yourMove, $page)) {
            return true;
        }

        return false;
    }

    private function isOutcomeOnlySuccessCriteria(string $doneWhen): bool
    {
        $implementationAnchors = [
            'crawl returns',
            'rich results test',
            'detected',
            'visible in body text',
            'word count',
            'title matches',
            'meta description',
            'schema',
            'internal links',
            'h1_matches_title',
            'contains ',
        ];

        $hasImplementationAnchor = false;
        foreach ($implementationAnchors as $anchor) {
            if (str_contains($doneWhen, $anchor)) {
                $hasImplementationAnchor = true;
                break;
            }
        }

        if ($hasImplementationAnchor) {
            return false;
        }

        return str_contains($doneWhen, 'ctr')
            || str_contains($doneWhen, 'bounce rate')
            || str_contains($doneWhen, 'position')
            || str_contains($doneWhen, 'rank')
            || str_contains($doneWhen, 'ranking')
            || str_contains($doneWhen, 'clicks')
            || str_contains($doneWhen, 'impressions');
    }

    private function hasContradictoryHeadingSuccessCriteria(string $doneWhen, string $yourMove): bool
    {
        $mentionsH1Match = str_contains($doneWhen, 'h1_matches_title')
            || (str_contains($doneWhen, 'h1') && str_contains($doneWhen, 'title') && str_contains($doneWhen, 'match'));

        if (!$mentionsH1Match) {
            return false;
        }

        $titleValue = $this->extractQuotedAfterLabel($yourMove, 'title');
        $h1Value = $this->extractQuotedAfterLabel($yourMove, 'h1');

        if ($titleValue === '' || $h1Value === '') {
            return false;
        }

        return trim($titleValue) !== trim($h1Value);
    }

    private function hasEditMismatchSuccessCriteria(string $doneWhen, string $yourMove, array $page): bool
    {
        $hasTitleEdit = str_contains($yourMove, 'title');
        $hasH1Edit = preg_match('/\bh1\b/', $yourMove) === 1;
        $hasSchemaEdit = str_contains($yourMove, 'schema') || str_contains($yourMove, 'json-ld');
        $hasBodyEdit = str_contains($yourMove, 'paragraph')
            || str_contains($yourMove, 'body copy')
            || str_contains($yourMove, 'opening copy')
            || str_contains($yourMove, 'insert ')
            || str_contains($yourMove, 'replace ');

        if (!$hasTitleEdit && str_contains($doneWhen, 'title')) {
            return true;
        }

        if (!$hasH1Edit && (str_contains($doneWhen, 'h1_matches_title') || preg_match('/\bh1\b/', $doneWhen) === 1)) {
            return true;
        }

        if (!$hasSchemaEdit && str_contains($doneWhen, 'rich results test')) {
            return true;
        }

        if (!$hasBodyEdit && str_contains($doneWhen, 'body contains')) {
            return true;
        }

        if (
            str_contains($doneWhen, '<= 500')
            && $this->classifyPagePolicy($page) === 'core_hub'
        ) {
            return true;
        }

        return false;
    }

    private function extractQuotedAfterLabel(string $text, string $label): string
    {
        if (preg_match('/' . preg_quote($label, '/') . '\s*[:\-]\s*["\']([^"\']+)["\']/i', $text, $m)) {
            return trim($m[1]);
        }

        return '';
    }

    private function buildSuccessCriteriaCaveat(array $brief, array $page): string
    {
        $doneWhen = strtolower(trim((string) ($brief['done_when'] ?? '')));
        $yourMove = strtolower(trim((string) ($brief['your_move'] ?? '')));

        if ($this->isOutcomeOnlySuccessCriteria($doneWhen)) {
            return 'The done-state relies on outcome metrics like CTR, impressions, or rankings without a concrete implementation verification step. Regenerate it with crawl-verifiable completion criteria first.';
        }

        if ($this->hasContradictoryHeadingSuccessCriteria($doneWhen, $yourMove)) {
            return 'The done-state says the H1 and title should match, but the brief proposes different H1 and title values. Regenerate the play with consistent heading instructions and success criteria.';
        }

        if ($this->hasEditMismatchSuccessCriteria($doneWhen, $yourMove, $page)) {
            return 'The done-state does not line up with the actual edit being requested. Regenerate the play so the completion checks directly verify the implementation work in the brief.';
        }

        return 'The completion criteria are not reliable for this play. Regenerate it with implementation-level checks instead of ambiguous or mismatched outcomes.';
    }

    private function shouldSuppressBriefFromBoard(array $brief, ?array $outputConsensus, array $stage1Consensus): bool
    {
        $rawUrl = trim((string) ($brief['url'] ?? ''));
        $normalizedUrl = $this->normalizeUrl($rawUrl);

        if ($this->shouldDropBriefForUrl($rawUrl, $normalizedUrl, null)) {
            return true;
        }

        if ($this->matchesStructuredRejectionGuardrail($brief)) {
            return true;
        }
        $score = $this->computeBriefBoardConfidence($brief, $outputConsensus, $stage1Consensus);

        if ($score < 0.55) {
            return true;
        }

        $title = strtolower(trim((string) ($brief['title'] ?? '')));
        $caveat = strtolower(trim((string) ($brief['caveat'] ?? '')));

        if (
            str_contains($title, 'review ')
            || str_contains($title, 'skip ')
            || str_contains($caveat, 'false positive')
            || str_contains($caveat, 'do not execute')
            || str_contains($caveat, 'regenerate')
        ) {
            return true;
        }

        return false;
    }

    private function shouldDropBriefForUrl(string $rawUrl, string $normalizedUrl, ?array $page): bool
    {
        $rawUrl = trim($rawUrl);
        $rawLower = strtolower($rawUrl);
        $normalizedLower = strtolower($normalizedUrl);

        if ($rawLower === '' || $normalizedLower === '') {
            return false;
        }

        if ($this->looksLikeSuppressedAssetUrl($normalizedLower)) {
            return true;
        }

        if ($this->looksLikeSuppressedUtilityUrl($normalizedLower)) {
            return true;
        }

        if (preg_match('#^/(title|url|slug|page|placeholder)/?$#', $normalizedLower) === 1) {
            return true;
        }

        if ($page === null && preg_match('#^/[A-Z][A-Za-z0-9-]*/?$#', $rawUrl) === 1) {
            return true;
        }

        if ($page === null && preg_match('#(?:^|/)(?:www\.)?doubledtrailers\.com(?:/|$)#', $rawLower) === 1) {
            return true;
        }

        if ($page === null && $normalizedLower === '/' && !preg_match('#^(?:https?://)?(?:www\.)?doubledtrailers\.com/?$#', $rawLower)) {
            return true;
        }

        return false;
    }

    private function looksLikeSuppressedAssetUrl(string $normalizedLower): bool
    {
        return str_contains($normalizedLower, '/wp-content/uploads/')
            || preg_match('/\.(jpg|jpeg|png|gif|webp|svg|pdf)$/', $normalizedLower) === 1;
    }

    private function looksLikeSuppressedUtilityUrl(string $normalizedLower): bool
    {
        return str_starts_with($normalizedLower, '/scripts/')
            || str_starts_with($normalizedLower, '/wp-json/')
            || str_starts_with($normalizedLower, '/wp-admin/')
            || preg_match('/\.(html|json|xml|txt)$/', $normalizedLower) === 1;
    }

    private function inferActionFamily(string $title, string $yourMove = '', string $doneWhen = ''): ?string
    {
        $text = strtolower(trim($title . "\n" . $yourMove . "\n" . $doneWhen));

        return match (true) {
            str_contains($text, 'alt text') || str_contains($text, 'decorative alt') || str_contains($text, 'image alt') => 'images',
            str_contains($text, 'videoobject') || str_contains($text, 'schema') || str_contains($text, 'json-ld') || str_contains($text, 'faqpage') || str_contains($text, 'organization schema') || str_contains($text, 'review schema') || str_contains($text, 'itemlist') || str_contains($text, 'product schema') => 'schema',
            str_contains($text, 'internal link') || str_contains($text, 'anchor text') || str_contains($text, 'cluster link') || (str_contains($text, 'link') && str_contains($text, 'inbound')) => 'internal_links',
            str_contains($text, 'title tag') || str_contains($text, 'meta description') => 'metadata',
            str_contains($text, 'h1') || str_contains($text, 'h2') || str_contains($text, 'heading') => 'headings',
            str_contains($text, 'z-frame') || str_contains($text, 'proprietary') || str_contains($text, 'brand term') || str_contains($text, 'testimonial') || str_contains($text, 'review block') => 'content_entity',
            str_contains($text, 'thin content') || str_contains($text, 'word count') || str_contains($text, '1,000') || str_contains($text, '1000') || str_contains($text, 'expand') => 'content_depth',
            str_contains($text, 'canonical') || str_contains($text, 'redirect') => 'canonical_redirect',
            default => null,
        };
    }

    private function findExistingTaskByUrlAndActionFamily(string $url, string $actionFamily, string $currentRuleId): ?array
    {
        $tasks = $this->db->fetchAllAssociative(
            "SELECT id, rule_id, title, description
             FROM tasks
             WHERE status NOT IN ('done','closed')
               AND title LIKE :url
               AND rule_id != :rule",
            ['url' => '%' . $url . '%', 'rule' => $currentRuleId]
        );

        foreach ($tasks as $task) {
            $existingFamily = $this->inferActionFamily(
                (string) ($task['title'] ?? ''),
                (string) ($task['description'] ?? ''),
                ''
            );
            if ($existingFamily === $actionFamily) {
                return $task;
            }
        }

        return null;
    }

    private function computeBriefBoardConfidence(array $brief, ?array $outputConsensus, array $stage1Consensus): float
    {
        $score = 1.0;

        $avgConf = isset($outputConsensus['avg_conf']) ? (float) $outputConsensus['avg_conf'] : 10.0;
        $stage1Avg = isset($stage1Consensus['avg_conf']) ? (float) $stage1Consensus['avg_conf'] : 10.0;

        $score *= max(0.4, min(1.0, $avgConf / 10));
        $score *= max(0.5, min(1.0, $stage1Avg / 10));

        $title = strtolower(trim((string) ($brief['title'] ?? '')));
        $caveat = strtolower(trim((string) ($brief['caveat'] ?? '')));
        $yourMove = strtolower(trim((string) ($brief['your_move'] ?? '')));

        if ($caveat !== '' && $caveat !== 'none') {
            $score -= 0.25;
        }

        if (
            str_contains($title, 'review ')
            || str_contains($title, 'skip ')
            || str_contains($caveat, 'do not execute')
            || str_contains($caveat, 'regenerate')
            || str_contains($yourMove, 'do not execute this implementation task yet')
        ) {
            $score -= 0.35;
        }

        if (
            str_contains($caveat, 'false positive')
            || str_contains($caveat, 'missing payload')
            || str_contains($caveat, 'vague placement')
            || str_contains($caveat, 'page-type policy')
            || str_contains($caveat, 'success criteria')
        ) {
            $score -= 0.2;
        }
        if ($this->matchesStructuredRejectionGuardrail($brief)) {
            $score -= 0.45;
        }
        return max(0.0, min(1.0, $score));
    }

    private function convertBriefToReview(array $brief, array $page, string $title, string $caveat): array
    {
        $url = $this->normalizeUrl((string) ($page['url'] ?? ($brief['url'] ?? '/')));

        $brief['title'] = $title . ' — ' . $url;
        $brief['assigned'] = 'Jeanne';
        $brief['priority'] = $brief['priority'] ?: 'High';
        $brief['current_state'] = $this->buildCurrentStateFromPage($page);
        $brief['your_move'] = "1. Do not execute this implementation task yet.\n2. Regenerate the Play only after the missing payload is available in the brief itself.\n3. If the page is a false positive, close it instead of assigning work.";
        $brief['done_when'] = 'The Play either includes the missing payload directly or is closed as a false positive.';
        $brief['recheck'] = preg_replace('/\D+/', '', (string) ($brief['recheck'] ?? '14')) ?: '14';
        $brief['caveat'] = $caveat;

        return $brief;
    }

    private function applyOrganizationSchemaGate(array $brief, array $page): array
    {
        $url = $this->normalizeUrl((string) ($page['url'] ?? ''));
        $wordCount = (int) ($page['word_count'] ?? 0);
        $position = isset($page['target_query_position']) ? (float) $page['target_query_position'] : null;
        $isWeakPage = $url !== '/' && $wordCount < 400 && ($position === null || $position > 30);

        if (!$isWeakPage) {
            return $brief;
        }

        $brief['title'] = 'Review page readiness before Organization schema — ' . $url;
        $brief['assigned'] = 'Jeanne';
        $brief['priority'] = 'High';
        $brief['current_state'] = $this->buildCurrentStateFromPage($page);
        $brief['your_move'] = "1. Do not deploy Organization schema on this page yet.\n2. Decide whether this page should first receive a stronger page-quality or content-depth Play.\n3. Only approve Organization schema after the page is competitive enough for entity-layer markup to be a marginal gain.";
        $brief['done_when'] = 'A decision is recorded: either approve Organization schema for this URL or route the page to a prerequisite content/page-quality Play first.';
        $brief['recheck'] = '14';
        $brief['caveat'] = 'Organization schema is gated here because the page is still weak for this intervention: low content depth and/or ranking beyond position 30.';

        return $brief;
    }

    private function buildCurrentStateFromPage(array $page): string
    {
        $schemaTypes = (string) ($page['schema_types'] ?? '[]');
        $canonicalUrl = (string) ($page['canonical_url'] ?? 'unknown');

        $lines = [
            '- page_type: ' . ($page['page_type'] ?? 'unknown'),
            '- word_count: ' . (int) ($page['word_count'] ?? 0),
            '- target_query: ' . ($page['target_query'] ?? 'NONE'),
            '- target_query_impressions: ' . (int) ($page['target_query_impressions'] ?? 0),
            '- target_query_position: ' . ($page['target_query_position'] ?? 'unknown'),
            '- schema_types: ' . $schemaTypes,
            '- canonical_url: ' . $canonicalUrl,
            '- h1_matches_title: ' . ($this->toBool($page['h1_matches_title'] ?? false) ? 'TRUE' : 'FALSE'),
            '- is_noindex: ' . ($this->toBool($page['is_noindex'] ?? false) ? 'TRUE' : 'FALSE'),
        ];

        return implode("\n", $lines);
    }

    private function normalizeUrl(string $url): string
    {
        $url = trim(html_entity_decode($url, ENT_QUOTES | ENT_HTML5));
        if ($url === '') {
            return '';
        }

        if (preg_match('#^[a-z][a-z0-9+.-]*://#i', $url) === 1) {
            $parsedPath = parse_url($url, PHP_URL_PATH);
            $url = is_string($parsedPath) && $parsedPath !== '' ? $parsedPath : '/';
        }

        $url = str_replace('\\', '/', $url);
        $url = preg_replace('#[?#].*$#', '', $url) ?? $url;

        if (preg_match('#^//[^/]+(?P<path>/.*)?$#', $url, $matches) === 1) {
            $url = $matches['path'] ?? '/';
        }

        $url = ltrim($url, '/');
        $url = preg_replace('#^(?:https?:/+)?(?:www\.)?doubledtrailers\.com/?#i', '', $url) ?? $url;
        $url = preg_replace('#/+#', '/', $url) ?? $url;

        $normalized = '/' . ltrim($url, '/');
        $normalized = preg_replace('#/+#', '/', $normalized) ?? $normalized;

        return $normalized === '/' ? '/' : rtrim($normalized, '/') . '/';
    }

    private function toBool(mixed $value): bool
    {
        return $value === true || $value === 1 || $value === '1' || $value === 't' || $value === 'true';
    }

    // ─────────────────────────────────────────────
    //  DISPLAY STAGE 2 OUTPUT CONSENSUS
    // ─────────────────────────────────────────────

    private function displayOutputConsensus(OutputInterface $output, array $oc, array $consensus): void
    {
        if (($oc['status'] ?? '') === 'NO_OUTPUT') {
            $output->writeln("   [!] Could not generate output consensus.");
            return;
        }

        $briefs = $oc['briefs'] ?? [];

        if (empty($briefs)) {
            $output->writeln("   [!] No play briefs parsed from LLM output.");
            // Show raw output for debugging
            if (!empty($oc['raw'])) {
                $output->writeln("   [RAW] " . substr($oc['raw'], 0, 300) . "...");
            }
            return;
        }

        $output->writeln('');
        $output->writeln('  ╔══════════════════════════════════════════════╗');
        $output->writeln('  ║           LOGIRI PLAY BRIEFS                 ║');
        $output->writeln('  ╚══════════════════════════════════════════════╝');

        foreach ($briefs as $i => $brief) {
            $n = $i + 1;
            $output->writeln('');
            $output->writeln("  ── Play Brief #{$n} ────────────────────────────");
            $output->writeln("  PLAY:     " . ($brief['title'] ?? '(no title)'));
            $output->writeln("  URL:      " . ($brief['url'] ?? '(no url)'));
            $output->writeln("  PRIORITY: " . ($brief['priority'] ?? 'High'));
            $output->writeln('');

            if ($brief['current_state'] ?? '') {
                $output->writeln("  CURRENT STATE:");
                foreach (explode("\n", $brief['current_state']) as $line) {
                    $line = trim($line);
                    if ($line) $output->writeln("    " . $line);
                }
                $output->writeln('');
            }

            if ($brief['your_move'] ?? '') {
                $output->writeln("  YOUR MOVE:");
                foreach (explode("\n", $brief['your_move']) as $line) {
                    $line = trim($line);
                    if ($line) $output->writeln("    " . $line);
                }
                $output->writeln('');
            }

            if ($brief['done_when'] ?? '') {
                $output->writeln("  DONE WHEN: " . $brief['done_when']);
            }

            $recheck = trim(str_ireplace('days', '', $brief['recheck'] ?? '14'));
            $output->writeln("  RECHECK:   {$recheck} days");

            if (($brief['caveat'] ?? '') && strtolower($brief['caveat']) !== 'none') {
                $output->writeln("  ⚠ CAVEAT:  " . $brief['caveat']);
            }
        }

        $output->writeln('');
        $output->writeln("  " . count($briefs) . " play brief(s) generated in " . ($oc['rounds_run'] ?? '?') . " round(s) | avg confidence: " . ($oc['avg_conf'] ?? '?') . "/10");
        $output->writeln('  ──────────────────────────────────────────────');
    }

    // ─────────────────────────────────────────────
    //  DISPLAY STAGE 1 VALIDATION RESULTS
    // ─────────────────────────────────────────────

    private function displayValidationResults(OutputInterface $output, array $verdicts, array $consensus, array $allRounds, int $roundsRun): void
    {
        $icon = $consensus['status'] === 'VALIDATED' ? '[PASS]' : '[FLAG]';
        $output->writeln('');
        $output->writeln("  {$icon} Rule Validation: {$consensus['status']} (avg conf: {$consensus['avg_conf']}/10) after {$roundsRun} round(s)");

        foreach ($verdicts as $llm => $v) {
            $vIcon   = $v['verdict'] === 'PASS' ? '[PASS]' : '[FLAG]';
            $changed = '';
            if ($roundsRun > 1 && isset($allRounds[1][$llm]) && $allRounds[1][$llm]['verdict'] !== $v['verdict']) {
                $changed = " [changed from {$allRounds[1][$llm]['verdict']} in R1]";
            }
            $output->writeln("    {$vIcon} " . strtoupper($llm) . ": {$v['verdict']} ({$v['confidence']}/10){$changed}");
            if ($v['summary']) $output->writeln("       -> " . $v['summary']);
            if ($v['needs_change'] === 'yes' && $v['suggested'] !== 'none') {
                $output->writeln("       SUGGESTED CHANGE: " . $v['suggested']);
            }
        }

        if ($roundsRun > 1) {
            $output->writeln("  Deliberation: " . implode(' → ', array_map(fn($r, $vs) =>
                "R{$r}[" . implode('|', array_map(fn($l, $v) => strtoupper($l[0]) . ':' . $v['verdict'], array_keys($vs), $vs)) . "]",
                array_keys($allRounds), $allRounds
            )));
        }
    }

    // ─────────────────────────────────────────────
    //  FIELD EXTRACTION HELPERS
    // ─────────────────────────────────────────────

    private function extractField(string $text, string $field): string
    {
        // Multi-line field: capture until next ALL_CAPS field or end
        if (preg_match('/' . preg_quote($field, '/') . '\s*:\s*(.*?)(?=\n[A-Z][A-Z_]{2,}\s*:|$)/s', $text, $m)) {
            return trim($m[1]);
        }
        // Single line fallback
        if (preg_match('/' . preg_quote($field, '/') . '\s*:\s*(.+)/i', $text, $m)) {
            return trim($m[1]);
        }
        return '';
    }

    private function extractPagesBlock(string $text): array
    {
        $pages = [];
        // Find PAGES: block
        if (preg_match('/PAGES\s*:\s*\n(.*?)(?=\n[A-Z][A-Z_]{2,}\s*:|$)/s', $text, $m)) {
            $lines = explode("\n", trim($m[1]));
            foreach ($lines as $line) {
                $line = trim($line);
                if ($line && !preg_match('/^[A-Z][A-Z_]{2,}\s*:/', $line)) {
                    $pages[] = ltrim($line, '-•* ');
                }
            }
        }
        return $pages;
    }

    // ─────────────────────────────────────────────
    //  BUILD DELIBERATION PROMPT (Rounds 2 & 3)
    // ─────────────────────────────────────────────

    private function buildDeliberationPrompt(string $basePrompt, array $allRounds, int $currentRound): string
    {
        $prevRound    = $currentRound - 1;
        $prevVerdicts = $allRounds[$prevRound] ?? [];
        $isFinal      = ($currentRound === self::MAX_ROUNDS);

        $peer  = "\n\n" . str_repeat('=', 60) . "\n";
        $peer .= "PEER REVIEW -- Round {$prevRound} responses from your fellow evaluators:\n";
        $peer .= str_repeat('=', 60) . "\n";

        foreach ($prevVerdicts as $llm => $v) {
            $peer .= "\n" . strtoupper($llm) . ": {$v['verdict']} (confidence: {$v['confidence']}/10)\n";
            if ($v['summary'])  $peer .= "  Summary: {$v['summary']}\n";
            if ($v['needs_change'] === 'yes' && $v['suggested'] !== 'none') {
                $peer .= "  Suggested: {$v['suggested']}\n";
            }
        }

        $peer .= "\n" . str_repeat('=', 60) . "\n";
        $peer .= "ROUND {$currentRound} INSTRUCTIONS:\n";

        if ($isFinal) {
            $peer .= "This is the FINAL round. Commit to your final position.\n";
            $peer .= "If changing your response, explain which peer argument convinced you.\n";
            $peer .= "If maintaining your response, state that clearly.\n";
        } else {
            $peer .= "Consider the peer responses above. You may revise or maintain your position.\n";
            $peer .= "If you revise, explain which peer argument convinced you.\n";
        }

        $peer .= "Respond in the SAME structured format as before.\n";
        $peer .= str_repeat('=', 60);

        return $basePrompt . $peer;
    }

    // ─────────────────────────────────────────────
    //  MAJORITY VOTE
    // ─────────────────────────────────────────────

    private function determineMajority(array $finalVerdicts, array $allRounds): array
    {
        $passes = $flags = $totalConf = $count = $changed = 0;

        foreach ($finalVerdicts as $v) {
            if ($v['verdict'] === 'PASS') $passes++;
            if ($v['verdict'] === 'FLAG') $flags++;
            if ($v['confidence'] > 0) { $totalConf += $v['confidence']; $count++; }
        }

        foreach (array_keys($finalVerdicts) as $llm) {
            $r1 = $allRounds[1][$llm]['verdict'] ?? 'UNKNOWN';
            $rN = $finalVerdicts[$llm]['verdict'] ?? 'UNKNOWN';
            if ($r1 !== $rN) $changed++;
        }

        $avgConf = $count > 0 ? round($totalConf / $count, 1) : 0;
        $rounds  = self::MAX_ROUNDS;

        if ($passes > $flags) {
            $status = 'VALIDATED';
            $reason = "Majority PASS after {$rounds} rounds ({$passes} pass, {$flags} flag). {$changed} LLM(s) changed position.";
        } elseif ($flags > $passes) {
            $status = 'FLAGGED';
            $reason = "Majority FLAG after {$rounds} rounds ({$flags} flag, {$passes} pass). {$changed} LLM(s) changed position.";
        } else {
            $status = 'NEEDS_HUMAN_REVIEW';
            $reason = "Deadlock after {$rounds} rounds. Human review required.";
        }

        return ['status' => $status, 'passes' => $passes, 'flags' => $flags, 'avg_conf' => $avgConf, 'reason' => $reason, 'majority' => true];
    }

    // ─────────────────────────────────────────────
    //  LOAD RULES
    // ─────────────────────────────────────────────

    private function loadRules(): array
    {
        // PRIMARY: Load rules from seo_rules database table
        try {
            $dbRules = $this->db->fetchAllAssociative(
                "SELECT * FROM seo_rules WHERE is_active = TRUE ORDER BY rule_id"
            );
            if (!empty($dbRules)) {
                $rules = [];
                foreach ($dbRules as $row) {
                    $rules[] = [
                        'id'                => $row['rule_id'],
                        'name'              => $row['name'],
                        'full_text'         => $row['full_text'],
                        'trigger_source'    => $row['trigger_source'],
                        'trigger_condition' => $row['trigger_condition'],
                        'trigger_sql'       => $row['trigger_sql'],
                        'threshold'         => $row['threshold'],
                        'diagnosis'         => $row['diagnosis'],
                        'priority'          => $row['priority'],
                        'assigned'          => $row['assigned'],
                    ];
                }
                return $rules;
            }
        } catch (\Exception $e) {
            // Table doesn't exist yet — fall through to file
        }

        // FALLBACK: Parse from system-prompt.txt (for first deploy before seed runs)
        // If we get rules from the file, auto-seed them into the DB so future runs use the table
        $promptPath = dirname(__DIR__, 2) . '/system-prompt.txt';
        if (!file_exists($promptPath)) return [];

        $content = file_get_contents($promptPath);
        $rules   = [];

        preg_match_all('/\n([A-Z][A-Z0-9]+(?:-[A-Z0-9]+)*-[A-Z]?\d+[a-z]?)\s*\|\s*([^\n]+)\n(.*?)(?=\n[A-Z][A-Z0-9]+(?:-[A-Z0-9]+)*-[A-Z]?\d+[a-z]?\s*\||\nSECTION\s+\d+|\nRESULTS VERIFICATION|\n={10,}|\z)/s', $content, $matches, PREG_SET_ORDER);

        foreach ($matches as $match) {
            $ruleText         = trim($match[3]);
            $triggerCondition = '';
            $triggerSql       = '';
            $triggerSource    = '';
            $diagnosis        = '';
            $priority         = '';
            $assigned         = '';
            $threshold        = '';

            if (preg_match('/Trigger Source:\s*([^\n]+)/', $ruleText, $m)) $triggerSource = trim($m[1]);
            if (preg_match('/Trigger Condition:\s*(.*?)(?=\nThreshold:|$)/s', $ruleText, $m)) {
                $triggerCondition = trim($m[1]);
                $triggerSql = $triggerCondition;
                $triggerSql = preg_replace('/```sql\s*/', '', $triggerSql);
                $triggerSql = preg_replace('/```\s*/', '', $triggerSql);
                $triggerSql = trim($triggerSql);
            }
            if (preg_match('/Threshold:\s*(.*?)(?=\nCrawl Parameter:|$)/s', $ruleText, $m)) $threshold = trim($m[1]);
            if (preg_match('/Diagnosis:\s*(.*?)(?=\nAction Output:|$)/s',   $ruleText, $m)) $diagnosis = trim($m[1]);
            if (preg_match('/Priority:\s*([^\n]+)/',                         $ruleText, $m)) $priority  = trim($m[1]);
            if (preg_match('/Assigned:\s*([^\n]+)/',                         $ruleText, $m)) $assigned  = trim($m[1]);

            $rule = [
                'id'                => trim($match[1]),
                'name'              => trim($match[2]),
                'full_text'         => $ruleText,
                'trigger_source'    => $triggerSource,
                'trigger_condition' => $triggerCondition,
                'trigger_sql'       => $triggerSql,
                'threshold'         => $threshold,
                'diagnosis'         => $diagnosis,
                'priority'          => $priority,
                'assigned'          => $assigned,
            ];
            $rules[] = $rule;

            // Auto-seed into DB so future runs use the table
            try {
                $this->db->executeStatement(
                    "INSERT INTO seo_rules (rule_id, name, trigger_source, trigger_condition, trigger_sql, threshold, diagnosis, priority, assigned, full_text, updated_by)
                     VALUES (:rid, :name, :ts, :tc, :tsql, :thr, :diag, :pri, :asgn, :ft, 'auto-seed')
                     ON CONFLICT (rule_id) DO NOTHING",
                    [
                        'rid'  => $rule['id'],
                        'name' => $rule['name'],
                        'ts'   => $triggerSource,
                        'tc'   => $triggerCondition,
                        'tsql' => $triggerSql,
                        'thr'  => $threshold,
                        'diag' => $diagnosis,
                        'pri'  => $priority,
                        'asgn' => $assigned,
                        'ft'   => $ruleText,
                    ]
                );
            } catch (\Exception $e) {
                // Non-fatal — seeding failure doesn't block evaluation
            }
        }

        return $rules;
    }

    // ─────────────────────────────────────────────
    //  GET FIRING PAGES
    // ─────────────────────────────────────────────

    private function getFiringPages(array $rule): array
    {
        try {
            $af = self::ASSET_FILTER;
            $utilExclude = "AND is_utility IS NOT TRUE AND url NOT LIKE '%thank-you%' AND url NOT LIKE '%thank_you%' AND url NOT LIKE '%thanks%' AND url NOT LIKE '%-submit%' AND url NOT LIKE '%-confirmation%' AND url NOT LIKE '%prize-wheel%' AND url NOT LIKE '%payment-failed%' AND url NOT LIKE '%payment-success%' AND url NOT LIKE '%terms-of-use%' AND url NOT LIKE '%privacy-policy%'";

            if (($rule['id'] ?? '') === 'MAO-R6') {
                return [];
            }

            // Try to extract executable SQL from the rule's trigger condition
            $sql = $rule['trigger_sql'] ?? '';
            $ruleId = $rule['id'];
            $tc = $rule['trigger_condition'] ?? '';

            // ── SIMPLIFIED QUERIES (PRIMARY) ──
            // These are hand-tuned queries with proper relevance filters,
            // dedup guards, and accurate column references. Use these first.
            $simplifiedQuery = $this->getSimplifiedQuery($ruleId, $tc);
            if ($simplifiedQuery) {
                // Inject global media exclusion into every query
                $simplifiedQuery = preg_replace(
                    '/\bLIMIT\b/i',
                    "AND url NOT LIKE '%/wp-content/%' AND url NOT LIKE '%.png' AND url NOT LIKE '%.jpg' AND url NOT LIKE '%.jpeg' AND url NOT LIKE '%.gif' AND url NOT LIKE '%.pdf' AND url NOT LIKE '%.svg' LIMIT",
                    $simplifiedQuery
                );
                $simplifiedQuery = $this->applyLatestSnapshotScope($simplifiedQuery);
                try {
                    $results = $this->filterNonActionableRows($this->db->fetchAllAssociative($simplifiedQuery));
                    if (!empty($results)) return $results;
                } catch (\Exception $e) {
                    // Simplified query failed — fall through to trigger_sql
                }
            }

            // ── TRIGGER_SQL FALLBACK ──
            // Only used if no simplified query exists or it returned empty
            if ($sql && preg_match('/^\s*SELECT\s/i', $sql)) {
                // Ensure LIMIT is present
                if (!preg_match('/LIMIT\s+\d+/i', $sql)) {
                    $sql .= ' LIMIT 15';
                }
                $sql = $this->applyLatestSnapshotScope($sql);
                try {
                    $results = $this->filterNonActionableRows($this->db->fetchAllAssociative($sql));
                    if (!empty($results)) return $results;
                } catch (\Exception $e) {
                    // SQL failed (missing columns/tables) — fall through to legacy
                }
            }

            // Fallback: build query from trigger_condition field (simple field = value conditions)
            $tc = $rule['trigger_condition'];

            // Legacy FC-R rules (backward compatibility)
            $t4 = self::TIER4_URLS;
            $query = match($rule['id']) {
                'FC-R1'  => "SELECT url, page_type, h1, title_tag, word_count, has_central_entity, central_entity_count FROM page_crawl_snapshots WHERE has_central_entity IS NOT TRUE AND word_count > 0 AND is_noindex IS NOT TRUE AND {$af} AND url NOT IN ({$t4}) {$utilExclude} LIMIT 10",
                'FC-R2'  => "SELECT url, page_type, h1, title_tag FROM page_crawl_snapshots WHERE (page_type IS NULL OR page_type NOT IN ('core','outer','utility')) AND is_noindex IS NOT TRUE AND {$af} LIMIT 10",
                'FC-R3'  => "SELECT url, page_type, word_count, h1, title_tag FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND word_count < 500 AND is_noindex IS NOT TRUE LIMIT 10",
                'FC-R5'  => "SELECT url, page_type, has_core_link, core_links_found FROM page_crawl_snapshots WHERE page_type = 'outer' AND has_core_link IS NOT TRUE AND is_noindex IS NOT TRUE AND (target_query IS NOT NULL OR target_query_impressions > 0) AND {$af} AND url NOT IN ({$t4}) {$utilExclude} LIMIT 10",
                'FC-R6'  => "SELECT url, page_type, word_count, h2s, schema_types FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND word_count < 800 AND is_noindex IS NOT TRUE LIMIT 10",
                'FC-R7'  => "SELECT url, page_type, h1, title_tag, h1_matches_title FROM page_crawl_snapshots WHERE (h1_matches_title IS NOT TRUE OR h1 IS NULL OR h1 = '') AND is_noindex IS NOT TRUE AND {$af} AND url NOT IN ({$t4}) {$utilExclude} LIMIT 10",
                'FC-R8'  => "SELECT url, page_type, h2s, word_count FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND (h2s IS NULL OR h2s = '[]' OR h2s = '') AND is_noindex IS NOT TRUE AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R9'  => "SELECT url, page_type, schema_types, h1 FROM page_crawl_snapshots WHERE page_type = 'core' AND (schema_types IS NULL OR schema_types = '[]' OR schema_types = '') AND is_noindex IS NOT TRUE AND url NOT LIKE '%//' AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R10' => "SELECT p.url, p.page_type, p.has_core_link, MAX(g.impressions) as impressions FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.has_core_link IS NOT TRUE AND g.impressions >= 100 AND g.date_range = '28d' GROUP BY p.url, p.page_type, p.has_core_link ORDER BY impressions DESC LIMIT 10",
                default  => null,
            };

            if ($query) {
                return $this->filterNonActionableRows($this->db->fetchAllAssociative($this->applyLatestSnapshotScope($query)));
            }

            // If no SQL and no legacy match, the trigger_condition is likely a bare WHERE clause
            // (e.g., "page_type IN ('core') AND (word_count = 0 OR word_count IS NULL) AND is_noindex = FALSE")
            // Determine the source table from the rule's trigger_source field or default to page_crawl_snapshots
            if ($tc) {
                $tc = trim($tc);
                // Strip any "WHERE" prefix if present
                $where = preg_replace('/^\s*WHERE\s+/i', '', $tc);
                // Strip any LIMIT clause
                $where = preg_replace('/\s+LIMIT\s+\d+/i', '', $where);

                // Determine table from trigger_source
                $triggerSource = strtolower($rule['trigger_source'] ?? '');
                $needsJoin = str_contains($triggerSource, 'gsc_snapshots');

                if ($needsJoin) {
                    // JOIN query for rules that need GSC data
                    return $this->filterNonActionableRows($this->db->fetchAllAssociative($this->applyLatestSnapshotScope(
                        "SELECT p.url, p.page_type, p.word_count, p.h1, p.title_tag, p.has_central_entity,
                                p.central_entity_count, p.schema_types, p.h1_matches_title, p.h2s,
                                p.has_core_link, p.canonical_url, p.is_noindex,
                                g.impressions, g.clicks, g.position, g.ctr
                         FROM page_crawl_snapshots p
                         LEFT JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url)
                         WHERE {$where}
                         LIMIT 15"
                    )));
                }

                // Default: page_crawl_snapshots only
                return $this->filterNonActionableRows($this->db->fetchAllAssociative($this->applyLatestSnapshotScope(
                    "SELECT url, page_type, word_count, h1, title_tag, has_central_entity,
                            central_entity_count, schema_types, h1_matches_title, h2s,
                            has_core_link, canonical_url, is_noindex
                     FROM page_crawl_snapshots
                     WHERE {$where}
                     LIMIT 15"
                )));
            }

            return [];
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  GET CORE PAGE LIST (for Stage 2 prompt context)
    // ─────────────────────────────────────────────

    private function getCorePageList(): array
    {
        try {
            return $this->db->fetchAllAssociative(
                "WITH latest_page_crawl_snapshots AS (
                    SELECT *
                    FROM (
                        SELECT pcs.*,
                               ROW_NUMBER() OVER (PARTITION BY pcs.url ORDER BY pcs.crawled_at DESC, pcs.id DESC) AS rn
                        FROM page_crawl_snapshots pcs
                    ) ranked
                    WHERE rn = 1
                )
                SELECT url, title_tag
                FROM latest_page_crawl_snapshots
                WHERE page_type = 'core'
                ORDER BY url"
            );
        } catch (\Exception $e) {
            return [];
        }
    }

    private function applyLatestSnapshotScope(string $sql): string
    {
        if (!str_contains($sql, 'page_crawl_snapshots') || str_contains($sql, 'latest_page_crawl_snapshots')) {
            return $sql;
        }

        $scopedSql = str_replace('page_crawl_snapshots', 'latest_page_crawl_snapshots', $sql);

        return "WITH latest_page_crawl_snapshots AS (
                    SELECT *
                    FROM (
                        SELECT pcs.*,
                               ROW_NUMBER() OVER (PARTITION BY pcs.url ORDER BY pcs.crawled_at DESC, pcs.id DESC) AS rn
                        FROM page_crawl_snapshots pcs
                    ) ranked
                    WHERE rn = 1
                )
                {$scopedSql}";
    }

    private function filterNonActionableRows(array $rows): array
    {
        return array_values(array_filter($rows, function (array $row): bool {
            $candidateUrl = null;
            foreach (['url', 'page'] as $key) {
                if (!empty($row[$key]) && is_string($row[$key])) {
                    $candidateUrl = $row[$key];
                    if ($this->isAssetLikeUrl($candidateUrl)) {
                        return false;
                    }
                }
            }

            return $this->isDdtTopicallyRelevantRow($row, $candidateUrl);
        }));
    }

    private function isAssetLikeUrl(string $url): bool
    {
        $path = parse_url($url, PHP_URL_PATH) ?: $url;
        $path = strtolower($path);

        if (str_contains($path, '/wp-content/uploads/')) {
            return true;
        }

        return (bool) preg_match('/\.(png|jpe?g|gif|svg|webp|avif|pdf|docx?|xlsx?|zip|mp4|mov|avi|wmv|webm)$/i', $path);
    }

    private function isDdtTopicallyRelevantRow(array $row, ?string $candidateUrl): bool
    {
        $pageType = strtolower((string) ($row['page_type'] ?? ''));
        if ($pageType === 'utility') {
            return false;
        }

        $urlPath = strtolower((string) (parse_url($candidateUrl ?? '', PHP_URL_PATH) ?: ($candidateUrl ?? '')));
        if ($urlPath === '/' || str_contains($urlPath, '/horse-') || str_contains($urlPath, '/trailer') || str_contains($urlPath, '/gooseneck') || str_contains($urlPath, '/bumper-pull') || str_contains($urlPath, '/living-quarters') || str_contains($urlPath, '/dealer') || str_contains($urlPath, '/review') || str_contains($urlPath, '/testimonial') || str_contains($urlPath, '/about') || str_contains($urlPath, '/contact')) {
            return true;
        }

        if ($pageType === 'core') {
            return true;
        }

        $haystack = strtolower(implode(' ', array_filter([
            $urlPath,
            (string) ($row['title_tag'] ?? ''),
            (string) ($row['h1'] ?? ''),
            (string) ($row['target_query'] ?? ''),
            (string) ($row['body_text_snippet'] ?? ''),
        ])));

        $positiveTerms = [
            'horse trailer',
            'horse trailers',
            'gooseneck',
            'bumper pull',
            'living quarters',
            'slant load',
            'straight load',
            'safetack',
            'safe tack',
            'safebump',
            'safebump',
            'safekick',
            'safekick',
            'z-frame',
            'z frame',
            'zframe',
            'box stall',
            'reverse load',
            'horse owner',
            'equine',
            'tack room',
            'trailer safety',
        ];

        foreach ($positiveTerms as $term) {
            if (str_contains($haystack, $term)) {
                return true;
            }
        }

        $negativeTerms = [
            '3d printing',
            'additive manufacturing',
            'manufacturing process',
            'rapid prototyping',
        ];

        foreach ($negativeTerms as $term) {
            if (str_contains($haystack, $term)) {
                return false;
            }
        }

        return false;
    }

    // ─────────────────────────────────────────────
    //  SIMPLIFIED FALLBACK QUERIES
    //  When a rule's SQL references columns that don't exist,
    //  this provides a working query using only available fields.
    // ─────────────────────────────────────────────

    private function getSimplifiedQuery(string $ruleId, string $triggerCondition): ?string
    {
        $cols = "url, page_type, word_count, h1, title_tag, has_central_entity, central_entity_count, schema_types, h1_matches_title, h2s, has_core_link, canonical_url, is_noindex, internal_links, internal_link_count, body_internal_links, body_internal_link_count, body_link_extraction_confident, body_link_extraction_scope, target_query, target_query_impressions, target_query_position";

        // Relevance filter — exclude pages with zero GSC presence unless they're core product pages
        // Also exclude wp-content/media assets that shouldn't be in the table at all
        $relevanceFilter = "AND (page_type = 'core' OR target_query IS NOT NULL OR target_query_impressions > 0) AND url NOT LIKE '%/wp-content/%' AND url NOT LIKE '%.png' AND url NOT LIKE '%.jpg' AND url NOT LIKE '%.pdf'";

        // Media exclusion — always applied, even to core-only queries
        $noMedia = "AND url NOT LIKE '%/wp-content/%' AND url NOT LIKE '%.png' AND url NOT LIKE '%.jpg' AND url NOT LIKE '%.jpeg' AND url NOT LIKE '%.gif' AND url NOT LIKE '%.pdf' AND url NOT LIKE '%.svg'";

        return match($ruleId) {
            // Entity & Topical Authority
            'ETA-01' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND has_central_entity = FALSE AND is_noindex = FALSE LIMIT 15",
            'ETA-02' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND has_central_entity = FALSE AND word_count > 0 AND is_noindex = FALSE LIMIT 15",
            'ETA-03' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND is_noindex = FALSE AND (has_zframe_mention = FALSE AND has_safetack_mention = FALSE AND has_safebump_mention = FALSE AND has_safekick_mention = FALSE) LIMIT 15",
            'ETA-04' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND schema_types NOT LIKE '%Product%' AND schema_types NOT LIKE '%Organization%' AND is_noindex = FALSE LIMIT 15",
            'ETA-05' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND word_count < 1000 AND word_count > 0 AND is_noindex = FALSE AND is_utility = FALSE {$relevanceFilter} LIMIT 15",
            'ETA-06' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 500 AND is_noindex = FALSE LIMIT 15",

            // E-E-A-T & Trust Signals
            'DDT-EEAT-03' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND schema_types NOT LIKE '%Review%' AND schema_types NOT LIKE '%AggregateRating%' AND is_noindex = FALSE LIMIT 15",
            'DDT-EEAT-04' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url LIKE '%/about%' AND (word_count < 1000 OR schema_types NOT LIKE '%Organization%') LIMIT 15",
            'DDT-EEAT-05' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%privacy%' OR url LIKE '%terms%') AND (word_count < 1000 OR is_noindex = TRUE) LIMIT 15",
            'DDT-EEAT-06' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url LIKE '%contact%' AND (schema_types NOT LIKE '%LocalBusiness%' OR schema_types NOT LIKE '%ContactPoint%') LIMIT 15",
            'DDT-EEAT-07' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND word_count >= 1000 AND is_noindex = FALSE AND is_utility = FALSE {$relevanceFilter} LIMIT 15",
            'DDT-EEAT-08' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND has_zframe_mention = FALSE AND is_noindex = FALSE LIMIT 15",

            // Schema & Structured Data
            'DDT-SD-002' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url = '/' AND schema_types NOT LIKE '%Organization%' AND is_noindex = FALSE LIMIT 1",
            'DDT-SD-003' => "SELECT p.url, p.page_type, p.word_count, p.schema_types, p.h1, p.target_query, g.impressions FROM page_crawl_snapshots p LEFT JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.word_count >= 1000 AND p.schema_types NOT LIKE '%FAQPage%' AND p.is_noindex = FALSE AND p.is_utility = FALSE AND g.impressions > 800 ORDER BY g.impressions DESC LIMIT 15",
            'DDT-SD-004' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count <= 500 AND schema_types NOT LIKE '%AggregateRating%' AND is_noindex = FALSE LIMIT 15",
            'DDT-SD-005' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url != '/' AND page_type NOT IN ('utility') AND schema_types NOT LIKE '%BreadcrumbList%' AND is_noindex = FALSE AND is_utility = FALSE {$relevanceFilter} LIMIT 15",
            'DDT-SD-006' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url IN ('/', '/horse-trailers/', '/gooseneck-horse-trailers/', '/bumper-pull-horse-trailers/', '/living-quarters-horse-trailers/') AND schema_types NOT LIKE '%ProductGroup%' LIMIT 15",

            // Local & Dealer SEO
            'DDT-LOCAL-01' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%dealer%' OR url LIKE '%location%') AND schema_types NOT LIKE '%LocalBusiness%' AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",
            'DDT-LOCAL-02' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%dealer%' OR url LIKE '%location%') AND (word_count < 100 OR h1 IS NULL OR h1 = '') AND is_noindex = FALSE LIMIT 15",
            'DDT-LOCAL-03' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%dealer%' OR url LIKE '%location%') AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",
            'DDT-LOCAL-04' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND (title_tag LIKE '%dealer%' OR title_tag LIKE '%trailer%in%' OR h1 LIKE '%dealer%') AND word_count < 1000 AND is_noindex = FALSE AND is_utility = FALSE {$relevanceFilter} LIMIT 15",
            'DDT-LOCAL-05' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%dealer%' OR url LIKE '%location%') AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",

            // User Signals & Engagement (GSC joins — already have impressions filter = relevance built in)
            'USE-R1' => "SELECT p.url, p.page_type, p.word_count, p.h1, p.h1_matches_title, p.has_central_entity, p.target_query, g.impressions, g.clicks, g.ctr, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'core' AND p.is_utility = FALSE AND g.impressions >= 500 AND g.position <= 15 AND g.ctr < 0.08 ORDER BY g.impressions DESC LIMIT 15",
            'USE-R2' => "SELECT p.url, p.page_type, p.word_count, p.h1, p.target_query, g.impressions, g.clicks, g.ctr, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.is_utility = FALSE AND g.impressions >= 1000 AND g.ctr < 0.01 ORDER BY g.impressions DESC LIMIT 15",
            'USE-R3' => "SELECT p.url, p.page_type, p.word_count, p.has_central_entity, p.h1_matches_title, p.target_query, g.position, g.clicks, g.impressions FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'core' AND g.position <= 10 AND g.clicks >= 5 AND p.word_count < 150 ORDER BY g.clicks DESC LIMIT 15",
            'USE-R4' => "SELECT p.url, p.page_type, p.word_count, p.h1, p.h2s, p.target_query, g.impressions, g.clicks, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.is_utility = FALSE AND p.word_count < 1000 AND g.impressions >= 500 AND g.position <= 30 ORDER BY g.impressions DESC LIMIT 15",
            'USE-R5' => "SELECT p.url, p.page_type, p.word_count, p.h2s, p.h1_matches_title, p.target_query, g.impressions FROM page_crawl_snapshots p LEFT JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type IN ('core', 'outer') AND p.is_utility = FALSE AND (p.h2s IS NULL OR p.h2s = '[]' OR p.h2s = '') AND p.word_count > 300 AND p.is_noindex = FALSE ORDER BY g.impressions DESC NULLS LAST LIMIT 15",
            'USE-R6' => "SELECT p.url, p.page_type, p.h1, p.title_tag, p.meta_description, p.target_query, g.impressions, g.clicks, g.ctr, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.is_utility = FALSE AND g.impressions >= 300 AND g.position <= 20 AND g.ctr < 0.025 ORDER BY g.impressions DESC LIMIT 15",
            'USE-R7' => "SELECT p.url, p.page_type, p.has_central_entity, p.word_count, p.schema_types, p.h1_matches_title, p.target_query, g.impressions, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.has_central_entity = FALSE AND g.impressions >= 200 AND g.position <= 20 AND p.page_type IN ('core', 'outer') AND p.is_noindex = FALSE AND p.is_utility = FALSE ORDER BY g.impressions DESC LIMIT 15",

            // Keyword & Intent Alignment (GSC)
            'KIA-R2' => "SELECT query, COUNT(DISTINCT page) AS page_count, SUM(impressions) as total_imp, AVG(position) as avg_pos FROM gsc_snapshots WHERE position <= 20 GROUP BY query HAVING COUNT(DISTINCT page) > 1 AND SUM(impressions) > 100 ORDER BY total_imp DESC LIMIT 15",
            'KIA-R3' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND (word_count = 0 OR has_central_entity = FALSE OR h1_matches_title = FALSE) AND is_noindex = FALSE LIMIT 15",
            'KIA-R4' => "SELECT g.query, SUM(g.impressions) as total_imp, AVG(g.position) as avg_pos FROM gsc_snapshots g WHERE g.impressions > 5000 AND g.position > 10 AND (g.query LIKE '%horse trailer%' OR g.query LIKE '%gooseneck%' OR g.query LIKE '%z-frame%' OR g.query LIKE '%safetack%') GROUP BY g.query ORDER BY total_imp DESC LIMIT 15",
            'KIA-R5' => "SELECT g.query, SUM(g.impressions) as total_imp, AVG(g.position) as avg_pos FROM gsc_snapshots g WHERE (g.query LIKE '%2 horse%' OR g.query LIKE '%3 horse%' OR g.query LIKE '%gooseneck%' OR g.query LIKE '%safetack%') AND g.impressions > 100 AND g.position > 30 GROUP BY g.query ORDER BY total_imp DESC LIMIT 15",
            'KIA-R6' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND word_count < 1000 AND word_count > 0 AND is_noindex = FALSE AND is_utility = FALSE AND h1 IS NOT NULL AND h1 <> '' AND title_tag IS NOT NULL AND title_tag <> '' AND body_text_snippet IS NOT NULL AND body_text_snippet <> '' {$relevanceFilter} LIMIT 15",
            'KIA-R7' => "SELECT g.query, SUM(g.impressions) as total_imp, AVG(g.position) as avg_pos FROM gsc_snapshots g WHERE (g.query LIKE '%benefits%' OR g.query LIKE '%vs%' OR g.query LIKE '%safetack%') AND g.impressions > 500 AND g.position > 20 GROUP BY g.query ORDER BY total_imp DESC LIMIT 15",
            'KIA-R8' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND internal_link_count > 3 AND is_noindex = FALSE ORDER BY internal_link_count DESC LIMIT 15",

            // Competitive Intelligence (mostly GSC-based — already filtered by impressions)
            'CI-R1' => "SELECT g.query, g.page, g.position, g.impressions, g.clicks, g.ctr FROM gsc_snapshots g JOIN page_crawl_snapshots p ON g.page LIKE CONCAT('%', p.url) WHERE g.position > 10 AND g.impressions > 500 AND p.page_type = 'core' ORDER BY g.impressions DESC LIMIT 15",
            'CI-R4' => "SELECT g.query, g.page, g.position, g.impressions, g.clicks FROM gsc_snapshots g WHERE g.impressions > 1000 ORDER BY g.impressions DESC LIMIT 15",
            'CI-R6' => "SELECT p.url, p.title_tag, p.meta_description, p.page_type, p.target_query, g.impressions, g.clicks, g.ctr, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE g.impressions > 5000 AND g.position < 10 AND g.ctr < 0.05 AND p.page_type IN ('core', 'outer') AND p.is_utility = FALSE ORDER BY g.impressions DESC LIMIT 15",

            // Content Freshness (already filtered by impressions)
            'CFL-04' => "SELECT p.url, p.word_count, p.page_type, p.target_query, g.impressions, g.clicks, g.position, g.ctr FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.is_utility = FALSE AND p.word_count >= 1000 AND p.is_noindex = FALSE AND g.impressions > 1000 AND g.position > 15 AND g.ctr < 0.02 ORDER BY g.impressions DESC LIMIT 15",

            // Media & Asset Optimization
            'MAO-R1' => "SELECT {$cols}, images_without_alt FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND images_without_alt > 0 AND is_noindex = FALSE ORDER BY images_without_alt DESC LIMIT 15",
            'MAO-R2' => "SELECT {$cols}, images_without_alt FROM page_crawl_snapshots WHERE page_type = 'outer' AND word_count >= 1000 AND images_without_alt > 0 AND is_noindex = FALSE AND is_utility = FALSE {$relevanceFilter} ORDER BY images_without_alt DESC LIMIT 15",
            'MAO-R4' => "SELECT {$cols}, has_main_content_video, video_metadata_valid, video_topic_aligned, video_urls, video_title FROM page_crawl_snapshots WHERE has_main_content_video = TRUE AND video_metadata_valid = TRUE AND video_topic_aligned = TRUE AND video_urls IS NOT NULL AND video_urls <> '' AND video_title IS NOT NULL AND video_title <> '' AND schema_types NOT LIKE '%VideoObject%' AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",
            'MAO-R6' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url LIKE '%.pdf' LIMIT 15",
            'MAO-R7' => "SELECT {$cols}, images_with_generic_alt FROM page_crawl_snapshots WHERE page_type IN ('core', 'outer') AND images_with_generic_alt > 0 AND is_noindex = FALSE AND is_utility = FALSE {$relevanceFilter} ORDER BY images_with_generic_alt DESC LIMIT 15",

            // Internal Link Architecture
            'ILA-004' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND is_noindex = FALSE AND url_depth > 3 ORDER BY url_depth DESC LIMIT 15",
            'ILA-005' => "SELECT {$cols} FROM page_crawl_snapshots WHERE body_link_extraction_confident = TRUE AND body_internal_link_count > 3 AND page_type IN ('core', 'outer') AND is_noindex = FALSE AND is_utility = FALSE {$relevanceFilter} ORDER BY body_internal_link_count DESC LIMIT 15",
            'ILA-006' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND is_noindex = FALSE AND has_core_link = FALSE LIMIT 15",
            'ILA-007' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND is_noindex = FALSE AND url_depth > 3 ORDER BY url_depth DESC LIMIT 15",

            // TECH rules
            'TECH-R1' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND is_noindex = TRUE AND http_status = 200 LIMIT 15",

            default => null,
        };
    }

    // ─────────────────────────────────────────────
    //  GET OWNER FEEDBACK (Jeanne's past reviews for this rule)
    // ─────────────────────────────────────────────

    private function getOwnerFeedback(string $ruleId): string
    {
        try {
            $reviews = $this->db->fetchAllAssociative(
                "SELECT verdict, feedback, corrections, reviewed_at, reviewed_by
                 FROM rule_reviews
                 WHERE rule_id = :rule_id
                 ORDER BY reviewed_at DESC
                 LIMIT 5",
                ['rule_id' => $ruleId]
            );

            if (empty($reviews)) return '';

            $lines = [];
            foreach ($reviews as $r) {
                $date    = $r['reviewed_at'] ? substr($r['reviewed_at'], 0, 10) : 'unknown';
                $by      = $r['reviewed_by'] ?? 'Unknown';
                $verdict = $r['verdict'] ?? 'no verdict';
                $fb      = $r['feedback'] ?? '';
                $corr    = $r['corrections'] ?? '';

                $line = "- [{$date}] {$by}: {$verdict}";
                if ($fb) $line .= " — {$fb}";
                if ($corr) {
                    $corrections = json_decode($corr, true);
                    if (is_array($corrections) && !empty($corrections)) {
                        $corrTexts = [];
                        foreach ($corrections as $c) {
                            $corrTexts[] = ($c['url'] ?? '?') . ' should be ' . ($c['override'] ?? '?') . ($c['reason'] ? " ({$c['reason']})" : '');
                        }
                        $line .= " | Corrections: " . implode('; ', $corrTexts);
                    }
                }
                $lines[] = $line;
            }

            return implode("\n", $lines);
        } catch (\Exception $e) {
            return '';
        }
    }

    // ─────────────────────────────────────────────
    //  GET OUTCOME FEEDBACK (what worked/didn't from past verifications)
    // ─────────────────────────────────────────────

    private function getOutcomeFeedback(string $ruleId): string
    {
        try {
            $feedback = $this->db->fetchAllAssociative(
                "SELECT outcome_status, what_worked, what_didnt_work, proposed_change, change_type, url, created_at
                 FROM rule_feedback
                 WHERE rule_id = :rule_id
                 ORDER BY created_at DESC
                 LIMIT 5",
                ['rule_id' => $ruleId]
            );

            if (empty($feedback)) return '';

            $lines = ["PAST OUTCOME FEEDBACK FOR THIS RULE:"];
            foreach ($feedback as $f) {
                $date   = $f['created_at'] ? substr($f['created_at'], 0, 10) : 'unknown';
                $status = $f['outcome_status'] ?? 'unknown';
                $url    = $f['url'] ?? '';
                $lines[] = "- [{$date}] {$status} on {$url}";
                if (!empty($f['what_worked']) && $f['what_worked'] !== 'N/A') {
                    $lines[] = "  What worked: {$f['what_worked']}";
                }
                if (!empty($f['what_didnt_work']) && $f['what_didnt_work'] !== 'N/A') {
                    $lines[] = "  What didn't work: {$f['what_didnt_work']}";
                }
                if (!empty($f['proposed_change'])) {
                    $lines[] = "  Proposed change ({$f['change_type']}): {$f['proposed_change']}";
                }
            }

            return implode("\n", $lines);
        } catch (\Exception $e) {
            return '';
        }
    }

    // ─────────────────────────────────────────────
    //  BRAND GLOSSARY (prevents hallucination in LLM output)
    // ─────────────────────────────────────────────

    private function getStructuredRejectionContext(string $ruleId, array $firingPages): string
    {
        if ($ruleId === '') {
            return '';
        }

        try {
            $urls = [];
            $pageTypes = [];
            foreach ($firingPages as $page) {
                $url = $this->normalizeUrl((string) ($page['url'] ?? ''));
                if ($url !== '') {
                    $urls[] = $url;
                }

                $pageType = trim((string) ($page['page_type'] ?? ''));
                if ($pageType !== '') {
                    $pageTypes[] = $pageType;
                }
            }

            $params = ['rule_id' => $ruleId];
            $types = [];
            $scopeClauses = ['rule_id = :rule_id'];

            if (!empty($urls)) {
                $params['urls'] = array_values(array_unique($urls));
                $types['urls'] = \Doctrine\DBAL\ArrayParameterType::STRING;
                $scopeClauses[] = 'url IN (:urls)';
            }

            if (!empty($pageTypes)) {
                $params['page_types'] = array_values(array_unique($pageTypes));
                $types['page_types'] = \Doctrine\DBAL\ArrayParameterType::STRING;
                $scopeClauses[] = 'page_type IN (:page_types)';
            }

            $rows = $this->db->executeQuery(
                "SELECT url, page_type, reason_code, guardrail_code, scope, reason_text, created_at
                 FROM task_rejections
                 WHERE (" . implode(' OR ', $scopeClauses) . ")
                 ORDER BY created_at DESC
                 LIMIT 8",
                $params,
                $types
            )->fetchAllAssociative();

            if (empty($rows)) {
                return '';
            }

            $lines = ['RECENT STRUCTURED TASK REJECTIONS:'];
            foreach ($rows as $row) {
                $date = !empty($row['created_at']) ? substr((string) $row['created_at'], 0, 10) : 'unknown';
                $subject = $row['url'] ?: (($row['page_type'] ?? '') !== '' ? $row['page_type'] . ' pages' : 'unknown scope');
                $lines[] = sprintf(
                    '- [%s] %s/%s on %s (%s): %s',
                    $date,
                    $row['reason_code'] ?? 'rejected',
                    $this->deriveGuardrailCodeFromRow($row),
                    $subject,
                    $row['scope'] ?? 'task_only',
                    trim((string) ($row['reason_text'] ?? 'No reason provided'))
                );
            }

            return implode("\n", $lines);
        } catch (\Exception $e) {
            return '';
        }
    }

    private function matchesStructuredRejectionGuardrail(array $brief): bool
    {
        $ruleId = trim((string) ($brief['rule_id'] ?? ''));
        $url = $this->normalizeUrl((string) ($brief['url'] ?? ''));
        $pageType = trim((string) ($brief['page_type'] ?? ''));

        if ($ruleId === '') {
            return false;
        }

        try {
            $rows = $this->fetchStructuredRejectionRows($ruleId, $url, $pageType);
            if (empty($rows)) {
                return false;
            }

            $pageTypeMatches = 0;
            $globalMatches = 0;

            foreach ($rows as $row) {
                if (!$this->structuredRejectionMatchesBrief($row, $brief)) {
                    continue;
                }

                $scope = (string) ($row['scope'] ?? 'task_only');
                if ($scope === 'url_only' && $url !== '' && (string) ($row['url'] ?? '') === $url) {
                    return true;
                }

                if ($scope === 'rule_page_type') {
                    $pageTypeMatches++;
                }

                if ($scope === 'rule_global') {
                    $globalMatches++;
                }
            }

            if ($pageTypeMatches >= 2 || $globalMatches >= 2) {
                return true;
            }

            return false;
        } catch (\Exception $e) {
            return false;
        }
    }

    private function fetchStructuredRejectionRows(string $ruleId, string $url, string $pageType): array
    {
        $params = ['rule_id' => $ruleId];
        $types = [];
        $scopeClauses = ['rule_id = :rule_id'];

        if ($url !== '') {
            $params['urls'] = [$url];
            $types['urls'] = \Doctrine\DBAL\ArrayParameterType::STRING;
            $scopeClauses[] = 'url IN (:urls)';
        }

        if ($pageType !== '') {
            $params['page_types'] = [$pageType];
            $types['page_types'] = \Doctrine\DBAL\ArrayParameterType::STRING;
            $scopeClauses[] = 'page_type IN (:page_types)';
        }

        return $this->db->executeQuery(
            "SELECT url, page_type, reason_code, guardrail_code, scope, reason_text, created_at
             FROM task_rejections
             WHERE (" . implode(' OR ', $scopeClauses) . ")
               AND created_at >= NOW() - INTERVAL '45 days'
             ORDER BY created_at DESC
             LIMIT 25",
            $params,
            $types
        )->fetchAllAssociative();
    }

    private function structuredRejectionMatchesBrief(array $row, array $brief): bool
    {
        $guardrailCode = $this->deriveGuardrailCodeFromRow($row);
        $yourMove = (string) ($brief['your_move'] ?? '');
        $combined = strtolower(trim(($brief['title'] ?? '') . "\n" . $yourMove . "\n" . ($brief['done_when'] ?? '') . "\n" . ($brief['current_state'] ?? '')));

        return match ($guardrailCode) {
            'no_video_on_page' => $this->requiresVideoPayload($combined),
            'missing_payload' => ($this->requiresSchemaPayload($combined) && !$this->containsSchemaPayload($yourMove))
                || ($this->requiresExactCopyPayload($combined) && !$this->containsExactCopyPayload($yourMove)),
            'vague_placement' => $this->hasVaguePlacementInstruction($yourMove) && !$this->containsQuotedPlacementAnchor($yourMove),
            'manual_serp_check' => $this->requiresManualSerpCheck($combined),
            'asset_or_bad_url' => $this->targetsAssetLikeUrl($brief),
            'page_type_mismatch', 'rule_scope_mismatch' => $this->briefViolatesStoredPagePolicy($brief, (string) ($row['page_type'] ?? '')),
            'duplicate_task' => trim((string) ($row['url'] ?? '')) !== '' && trim((string) ($row['url'] ?? '')) === trim((string) ($brief['url'] ?? '')),
            'rule_false_positive' => $this->targetsAssetLikeUrl($brief)
                || $this->requiresVideoPayload($combined)
                || $this->requiresManualSerpCheck($combined)
                || $this->briefViolatesStoredPagePolicy($brief, (string) ($row['page_type'] ?? '')),
            'crawl_data_mismatch', 'general_rejection' => false,
            default => false,
        };
    }

    private function deriveGuardrailCodeFromRow(array $row): string
    {
        $stored = trim((string) ($row['guardrail_code'] ?? ''));
        if ($stored !== '') {
            return $stored;
        }

        $reason = strtolower(trim((string) ($row['reason_text'] ?? '')));
        $reasonCode = strtolower(trim((string) ($row['reason_code'] ?? '')));

        if (str_contains($reason, 'no video') || str_contains($reason, 'no embed')) {
            return 'no_video_on_page';
        }

        if (str_contains($reason, 'play brief') || str_contains($reason, 'verbatim') || str_contains($reason, 'schema')) {
            return 'missing_payload';
        }

        if (str_contains($reason, 'generic') || str_contains($reason, 'specific')) {
            return 'vague_placement';
        }

        if (str_contains($reason, 'incognito') || str_contains($reason, 'featured snippet') || str_contains($reason, 'ai overview')) {
            return 'manual_serp_check';
        }

        if (str_contains($reason, '/wp-content/') || str_contains($reason, '.jpg') || str_contains($reason, '.png') || str_contains($reason, '.pdf')) {
            return 'asset_or_bad_url';
        }

        if (str_contains($reason, 'review') || str_contains($reason, '3d printing') || str_contains($reason, 'horse jockeys') || str_contains($reason, 'irrelevant')) {
            return 'page_type_mismatch';
        }

        if (str_contains($reason, 'crawl data') || str_contains($reason, 'fresh crawl') || str_contains($reason, 'stale data')) {
            return 'crawl_data_mismatch';
        }

        return match ($reasonCode) {
            'duplicate' => 'duplicate_task',
            'false_positive' => 'rule_false_positive',
            'invalid', 'not_applicable' => 'rule_scope_mismatch',
            default => 'general_rejection',
        };
    }

    private function requiresManualSerpCheck(string $combined): bool
    {
        return str_contains($combined, 'search incognito')
            || str_contains($combined, 'report back what you see')
            || str_contains($combined, 'featured snippet')
            || str_contains($combined, 'ai overview')
            || str_contains($combined, 'identify exactly who owns')
            || str_contains($combined, 'name the competitor');
    }

    private function targetsAssetLikeUrl(array $brief): bool
    {
        $url = strtolower(trim((string) ($brief['url'] ?? '')));
        if ($url === '') {
            return false;
        }

        return str_contains($url, '/wp-content/')
            || preg_match('/\.(jpg|jpeg|png|gif|webp|pdf)$/', $url) === 1;
    }

    private function briefViolatesStoredPagePolicy(array $brief, string $pageType): bool
    {
        $page = [
            'url' => $this->normalizeUrl((string) ($brief['url'] ?? '')),
            'page_type' => $pageType !== '' ? $pageType : (string) ($brief['page_type'] ?? ''),
            'title_tag' => (string) ($brief['title'] ?? ''),
            'h1' => '',
            'body_text_snippet' => trim((string) (($brief['current_state'] ?? '') . "\n" . ($brief['your_move'] ?? ''))),
            'word_count' => (int) ($brief['word_count'] ?? 0),
        ];

        $combined = strtolower(trim(($brief['title'] ?? '') . "\n" . ($brief['your_move'] ?? '') . "\n" . ($brief['done_when'] ?? '') . "\n" . ($brief['current_state'] ?? '')));

        return $this->violatesPageTypePolicy($combined, $brief, $page);
    }

    private function getBrandGlossary(): string
    {
        return <<<GLOSSARY
- Company: Double D Trailers (DDT), founded 1997 in Pink Hill NC, HQ Wilmington NC
- Website: https://www.doubledtrailers.com
- Construction: Z-Frame — high-tensile, zinc-infused material (NOT aluminum, NOT traditional steel)
- SafeTack: Patented reverse-load design with swing-out rear tack (NOT SafeTrack, NOT safe tack)
- SafeBump: Single-piece molded fiber composite roof reinforced with Z-Frame tubing every 16 inches (NOT SafeKill)
- SafeKick: Flexible wall panel made of recycled plastic and rubber compound
- DO NOT reference: aluminum, Z-Bar, SafeKill, or any invented product names or pricing
- Product pages: max 500 words body text. MSE elements carry the page.
- Outer pages: min 1000 words. Below that = thin content.
- Max 3 internal links per page. Zero external links.

VERIFIED EXTERNAL URLS (use ONLY these — do NOT invent or guess URLs):
- Facebook: https://www.facebook.com/DoubleDHorseTrailers
- Instagram: https://www.instagram.com/doubledtrailers
- YouTube: https://www.youtube.com/doubledtrailers
- LinkedIn: https://www.linkedin.com/company/double-d-horse-trailers
- Google Business Profile: https://www.google.com/maps/place//data=!4m3!3m2!1s0x89a94a91f7ce6007:0x258a33898d33e04a!12e1?source=g.page.default
- Logo URL: https://www.doubledtrailers.com/wp-content/uploads/2023/10/Blog-Logo.jpg

CRITICAL: When generating schema markup, copy URLs EXACTLY from this list. Do NOT truncate, abbreviate, or reconstruct any URL.
GLOSSARY;
    }

    // ─────────────────────────────────────────────
    //  CALL ALL FIVE LLMs IN PARALLEL
    // ─────────────────────────────────────────────

    private function callAllLLMs(string $prompt, int $maxTokens = 1500): array
    {
        $claudeKey     = $_ENV['ANTHROPIC_API_KEY']  ?? '';
        $openaiKey     = $_ENV['OPENAI_API_KEY']     ?? '';
        $geminiKey     = $_ENV['GEMINI_API_KEY']     ?? '';
        $grokKey       = $_ENV['XAI_API_KEY']        ?? '';
        $perplexityKey = $_ENV['PERPLEXITY_API_KEY'] ?? '';

        $handles = [];
        $mh      = curl_multi_init();

        if ($claudeKey) {
            $ch = curl_init('https://api.anthropic.com/v1/messages');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'claude-sonnet-4-6', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'x-api-key: ' . $claudeKey, 'anthropic-version: 2023-06-01'],
                CURLOPT_TIMEOUT        => 90,
            ]);
            $handles['claude'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($openaiKey) {
            $ch = curl_init('https://api.openai.com/v1/chat/completions');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'gpt-4o', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => 'You are an expert SEO strategist for a horse trailer manufacturer. Be specific, concise, and actionable.'], ['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'Authorization: Bearer ' . $openaiKey],
                CURLOPT_TIMEOUT        => 90,
            ]);
            $handles['gpt4o'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($geminiKey) {
            $ch = curl_init("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={$geminiKey}");
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['contents' => [['parts' => [['text' => $prompt]]]], 'generationConfig' => ['maxOutputTokens' => $maxTokens]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
                CURLOPT_TIMEOUT        => 90,
            ]);
            $handles['gemini'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($grokKey) {
            $ch = curl_init('https://api.x.ai/v1/chat/completions');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'grok-3-fast', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => 'You are an expert SEO strategist for a horse trailer manufacturer. Be specific, concise, and actionable.'], ['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'Authorization: Bearer ' . $grokKey],
                CURLOPT_TIMEOUT        => 90,
            ]);
            $handles['grok'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($perplexityKey) {
            $ch = curl_init('https://api.perplexity.ai/chat/completions');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'sonar-pro', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => 'You are an expert SEO strategist for a horse trailer manufacturer. Be specific, concise, and actionable.'], ['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'Authorization: Bearer ' . $perplexityKey],
                CURLOPT_TIMEOUT        => 90,
            ]);
            $handles['perplexity'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        $running = null;
        do { curl_multi_exec($mh, $running); curl_multi_select($mh); } while ($running > 0);

        $results = [];
        foreach ($handles as $llm => $ch) {
            $raw     = curl_multi_getcontent($ch);
            curl_multi_remove_handle($mh, $ch);
            curl_close($ch);
            $decoded = json_decode($raw, true);
            $results[$llm] = match($llm) {
                'claude'     => isset($decoded['content'][0]['text'])                           ? ['text' => $decoded['content'][0]['text']]                           : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'gpt4o'      => isset($decoded['choices'][0]['message']['content'])             ? ['text' => $decoded['choices'][0]['message']['content']]             : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'gemini'     => isset($decoded['candidates'][0]['content']['parts'][0]['text']) ? ['text' => $decoded['candidates'][0]['content']['parts'][0]['text']] : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'grok'       => isset($decoded['choices'][0]['message']['content'])             ? ['text' => $decoded['choices'][0]['message']['content']]             : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'perplexity' => isset($decoded['choices'][0]['message']['content'])             ? ['text' => $decoded['choices'][0]['message']['content']]             : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                default      => ['error' => 'Unknown LLM'],
            };
        }

        curl_multi_close($mh);
        return $results;
    }

    // ─────────────────────────────────────────────
    //  PARSE VERDICT
    // ─────────────────────────────────────────────

    private function parseVerdict(string $text): array
    {
        $verdict = 'UNKNOWN'; $confidence = 0; $summary = ''; $needsChange = 'no'; $suggested = 'none';

        if (preg_match('/VERDICT\s*:\s*(PASS|FLAG)/i',                        $text, $m)) $verdict    = strtoupper(trim($m[1]));
        if (preg_match('/SUMMARY\s*:\s*(.+)/i',                               $text, $m)) $summary    = trim($m[1]);
        if (preg_match('/NEEDS_ADJUSTMENT\s*:\s*(yes|no)\s*[—\-–]\s*(.+)/i', $text, $m)) {
            $needsChange = strtolower(trim($m[1]));
            $suggested   = trim($m[2]);
        }

        // Confidence — runs always regardless of verdict format
        if (preg_match('/CONFIDENCE\s*:\s*(\d+)/i',                           $text, $m)) $confidence = (int) $m[1];
        if ($confidence === 0 && preg_match('/(\d+)\s*\/\s*10/i',             $text, $m)) $confidence = (int) $m[1];
        if ($confidence === 0 && preg_match('/confidence[^.]{0,30}?(\d+)/i',  $text, $m)) $confidence = (int) $m[1];
        // Gemini-specific: "Confidence Score: 8" or "confidence_score: 8" or "Rating: 8"
        if ($confidence === 0 && preg_match('/confidence[_\s]score\s*:\s*(\d+)/i', $text, $m)) $confidence = (int) $m[1];
        if ($confidence === 0 && preg_match('/rating\s*:\s*(\d+)/i',               $text, $m)) $confidence = (int) $m[1];
        // Last resort: find any standalone digit 1-10 near the word "confidence" within 100 chars
        if ($confidence === 0 && preg_match('/confidence.{0,100}?\b([1-9]|10)\b/is', $text, $m)) $confidence = (int) $m[1];

        // Fallback verdict parsing for Gemini / prose responses
        if ($verdict === 'UNKNOWN') {
            $lower = strtolower($text);
            if (preg_match('/firing_correctly\s*:\s*(yes|no)/i', $text, $m)) {
                $fc    = strtolower(trim($m[1]));
                $hasFP = (bool) preg_match('/false_positives\s*:\s*yes/i', $text);
                $verdict = ($fc === 'yes' && !$hasFP) ? 'PASS' : 'FLAG';
            } else {
                $fS = ['false positive','not firing correctly','needs adjustment','should be revised','inaccurate','misclassified'];
                $pS = ['firing correctly','accurate','no false positives','rule is correct','working as intended','correctly identifies'];
                $fC = $pC = 0;
                foreach ($fS as $s) { if (str_contains($lower, $s)) $fC++; }
                foreach ($pS as $s) { if (str_contains($lower, $s)) $pC++; }
                if ($fC > $pC) $verdict = 'FLAG';
                elseif ($pC > 0) $verdict = 'PASS';
            }
        }

        // Summary fallback — skip structured KEY: value lines
        if (!$summary) {
            $lines = preg_split('/\r?\n/', strip_tags($text));
            foreach ($lines as $line) {
                $line = trim($line);
                if ($line === '') continue;
                if (preg_match('/^[A-Z][A-Z_]{2,}\s*:/i', $line)) continue;
                if (preg_match('/^[#\*\-]/', $line)) continue;
                if (strlen($line) < 25) continue;
                $summary = $line;
                break;
            }
            if (!$summary) $summary = substr(strip_tags($text), 0, 120);
        }

        return ['verdict' => $verdict, 'confidence' => $confidence, 'summary' => $summary, 'needs_change' => $needsChange, 'suggested' => $suggested, 'raw' => $text];
    }

    // ─────────────────────────────────────────────
    //  DETERMINE CONSENSUS (single round)
    // ─────────────────────────────────────────────

    private function determineConsensus(array $verdicts): array
    {
        $passes = $flags = $totalConf = $count = 0;
        foreach ($verdicts as $v) {
            if ($v['verdict'] === 'PASS') $passes++;
            if ($v['verdict'] === 'FLAG') $flags++;
            if ($v['confidence'] > 0) { $totalConf += $v['confidence']; $count++; }
        }
        $avgConf = $count > 0 ? round($totalConf / $count, 1) : 0;
        $status  = ($flags > 0 || $avgConf < 6) ? 'FLAGGED' : 'VALIDATED';
        $reason  = $flags > 0 ? "{$flags} of " . count($verdicts) . " LLMs flagged this rule." : ($avgConf < 6 ? "Low avg confidence ({$avgConf}/10)." : 'All LLMs agree rule is firing correctly.');
        return ['status' => $status, 'passes' => $passes, 'flags' => $flags, 'avg_conf' => $avgConf, 'reason' => $reason, 'majority' => false];
    }

    // ─────────────────────────────────────────────
    //  STORE EVALUATION (both stages)
    // ─────────────────────────────────────────────

    private function storeEvaluation(array $rule, array $firingPages, array $verdicts, array $consensus, array $allRounds, int $roundsRun, ?array $outputConsensus): void
    {
        try {
            $this->db->insert('rule_evaluations', [
                'rule_id'              => $rule['id'],
                'rule_name'            => $rule['name'],
                'pages_firing'         => count($firingPages),
                'sample_urls'          => json_encode(array_column(array_slice($firingPages, 0, 5), 'url')),
                'claude_verdict'       => $verdicts['claude']['verdict']       ?? 'N/A',
                'claude_conf'          => $verdicts['claude']['confidence']    ?? 0,
                'claude_summary'       => $verdicts['claude']['summary']       ?? '',
                'gpt4o_verdict'        => $verdicts['gpt4o']['verdict']        ?? 'N/A',
                'gpt4o_conf'           => $verdicts['gpt4o']['confidence']     ?? 0,
                'gpt4o_summary'        => $verdicts['gpt4o']['summary']        ?? '',
                'gemini_verdict'       => $verdicts['gemini']['verdict']       ?? 'N/A',
                'gemini_conf'          => $verdicts['gemini']['confidence']    ?? 0,
                'gemini_summary'       => $verdicts['gemini']['summary']       ?? '',
                'grok_verdict'         => $verdicts['grok']['verdict']         ?? 'N/A',
                'grok_conf'            => $verdicts['grok']['confidence']      ?? 0,
                'grok_summary'         => $verdicts['grok']['summary']         ?? '',
                'perplexity_verdict'   => $verdicts['perplexity']['verdict']   ?? 'N/A',
                'perplexity_conf'      => $verdicts['perplexity']['confidence'] ?? 0,
                'perplexity_summary'   => $verdicts['perplexity']['summary']   ?? '',
                'consensus_status'     => $consensus['status'],
                'avg_confidence'       => $consensus['avg_conf'],
                'consensus_reason'     => $consensus['reason'],
                'rounds_run'           => $roundsRun,
                'round_history'        => json_encode($allRounds),
                // Stage 2 output — unified play brief
                'play_brief'           => $outputConsensus['raw'] ?? null,
                'output_finding'       => $outputConsensus['finding']    ?? null,
                'output_diagnosis'     => $outputConsensus['diagnosis']  ?? null,
                'output_pages'         => $outputConsensus['pages']      ? json_encode($outputConsensus['pages']) : null,
                'output_priority'      => $outputConsensus['priority']   ?? null,
                'output_verify_in'     => $outputConsensus['verify_in']  ?? null,
                'output_brook'         => $outputConsensus['role_brook'] ?? null,
                'output_brad'          => $outputConsensus['role_brad']  ?? null,
                'output_kalib'         => $outputConsensus['role_kalib'] ?? null,
                'output_jeanne'        => $outputConsensus['role_jeanne'] ?? null,
                'output_caveat'        => $outputConsensus['caveat']     ?? null,
                'output_conf'          => $outputConsensus['avg_conf']   ?? null,
                'evaluated_at'         => date('Y-m-d H:i:s'),
            ]);
        } catch (\Exception $e) {
            // Non-fatal
        }
    }

    // ─────────────────────────────────────────────
    //  ENSURE DB SCHEMA
    // ─────────────────────────────────────────────

    private function ensureSchema(): void
    {
        try {
            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS rule_evaluations (
                    id                  SERIAL PRIMARY KEY,
                    rule_id             VARCHAR(20) NOT NULL,
                    rule_name           TEXT,
                    pages_firing        INT DEFAULT 0,
                    sample_urls         TEXT,
                    claude_verdict      VARCHAR(10),
                    claude_conf         INT DEFAULT 0,
                    claude_summary      TEXT,
                    gpt4o_verdict       VARCHAR(10),
                    gpt4o_conf          INT DEFAULT 0,
                    gpt4o_summary       TEXT,
                    gemini_verdict      VARCHAR(10),
                    gemini_conf         INT DEFAULT 0,
                    gemini_summary      TEXT,
                    grok_verdict        VARCHAR(10),
                    grok_conf           INT DEFAULT 0,
                    grok_summary        TEXT,
                    perplexity_verdict  VARCHAR(10),
                    perplexity_conf     INT DEFAULT 0,
                    perplexity_summary  TEXT,
                    consensus_status    VARCHAR(30),
                    avg_confidence      NUMERIC(4,1),
                    consensus_reason    TEXT,
                    rounds_run          INT DEFAULT 1,
                    round_history       TEXT,
                    play_brief          TEXT,
                    output_finding      TEXT,
                    output_diagnosis    TEXT,
                    output_pages        TEXT,
                    output_priority     VARCHAR(20),
                    output_verify_in    VARCHAR(20),
                    output_brook        TEXT,
                    output_brad         TEXT,
                    output_kalib        TEXT,
                    output_jeanne       TEXT,
                    output_caveat       TEXT,
                    output_conf         NUMERIC(4,1),
                    evaluated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
            // Add columns to existing tables if missing
            $newCols = [
                'rounds_run INT DEFAULT 1', 'round_history TEXT', 'play_brief TEXT',
                'output_finding TEXT', 'output_diagnosis TEXT', 'output_pages TEXT',
                'output_priority VARCHAR(20)', 'output_verify_in VARCHAR(20)',
                'output_brook TEXT', 'output_brad TEXT', 'output_kalib TEXT',
                'output_jeanne TEXT', 'output_caveat TEXT', 'output_conf NUMERIC(4,1)',
                'grok_verdict VARCHAR(10)', 'grok_conf INT DEFAULT 0', 'grok_summary TEXT',
                'perplexity_verdict VARCHAR(10)', 'perplexity_conf INT DEFAULT 0', 'perplexity_summary TEXT',
            ];
            foreach ($newCols as $col) {
                $this->db->executeStatement("ALTER TABLE rule_evaluations ADD COLUMN IF NOT EXISTS {$col}");
            }
        } catch (\Exception $e) {
            // May already exist
        }
    }
}
