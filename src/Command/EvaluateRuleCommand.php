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

            if (!$skipOutput) {
                $output->writeln('');
                $output->writeln("   -- Stage 2: Output Consensus --");

                $outputPrompt  = $this->buildOutputPrompt($rule, $firingPages, $finalConsensus, $finalVerdicts);
                $stage2Result  = $this->runDeliberation($outputPrompt, $output, $verboseLlm, 'S2', 8000, 1);
                $outputConsensus = $this->synthesiseOutput($stage2Result['verdicts'], $stage2Result['consensus'], $rule);
                $outputConsensus['rounds_run'] = $stage2Result['rounds_run'];

                $this->displayOutputConsensus($output, $outputConsensus, $stage2Result['consensus']);

                // ── CREATE TASKS FROM PLAY BRIEFS ──
                $briefs = $outputConsensus['briefs'] ?? [];
                foreach ($briefs as $brief) {
                    $title = $brief['title'] ?? '';
                    $url   = $brief['url'] ?? '';
                    if (!$title || !$url) continue;

                    // Skip meta-commentary briefs (LLM explaining its process instead of an actual task)
                    if (str_contains(strtolower($title), 'maintaining my response') || str_contains(strtolower($title), 'peer summaries')) continue;

                    // Check for duplicate — don't create if same rule+url task already exists and is not done
                    $existing = $this->db->fetchAssociative(
                        "SELECT id FROM tasks WHERE rule_id = :rule AND title LIKE :url AND status != 'done'",
                        ['rule' => $rule['id'], 'url' => '%' . $url . '%']
                    );
                    if ($existing) continue;

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
            foreach ($page as $key => $val) {
                if ($key === 'url' || in_array($key, ['internal_links', 'crawled_at'])) continue;
                $boolFields = ['has_central_entity', 'has_core_link', 'h1_matches_title', 'is_noindex', 'is_utility'];
                if (is_null($val)) { $display = 'NULL'; }
                elseif (in_array($key, $boolFields)) { $display = ($val && $val !== 'f' && $val !== '0') ? 'TRUE' : 'FALSE'; }
                elseif (is_bool($val)) { $display = $val ? 'TRUE' : 'FALSE'; }
                else { $display = (string) $val; }
                $pageDetails .= "\n  {$key}: {$display}";
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

        return <<<PROMPT
You are Logiri, an SEO intelligence engine for doubledtrailers.com (Double D Trailers — custom horse trailer manufacturer).

Your job: produce ONE PLAY BRIEF per affected page. A play brief is a task ticket — specific, actionable, copy-paste ready.

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
{$feedbackSection}{$outcomeSection}
REAL CORE PAGES ON THIS SITE (use ONLY these URLs when suggesting Core link targets):
{$coreList}

DATA FOR AFFECTED PAGES ({$total} total):
{$pageDetails}

INSTRUCTIONS:
Write one PLAY_BRIEF block per page. Each brief must include:
1. CURRENT STATE — the exact data fields from the crawl that triggered this rule. Use the actual values above. Format as bullet points.
2. YOUR MOVE — numbered steps the person should take. Be surgical. If the fix involves code (schema, meta tags, HTML), include the actual code snippet. If it involves copy changes, write the actual new copy or give a specific before/after example. Reference the exact URL, exact field values, exact text.
3. DONE WHEN — the specific crawl field check that confirms the fix worked, plus any manual verification step (e.g. "Run Google Rich Results Test — 0 errors, Product detected").
4. RECHECK — number of days until recheck.

CRITICAL RULES FOR OUTPUT:
- Do NOT write a report or analysis. Write task tickets.
- Do NOT split by team role. One unified brief per page.
- Include actual code snippets where relevant (JSON-LD, meta tags, HTML).
- Include actual copy rewrites where relevant (before/after).
- Reference the EXACT data values from the crawl data above.
- When suggesting Core page link targets, use ONLY URLs from the REAL CORE PAGES list above. Do not invent URLs.
- Keep each brief under 300 words.
- If a page is a false positive or edge case, say so in a CAVEAT line and suggest skipping or reclassifying instead of fixing.
- Product pages: body text must NOT exceed 500 words. MSE elements (images, attributes, CTAs, reviews, FAQs) carry the page.
- Outer pages: minimum 1000 words. Below that = thin content, recommend merge into relevant HSV page.
- Max 3 internal links per page. Zero external links (replace with citation mentions).
- First sentence under any heading must directly answer the heading's implied question.

FORMAT (repeat for each page):

PLAY_BRIEF: [Short title — verb + what + where]
URL: [exact url path]
PRIORITY: [Critical / High / Medium / Low]
ASSIGNED: [Brook / Brad / Kalib / Jeanne]
CURRENT_STATE:
- [field]: [value]
- [field]: [value]
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

    private function synthesiseOutput(array $verdicts, array $consensus, array $rule): array
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
        $promptPath = dirname(__DIR__, 2) . '/system-prompt.txt';
        if (!file_exists($promptPath)) return [];

        $content = file_get_contents($promptPath);
        $rules   = [];

        // Match any rule ID pattern: OPQ-001, TECH-R1, DDT-SD-002, DDT-EEAT-03, DDT-LOCAL-01, CTA-F4, OPQ-R4b, etc.
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
                // Extract SQL if present (may be on multiple lines)
                $triggerSql = $triggerCondition;
                // Clean SQL markers
                $triggerSql = preg_replace('/```sql\s*/', '', $triggerSql);
                $triggerSql = preg_replace('/```\s*/', '', $triggerSql);
                $triggerSql = trim($triggerSql);
            }
            if (preg_match('/Threshold:\s*(.*?)(?=\nCrawl Parameter:|$)/s', $ruleText, $m)) $threshold = trim($m[1]);
            if (preg_match('/Diagnosis:\s*(.*?)(?=\nAction Output:|$)/s',   $ruleText, $m)) $diagnosis = trim($m[1]);
            if (preg_match('/Priority:\s*([^\n]+)/',                         $ruleText, $m)) $priority  = trim($m[1]);
            if (preg_match('/Assigned:\s*([^\n]+)/',                         $ruleText, $m)) $assigned  = trim($m[1]);

            $rules[] = [
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

            // Try to extract executable SQL from the rule's trigger condition
            $sql = $rule['trigger_sql'] ?? '';

            // If the trigger_sql looks like a valid SELECT, execute it directly
            if ($sql && preg_match('/^\s*SELECT\s/i', $sql)) {
                // Ensure LIMIT is present
                if (!preg_match('/LIMIT\s+\d+/i', $sql)) {
                    $sql .= ' LIMIT 15';
                }
                try {
                    $results = $this->db->fetchAllAssociative($sql);
                    if (!empty($results)) return $results;
                } catch (\Exception $e) {
                    // SQL failed (missing columns/tables) — fall through to simplified query
                }
            }

            // ── SIMPLIFIED FALLBACK QUERIES ──
            // When the rule's SQL references columns that don't exist yet,
            // use the rule's core intent to build a working query from available fields.
            $ruleId = $rule['id'];
            $tc = $rule['trigger_condition'] ?? '';

            // Detect the rule's intent from its trigger condition text and build a simplified query
            $simplifiedQuery = $this->getSimplifiedQuery($ruleId, $tc);
            if ($simplifiedQuery) {
                try {
                    return $this->db->fetchAllAssociative($simplifiedQuery);
                } catch (\Exception $e) {
                    // Still failed — continue to legacy fallback
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
                'FC-R5'  => "SELECT url, page_type, has_core_link, core_links_found FROM page_crawl_snapshots WHERE page_type = 'outer' AND has_core_link IS NOT TRUE AND is_noindex IS NOT TRUE AND {$af} AND url NOT IN ({$t4}) {$utilExclude} LIMIT 10",
                'FC-R6'  => "SELECT url, page_type, word_count, h2s, schema_types FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND word_count < 800 AND is_noindex IS NOT TRUE LIMIT 10",
                'FC-R7'  => "SELECT url, page_type, h1, title_tag, h1_matches_title FROM page_crawl_snapshots WHERE (h1_matches_title IS NOT TRUE OR h1 IS NULL OR h1 = '') AND is_noindex IS NOT TRUE AND {$af} AND url NOT IN ({$t4}) {$utilExclude} LIMIT 10",
                'FC-R8'  => "SELECT url, page_type, h2s, word_count FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND (h2s IS NULL OR h2s = '[]' OR h2s = '') AND is_noindex IS NOT TRUE AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R9'  => "SELECT url, page_type, schema_types, h1 FROM page_crawl_snapshots WHERE page_type = 'core' AND (schema_types IS NULL OR schema_types = '[]' OR schema_types = '') AND is_noindex IS NOT TRUE AND url NOT LIKE '%//' AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R10' => "SELECT p.url, p.page_type, p.has_core_link, MAX(g.impressions) as impressions FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.has_core_link IS NOT TRUE AND g.impressions >= 100 AND g.date_range = '28d' GROUP BY p.url, p.page_type, p.has_core_link ORDER BY impressions DESC LIMIT 10",
                default  => null,
            };

            if ($query) {
                return $this->db->fetchAllAssociative($query);
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
                    return $this->db->fetchAllAssociative(
                        "SELECT p.url, p.page_type, p.word_count, p.h1, p.title_tag, p.has_central_entity,
                                p.central_entity_count, p.schema_types, p.h1_matches_title, p.h2s,
                                p.has_core_link, p.canonical_url, p.is_noindex,
                                g.impressions, g.clicks, g.position, g.ctr
                         FROM page_crawl_snapshots p
                         LEFT JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url)
                         WHERE {$where}
                         LIMIT 15"
                    );
                }

                // Default: page_crawl_snapshots only
                return $this->db->fetchAllAssociative(
                    "SELECT url, page_type, word_count, h1, title_tag, has_central_entity,
                            central_entity_count, schema_types, h1_matches_title, h2s,
                            has_core_link, canonical_url, is_noindex
                     FROM page_crawl_snapshots
                     WHERE {$where}
                     LIMIT 15"
                );
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
                "SELECT url, title_tag FROM page_crawl_snapshots WHERE page_type = 'core' ORDER BY url"
            );
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  SIMPLIFIED FALLBACK QUERIES
    //  When a rule's SQL references columns that don't exist,
    //  this provides a working query using only available fields.
    // ─────────────────────────────────────────────

    private function getSimplifiedQuery(string $ruleId, string $triggerCondition): ?string
    {
        $cols = "url, page_type, word_count, h1, title_tag, has_central_entity, central_entity_count, schema_types, h1_matches_title, h2s, has_core_link, canonical_url, is_noindex, internal_links";

        return match($ruleId) {
            // Entity & Topical Authority
            'ETA-01' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND has_central_entity = FALSE AND is_noindex = FALSE LIMIT 15",
            'ETA-02' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type IN ('core') AND has_central_entity = FALSE AND word_count > 0 AND is_noindex = FALSE LIMIT 15",
            'ETA-03' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND is_noindex = FALSE LIMIT 15",
            'ETA-04' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type IN ('core') AND schema_types NOT LIKE '%Product%' AND schema_types NOT LIKE '%Organization%' AND is_noindex = FALSE LIMIT 15",
            'ETA-05' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND word_count < 1000 AND word_count > 0 AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",
            'ETA-06' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 500 AND is_noindex = FALSE LIMIT 15",

            // E-E-A-T & Trust Signals
            'DDT-EEAT-03' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND schema_types NOT LIKE '%Review%' AND schema_types NOT LIKE '%AggregateRating%' AND is_noindex = FALSE LIMIT 15",
            'DDT-EEAT-04' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url LIKE '%/about%' AND (word_count < 1000 OR schema_types NOT LIKE '%Organization%') LIMIT 15",
            'DDT-EEAT-05' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%privacy%' OR url LIKE '%terms%') AND (word_count < 1000 OR is_noindex = TRUE) LIMIT 15",
            'DDT-EEAT-06' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url LIKE '%contact%' AND (schema_types NOT LIKE '%LocalBusiness%' OR schema_types NOT LIKE '%ContactPoint%') LIMIT 15",
            'DDT-EEAT-07' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND word_count >= 1000 AND is_noindex = FALSE LIMIT 15",
            'DDT-EEAT-08' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND is_noindex = FALSE LIMIT 15",

            // Schema & Structured Data
            'DDT-SD-002' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type IN ('core') AND schema_types NOT LIKE '%Organization%' AND is_noindex = FALSE LIMIT 15",
            'DDT-SD-003' => "SELECT p.url, p.page_type, p.word_count, p.schema_types, p.h1, g.impressions FROM page_crawl_snapshots p LEFT JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.word_count >= 1000 AND p.schema_types NOT LIKE '%FAQPage%' AND p.is_noindex = FALSE AND g.impressions > 800 ORDER BY g.impressions DESC LIMIT 15",
            'DDT-SD-004' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count <= 500 AND schema_types NOT LIKE '%AggregateRating%' AND is_noindex = FALSE LIMIT 15",
            'DDT-SD-005' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url != '/' AND page_type NOT IN ('utility') AND schema_types NOT LIKE '%BreadcrumbList%' AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",
            'DDT-SD-006' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type IN ('core') AND url IN ('/', '/horse-trailers/', '/gooseneck-horse-trailers/', '/bumper-pull-horse-trailers/', '/living-quarters-horse-trailers/') AND schema_types NOT LIKE '%ProductGroup%' AND word_count >= 0 LIMIT 15",

            // Local & Dealer SEO
            'DDT-LOCAL-01' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%dealer%' OR url LIKE '%location%') AND schema_types NOT LIKE '%LocalBusiness%' AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",
            'DDT-LOCAL-02' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%dealer%' OR url LIKE '%location%') AND (word_count < 100 OR h1 IS NULL OR h1 = '') AND is_noindex = FALSE LIMIT 15",
            'DDT-LOCAL-03' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%dealer%' OR url LIKE '%location%') AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",
            'DDT-LOCAL-04' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND (title_tag LIKE '%dealer%' OR title_tag LIKE '%trailer%in%' OR h1 LIKE '%dealer%') AND word_count < 1000 AND is_noindex = FALSE LIMIT 15",
            'DDT-LOCAL-05' => "SELECT {$cols} FROM page_crawl_snapshots WHERE (url LIKE '%dealer%' OR url LIKE '%location%') AND is_noindex = FALSE AND is_utility = FALSE LIMIT 15",

            // User Signals & Engagement (GSC joins)
            'USE-R1' => "SELECT p.url, p.page_type, p.word_count, p.h1, p.h1_matches_title, p.has_central_entity, g.impressions, g.clicks, g.ctr, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'core' AND g.impressions >= 500 AND g.position <= 15 AND g.ctr < 0.08 ORDER BY g.impressions DESC LIMIT 15",
            'USE-R2' => "SELECT p.url, p.page_type, p.word_count, p.h1, g.impressions, g.clicks, g.ctr, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND g.impressions >= 1000 AND g.ctr < 0.01 ORDER BY g.impressions DESC LIMIT 15",
            'USE-R3' => "SELECT p.url, p.page_type, p.word_count, p.has_central_entity, p.h1_matches_title, g.position, g.clicks, g.impressions FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'core' AND g.position <= 10 AND g.clicks >= 5 AND p.word_count < 150 ORDER BY g.clicks DESC LIMIT 15",
            'USE-R4' => "SELECT p.url, p.page_type, p.word_count, p.h1, p.h2s, g.impressions, g.clicks, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.word_count < 1000 AND g.impressions >= 500 AND g.position <= 30 ORDER BY g.impressions DESC LIMIT 15",
            'USE-R5' => "SELECT p.url, p.page_type, p.word_count, p.h2s, p.h1_matches_title, g.impressions FROM page_crawl_snapshots p LEFT JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type IN ('core', 'outer') AND (p.h2s IS NULL OR p.h2s = '[]' OR p.h2s = '') AND p.word_count > 300 AND p.is_noindex = FALSE ORDER BY g.impressions DESC NULLS LAST LIMIT 15",
            'USE-R6' => "SELECT p.url, p.page_type, p.h1, p.title_tag, p.meta_description, g.impressions, g.clicks, g.ctr, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND g.impressions >= 300 AND g.position <= 20 AND g.ctr < 0.025 ORDER BY g.impressions DESC LIMIT 15",
            'USE-R7' => "SELECT p.url, p.page_type, p.has_central_entity, p.word_count, p.schema_types, p.h1_matches_title, g.impressions, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.has_central_entity = FALSE AND g.impressions >= 200 AND g.position <= 20 AND p.page_type IN ('core', 'outer') AND p.is_noindex = FALSE ORDER BY g.impressions DESC LIMIT 15",

            // Keyword & Intent Alignment (GSC)
            'KIA-R2' => "SELECT query, COUNT(DISTINCT page) AS page_count, SUM(impressions) as total_imp, AVG(position) as avg_pos FROM gsc_snapshots WHERE position <= 20 GROUP BY query HAVING COUNT(DISTINCT page) > 1 AND SUM(impressions) > 100 ORDER BY total_imp DESC LIMIT 15",
            'KIA-R3' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND (word_count = 0 OR has_central_entity = FALSE OR h1_matches_title = FALSE OR word_count > 500) AND is_noindex = FALSE LIMIT 15",
            'KIA-R4' => "SELECT g.query, SUM(g.impressions) as total_imp, AVG(g.position) as avg_pos FROM gsc_snapshots g WHERE g.impressions > 5000 AND g.position > 10 AND (g.query LIKE '%horse trailer%' OR g.query LIKE '%gooseneck%' OR g.query LIKE '%z-frame%' OR g.query LIKE '%safetack%') GROUP BY g.query ORDER BY total_imp DESC LIMIT 15",
            'KIA-R5' => "SELECT g.query, SUM(g.impressions) as total_imp, AVG(g.position) as avg_pos FROM gsc_snapshots g WHERE (g.query LIKE '%2 horse%' OR g.query LIKE '%3 horse%' OR g.query LIKE '%gooseneck%' OR g.query LIKE '%safetack%') AND g.impressions > 100 AND g.position > 30 GROUP BY g.query ORDER BY total_imp DESC LIMIT 15",
            'KIA-R6' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND word_count < 1000 AND word_count > 0 AND is_noindex = FALSE LIMIT 15",
            'KIA-R7' => "SELECT g.query, SUM(g.impressions) as total_imp, AVG(g.position) as avg_pos FROM gsc_snapshots g WHERE (g.query LIKE '%benefits%' OR g.query LIKE '%vs%' OR g.query LIKE '%safetack%') AND g.impressions > 500 AND g.position > 20 GROUP BY g.query ORDER BY total_imp DESC LIMIT 15",
            'KIA-R8' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND is_noindex = FALSE LIMIT 15",

            // Competitive Intelligence (mostly GSC-based)
            'CI-R1' => "SELECT g.query, g.page, g.position, g.impressions, g.clicks, g.ctr FROM gsc_snapshots g JOIN page_crawl_snapshots p ON g.page LIKE CONCAT('%', p.url) WHERE g.position > 10 AND g.impressions > 500 AND p.page_type = 'core' ORDER BY g.impressions DESC LIMIT 15",
            'CI-R4' => "SELECT g.query, g.page, g.position, g.impressions, g.clicks FROM gsc_snapshots g WHERE g.impressions > 1000 ORDER BY g.impressions DESC LIMIT 15",
            'CI-R6' => "SELECT p.url, p.title_tag, p.meta_description, p.page_type, g.impressions, g.clicks, g.ctr, g.position FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE g.impressions > 5000 AND g.position < 10 AND g.ctr < 0.05 AND p.page_type IN ('core', 'outer') ORDER BY g.impressions DESC LIMIT 15",

            // Content Freshness
            'CFL-04' => "SELECT p.url, p.word_count, p.page_type, g.impressions, g.clicks, g.position, g.ctr FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.word_count >= 1000 AND p.is_noindex = FALSE AND g.impressions > 1000 AND g.position > 15 AND g.ctr < 0.02 ORDER BY g.impressions DESC LIMIT 15",

            // Media & Asset Optimization (simplified — can't check actual image alt text)
            'MAO-R1' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count > 0 AND is_noindex = FALSE LIMIT 15",
            'MAO-R2' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'outer' AND word_count >= 1000 AND is_noindex = FALSE LIMIT 15",
            'MAO-R4' => "SELECT {$cols} FROM page_crawl_snapshots WHERE schema_types NOT LIKE '%VideoObject%' AND is_noindex = FALSE AND page_type IN ('core', 'outer') LIMIT 15",
            'MAO-R6' => "SELECT {$cols} FROM page_crawl_snapshots WHERE url LIKE '%.pdf' LIMIT 15",
            'MAO-R7' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type IN ('core', 'outer') AND is_noindex = FALSE LIMIT 15",

            // Internal Link Architecture
            'ILA-004' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND is_noindex = FALSE LIMIT 15",
            'ILA-005' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type IN ('core', 'outer') AND is_noindex = FALSE LIMIT 15",
            'ILA-006' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND is_noindex = FALSE LIMIT 15",
            'ILA-007' => "SELECT {$cols} FROM page_crawl_snapshots WHERE page_type = 'core' AND is_noindex = FALSE LIMIT 15",

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
    
