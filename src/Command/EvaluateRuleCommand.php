<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:evaluate-rule', description: 'Send a rule + its crawl findings to Claude, GPT-4, and Gemini for consensus validation')]
class EvaluateRuleCommand extends Command
{
    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('rule', null, InputOption::VALUE_OPTIONAL, 'Specific rule ID to evaluate (e.g. FC-R7). Omit to evaluate all firing rules.')
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Show what would be sent to LLMs without calling APIs')
            ->addOption('verbose-llm', null, InputOption::VALUE_NONE, 'Show full LLM responses');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $ruleFilter = $input->getOption('rule');
        $dryRun     = (bool) $input->getOption('dry-run');
        $verboseLlm = (bool) $input->getOption('verbose-llm');

        $this->ensureSchema();

        // Load rules from system-prompt.txt
        $rules = $this->loadRules();
        if (empty($rules)) {
            $output->writeln('<error>Could not load rules from system-prompt.txt</error>');
            return Command::FAILURE;
        }

        // Filter to specific rule if requested
        if ($ruleFilter) {
            $rules = array_filter($rules, fn($r) => $r['id'] === strtoupper($ruleFilter));
            if (empty($rules)) {
                $output->writeln("<error>Rule {$ruleFilter} not found in system-prompt.txt</error>");
                return Command::FAILURE;
            }
        }

        $output->writeln('');
        $output->writeln('╔══════════════════════════════════════════╗');
        $output->writeln('║     LOGIRI MULTI-LLM RULE EVALUATOR      ║');
        $output->writeln('╚══════════════════════════════════════════╝');
        $output->writeln('');

        $totalEvaluated = 0;
        $totalFlagged   = 0;

        foreach ($rules as $rule) {
            // Find pages where this rule is currently firing
            $firingPages = $this->getFiringPages($rule);

            if (empty($firingPages)) {
                $output->writeln("⬜ {$rule['id']} — no pages currently firing, skipping.");
                continue;
            }

            $output->writeln("▶ Evaluating {$rule['id']}: {$rule['name']}");
            $output->writeln("  Pages firing: " . count($firingPages));

            // Build the evaluation prompt
            $prompt = $this->buildPrompt($rule, $firingPages);

            if ($dryRun) {
                $output->writeln("  [DRY RUN] Prompt preview:");
                $output->writeln("  " . substr($prompt, 0, 300) . "...");
                $output->writeln('');
                continue;
            }

            // Call all three LLMs in parallel using curl_multi
            $output->writeln("  Sending to Claude, GPT-4o, Gemini...");
            $responses = $this->callAllLLMs($prompt);

            // Parse and score each response
            $verdicts = [];
            foreach ($responses as $llm => $response) {
                if (isset($response['error'])) {
                    $output->writeln("  ⚠ {$llm}: API error — {$response['error']}");
                    continue;
                }
                $parsed = $this->parseVerdict($response['text']);
                $verdicts[$llm] = $parsed;

                if ($verboseLlm) {
                    $output->writeln("  [{$llm}] Raw response: " . substr($response['text'], 0, 200));
                }
            }

            if (empty($verdicts)) {
                $output->writeln("  ✗ All LLM calls failed — skipping\n");
                continue;
            }

            // Determine consensus
            $consensus = $this->determineConsensus($verdicts);

            // Display results
            $this->displayResults($output, $rule, $firingPages, $verdicts, $consensus);

            // Store in DB
            $this->storeEvaluation($rule, $firingPages, $verdicts, $consensus);

            $totalEvaluated++;
            if ($consensus['status'] === 'FLAGGED') {
                $totalFlagged++;
            }

            $output->writeln('');
        }

        // Summary
        $output->writeln('══════════════════════════════════════════');
        $output->writeln("SUMMARY: {$totalEvaluated} rules evaluated | {$totalFlagged} flagged for review");
        $output->writeln('');

        if ($totalFlagged > 0) {
            $output->writeln("⚠  Run: php bin/console app:evaluate-rule --rule=FC-RX to dig into specific rules");
            $output->writeln("   View all evaluations: SELECT * FROM rule_evaluations ORDER BY evaluated_at DESC;");
        } else {
            $output->writeln("✅ All evaluated rules passed LLM consensus.");
        }

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  LOAD RULES FROM system-prompt.txt
    // ─────────────────────────────────────────────

    private function loadRules(): array
    {
        $promptPath = dirname(__DIR__, 2) . '/system-prompt.txt';
        if (!file_exists($promptPath)) {
            return [];
        }

        $content = file_get_contents($promptPath);
        $rules   = [];

        // Parse each rule block — format: FC-R1 | Rule Name
        preg_match_all('/\n(FC-R\d+)\s*\|\s*([^\n]+)\n(.*?)(?=\nFC-R\d+|\nRESULTS VERIFICATION|\z)/s', $content, $matches, PREG_SET_ORDER);

        foreach ($matches as $match) {
            $ruleText = trim($match[3]);

            // Extract key fields
            $triggerCondition = '';
            $crawlParams      = '';
            $diagnosis        = '';

            if (preg_match('/Trigger Condition:\s*([^\n]+)/', $ruleText, $m)) {
                $triggerCondition = trim($m[1]);
            }
            if (preg_match('/Crawl Parameter:\s*([^\n]+)/', $ruleText, $m)) {
                $crawlParams = trim($m[1]);
            }
            if (preg_match('/Diagnosis:\s*([^\n]+)/', $ruleText, $m)) {
                $diagnosis = trim($m[1]);
            }

            $rules[] = [
                'id'                => trim($match[1]),
                'name'              => trim($match[2]),
                'full_text'         => $ruleText,
                'trigger_condition' => $triggerCondition,
                'crawl_params'      => $crawlParams,
                'diagnosis'         => $diagnosis,
            ];
        }

        return $rules;
    }

    // ─────────────────────────────────────────────
    //  GET PAGES WHERE RULE IS CURRENTLY FIRING
    // ─────────────────────────────────────────────

    private function getFiringPages(array $rule): array
    {
        try {
            $ruleId = $rule['id'];

            $query = match($ruleId) {
                'FC-R1'  => "SELECT url, page_type, h1, title_tag, word_count, has_central_entity, central_entity_count FROM page_crawl_snapshots WHERE has_central_entity = FALSE AND is_noindex = FALSE AND is_utility = FALSE LIMIT 10",
                'FC-R2'  => "SELECT url, page_type, h1, title_tag FROM page_crawl_snapshots WHERE (page_type IS NULL OR page_type NOT IN ('core','outer')) AND is_noindex = FALSE LIMIT 10",
                'FC-R3'  => "SELECT url, page_type, word_count, h1, title_tag FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count < 500 AND is_noindex = FALSE LIMIT 10",
                'FC-R5'  => "SELECT url, page_type, has_core_link, core_links_found FROM page_crawl_snapshots WHERE page_type = 'outer' AND has_core_link = FALSE AND is_noindex = FALSE AND is_utility = FALSE LIMIT 10",
                'FC-R6'  => "SELECT url, page_type, word_count, h2s, schema_types FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count < 800 AND is_noindex = FALSE LIMIT 10",
                'FC-R7'  => "SELECT url, page_type, h1, title_tag, h1_matches_title FROM page_crawl_snapshots WHERE h1_matches_title = FALSE AND is_noindex = FALSE AND is_utility = FALSE LIMIT 10",
                'FC-R8'  => "SELECT url, page_type, h2s, word_count FROM page_crawl_snapshots WHERE page_type = 'core' AND (h2s = '[]' OR h2s IS NULL) AND is_noindex = FALSE LIMIT 10",
                'FC-R9'  => "SELECT url, page_type, schema_types, h1 FROM page_crawl_snapshots WHERE page_type = 'core' AND schema_types = '[]' AND is_noindex = FALSE LIMIT 10",
                'FC-R10' => "SELECT p.url, p.page_type, p.has_core_link, g.impressions FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.has_core_link = FALSE AND g.impressions >= 100 AND g.date_range = '28d' ORDER BY g.impressions DESC LIMIT 10",
                default  => null,
            };

            if (!$query) return [];

            return $this->db->fetchAllAssociative($query);
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  BUILD EVALUATION PROMPT
    // ─────────────────────────────────────────────

    private function buildPrompt(array $rule, array $firingPages): string
    {
        $pageList = '';
        foreach (array_slice($firingPages, 0, 5) as $page) {
            $pageList .= "\n- URL: " . ($page['url'] ?? 'n/a');
            foreach ($page as $key => $val) {
                if ($key === 'url') continue;
                if (in_array($key, ['internal_links', 'crawled_at'])) continue;
                $pageList .= " | {$key}: " . (is_null($val) ? 'NULL' : $val);
            }
        }

        $totalFiring = count($firingPages);

        return <<<PROMPT
You are an expert SEO architect evaluating whether an SEO rule is firing correctly.

SITE CONTEXT:
- Domain: doubledtrailers.com
- Business: Custom horse trailer manufacturer (Double D Trailers)
- Central entity: horse trailer

RULE BEING EVALUATED:
ID: {$rule['id']}
Name: {$rule['name']}
Trigger condition: {$rule['trigger_condition']}
Diagnosis: {$rule['diagnosis']}

Full rule text:
{$rule['full_text']}

CURRENT FIRING DATA ({$totalFiring} pages triggering this rule):
{$pageList}

YOUR EVALUATION TASK:
1. Is this rule firing correctly given the data above? (yes/no)
2. Are there false positives — pages being flagged that shouldn't be? (yes/no, explain)
3. Are there false negatives — pages NOT being flagged that should be? (yes/no, explain)
4. Is the diagnosis accurate for the pages shown? (yes/no)
5. Does the rule need adjustment? If yes, what specific change?
6. Confidence score: how confident are you in this rule's accuracy? (1-10)
7. Overall verdict: PASS (rule is working correctly) or FLAG (rule needs review)

Respond in this exact format:
FIRING_CORRECTLY: yes/no
FALSE_POSITIVES: yes/no — [explanation]
FALSE_NEGATIVES: yes/no — [explanation]
DIAGNOSIS_ACCURATE: yes/no
NEEDS_ADJUSTMENT: yes/no — [specific suggested change or "none"]
CONFIDENCE: [1-10]
VERDICT: PASS/FLAG
SUMMARY: [one sentence]
PROMPT;
    }

    // ─────────────────────────────────────────────
    //  CALL ALL THREE LLMs IN PARALLEL
    // ─────────────────────────────────────────────

    private function callAllLLMs(string $prompt): array
    {
        $claudeKey  = $_ENV['ANTHROPIC_API_KEY']  ?? '';
        $openaiKey  = $_ENV['OPENAI_API_KEY']      ?? '';
        $geminiKey  = $_ENV['GEMINI_API_KEY']       ?? '';

        $handles = [];
        $results = [];
        $mh      = curl_multi_init();

        // ── Claude ──
        if ($claudeKey) {
            $payload = json_encode([
                'model'      => 'claude-sonnet-4-6',
                'max_tokens' => 1024,
                'messages'   => [['role' => 'user', 'content' => $prompt]],
            ]);
            $ch = curl_init('https://api.anthropic.com/v1/messages');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => $payload,
                CURLOPT_HTTPHEADER     => [
                    'Content-Type: application/json',
                    'x-api-key: ' . $claudeKey,
                    'anthropic-version: 2023-06-01',
                ],
                CURLOPT_TIMEOUT => 30,
            ]);
            $handles['claude'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        // ── GPT-4o ──
        if ($openaiKey) {
            $payload = json_encode([
                'model'      => 'gpt-4o',
                'max_tokens' => 1024,
                'messages'   => [
                    ['role' => 'system', 'content' => 'You are an expert SEO architect. Be concise and precise.'],
                    ['role' => 'user',   'content' => $prompt],
                ],
            ]);
            $ch = curl_init('https://api.openai.com/v1/chat/completions');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => $payload,
                CURLOPT_HTTPHEADER     => [
                    'Content-Type: application/json',
                    'Authorization: Bearer ' . $openaiKey,
                ],
                CURLOPT_TIMEOUT => 30,
            ]);
            $handles['gpt4o'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        // ── Gemini ──
        if ($geminiKey) {
            $payload = json_encode([
                'contents' => [['parts' => [['text' => $prompt]]]],
                'generationConfig' => ['maxOutputTokens' => 1024],
            ]);
            $ch = curl_init("https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={$geminiKey}");
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => $payload,
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
                CURLOPT_TIMEOUT        => 30,
            ]);
            $handles['gemini'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        // Execute all in parallel
        $running = null;
        do {
            curl_multi_exec($mh, $running);
            curl_multi_select($mh);
        } while ($running > 0);

        // Collect responses
        foreach ($handles as $llm => $ch) {
            $raw = curl_multi_getcontent($ch);
            curl_multi_remove_handle($mh, $ch);
            curl_close($ch);

            $decoded = json_decode($raw, true);

            $results[$llm] = match($llm) {
                'claude' => isset($decoded['content'][0]['text'])
                    ? ['text' => $decoded['content'][0]['text']]
                    : ['error' => $decoded['error']['message'] ?? 'Unknown error'],

                'gpt4o'  => isset($decoded['choices'][0]['message']['content'])
                    ? ['text' => $decoded['choices'][0]['message']['content']]
                    : ['error' => $decoded['error']['message'] ?? 'Unknown error'],

                'gemini' => isset($decoded['candidates'][0]['content']['parts'][0]['text'])
                    ? ['text' => $decoded['candidates'][0]['content']['parts'][0]['text']]
                    : ['error' => $decoded['error']['message'] ?? 'Unknown error'],

                default => ['error' => 'Unknown LLM'],
            };
        }

        curl_multi_close($mh);
        return $results;
    }

    // ─────────────────────────────────────────────
    //  PARSE VERDICT FROM LLM RESPONSE
    // ─────────────────────────────────────────────

    private function parseVerdict(string $text): array
    {
        $verdict    = 'UNKNOWN';
        $confidence = 0;
        $summary    = '';
        $needsChange = 'no';
        $suggested  = 'none';

        if (preg_match('/VERDICT:\s*(PASS|FLAG)/i', $text, $m)) {
            $verdict = strtoupper(trim($m[1]));
        }
        if (preg_match('/CONFIDENCE:\s*(\d+)/i', $text, $m)) {
            $confidence = (int) $m[1];
        }
        if (preg_match('/SUMMARY:\s*(.+)/i', $text, $m)) {
            $summary = trim($m[1]);
        }
        if (preg_match('/NEEDS_ADJUSTMENT:\s*(yes|no)\s*[—-]\s*(.+)/i', $text, $m)) {
            $needsChange = strtolower(trim($m[1]));
            $suggested   = trim($m[2]);
        }

        return [
            'verdict'     => $verdict,
            'confidence'  => $confidence,
            'summary'     => $summary,
            'needs_change' => $needsChange,
            'suggested'   => $suggested,
            'raw'         => $text,
        ];
    }

    // ─────────────────────────────────────────────
    //  DETERMINE CONSENSUS ACROSS LLMs
    // ─────────────────────────────────────────────

    private function determineConsensus(array $verdicts): array
    {
        $passes     = 0;
        $flags      = 0;
        $totalConf  = 0;
        $count      = 0;

        foreach ($verdicts as $v) {
            if ($v['verdict'] === 'PASS') $passes++;
            if ($v['verdict'] === 'FLAG') $flags++;
            if ($v['confidence'] > 0) {
                $totalConf += $v['confidence'];
                $count++;
            }
        }

        $avgConf = $count > 0 ? round($totalConf / $count, 1) : 0;

        // Consensus rules:
        // - All PASS → VALIDATED
        // - 1+ FLAG → FLAGGED (any dissent surfaces for review)
        // - Avg confidence < 6 → FLAGGED regardless
        $status = 'VALIDATED';
        $reason = 'All LLMs agree rule is firing correctly.';

        if ($flags > 0) {
            $status = 'FLAGGED';
            $reason = "{$flags} of " . count($verdicts) . " LLMs flagged this rule for review.";
        } elseif ($avgConf < 6) {
            $status = 'FLAGGED';
            $reason = "Low average confidence ({$avgConf}/10) — rule needs review.";
        }

        return [
            'status'   => $status,
            'passes'   => $passes,
            'flags'    => $flags,
            'avg_conf' => $avgConf,
            'reason'   => $reason,
        ];
    }

    // ─────────────────────────────────────────────
    //  DISPLAY RESULTS
    // ─────────────────────────────────────────────

    private function displayResults(OutputInterface $output, array $rule, array $firingPages, array $verdicts, array $consensus): void
    {
        $icon = $consensus['status'] === 'VALIDATED' ? '✅' : '⚠️';
        $output->writeln("  {$icon} Consensus: {$consensus['status']} (avg confidence: {$consensus['avg_conf']}/10)");
        $output->writeln("  Reason: {$consensus['reason']}");
        $output->writeln('');
        $output->writeln('  LLM Verdicts:');

        foreach ($verdicts as $llm => $v) {
            $vIcon = $v['verdict'] === 'PASS' ? '✅' : '⚠️';
            $label = strtoupper($llm);
            $output->writeln("    {$vIcon} {$label}: {$v['verdict']} (confidence: {$v['confidence']}/10)");
            if ($v['summary']) {
                $output->writeln("       → {$v['summary']}");
            }
            if ($v['needs_change'] === 'yes' && $v['suggested'] !== 'none') {
                $output->writeln("       💡 Suggested change: {$v['suggested']}");
            }
        }

        if ($consensus['status'] === 'FLAGGED') {
            $output->writeln('');
            $output->writeln('  ── ACTION REQUIRED ──────────────────────────');
            $output->writeln("  Review this rule before relying on its output.");
            $output->writeln("  Check rule_evaluations table for full details.");
            $output->writeln('  ─────────────────────────────────────────────');
        }
    }

    // ─────────────────────────────────────────────
    //  STORE EVALUATION IN DB
    // ─────────────────────────────────────────────

    private function storeEvaluation(array $rule, array $firingPages, array $verdicts, array $consensus): void
    {
        try {
            $this->db->insert('rule_evaluations', [
                'rule_id'          => $rule['id'],
                'rule_name'        => $rule['name'],
                'pages_firing'     => count($firingPages),
                'sample_urls'      => json_encode(array_column(array_slice($firingPages, 0, 5), 'url')),
                'claude_verdict'   => $verdicts['claude']['verdict']    ?? 'N/A',
                'claude_conf'      => $verdicts['claude']['confidence'] ?? 0,
                'claude_summary'   => $verdicts['claude']['summary']    ?? '',
                'gpt4o_verdict'    => $verdicts['gpt4o']['verdict']     ?? 'N/A',
                'gpt4o_conf'       => $verdicts['gpt4o']['confidence']  ?? 0,
                'gpt4o_summary'    => $verdicts['gpt4o']['summary']     ?? '',
                'gemini_verdict'   => $verdicts['gemini']['verdict']    ?? 'N/A',
                'gemini_conf'      => $verdicts['gemini']['confidence'] ?? 0,
                'gemini_summary'   => $verdicts['gemini']['summary']    ?? '',
                'consensus_status' => $consensus['status'],
                'avg_confidence'   => $consensus['avg_conf'],
                'consensus_reason' => $consensus['reason'],
                'evaluated_at'     => date('Y-m-d H:i:s'),
            ]);
        } catch (\Exception $e) {
            // Non-fatal — evaluation still displayed even if storage fails
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
                    id                SERIAL PRIMARY KEY,
                    rule_id           VARCHAR(20) NOT NULL,
                    rule_name         TEXT,
                    pages_firing      INT DEFAULT 0,
                    sample_urls       TEXT,
                    claude_verdict    VARCHAR(10),
                    claude_conf       INT DEFAULT 0,
                    claude_summary    TEXT,
                    gpt4o_verdict     VARCHAR(10),
                    gpt4o_conf        INT DEFAULT 0,
                    gpt4o_summary     TEXT,
                    gemini_verdict    VARCHAR(10),
                    gemini_conf       INT DEFAULT 0,
                    gemini_summary    TEXT,
                    consensus_status  VARCHAR(20),
                    avg_confidence    NUMERIC(4,1),
                    consensus_reason  TEXT,
                    evaluated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
        } catch (\Exception $e) {
            // Table may already exist
        }
    }
}
