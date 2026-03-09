<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:evaluate-rule', description: 'Send a rule + its crawl findings to Claude, GPT-4o, and Gemini for consensus validation with deliberation loop')]
class EvaluateRuleCommand extends Command
{
    private const MAX_ROUNDS   = 3;
    private const ASSET_FILTER = "url NOT LIKE '%.pdf' AND url NOT LIKE '%.doc' AND url NOT LIKE '%.docx' AND url NOT LIKE '%.xls' AND url NOT LIKE '%.xlsx' AND url NOT LIKE '%.jpg' AND url NOT LIKE '%.jpeg' AND url NOT LIKE '%.png' AND url NOT LIKE '%.zip'";
    private const TIER4_URLS   = "'/contact-us/','/get-quote/','/trailer-finder/','/book-a-video-call/','/join-our-mailing-list/','/freebook/','/horse-trailer-safety-webinars/','/virtual-horse-trailer-safety-inspection/'";

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('rule',        null, InputOption::VALUE_OPTIONAL, 'Specific rule ID (e.g. FC-R7). Omit to evaluate all firing rules.')
            ->addOption('dry-run',     null, InputOption::VALUE_NONE,     'Show what would be sent to LLMs without calling APIs')
            ->addOption('verbose-llm', null, InputOption::VALUE_NONE,     'Show full LLM responses per round');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $ruleFilter = $input->getOption('rule');
        $dryRun     = (bool) $input->getOption('dry-run');
        $verboseLlm = (bool) $input->getOption('verbose-llm');

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
        $output->writeln('+==========================================+');
        $output->writeln('|     LOGIRI MULTI-LLM RULE EVALUATOR      |');
        $output->writeln('|        with 3-Round Deliberation         |');
        $output->writeln('+==========================================+');
        $output->writeln('');

        $totalEvaluated = 0;
        $totalFlagged   = 0;

        foreach ($rules as $rule) {
            $firingPages = $this->getFiringPages($rule);

            if (empty($firingPages)) {
                $output->writeln("[ ] {$rule['id']} -- no pages currently firing, skipping.");
                continue;
            }

            $output->writeln(">> Evaluating {$rule['id']}: {$rule['name']}");
            $output->writeln("   Pages firing: " . count($firingPages));

            $basePrompt = $this->buildPrompt($rule, $firingPages);

            if ($dryRun) {
                $output->writeln("   [DRY RUN] Prompt preview:");
                $output->writeln("   " . substr($basePrompt, 0, 300) . "...");
                $output->writeln('');
                continue;
            }

            $allRounds      = [];
            $finalVerdicts  = [];
            $finalConsensus = null;
            $roundsRun      = 0;

            for ($round = 1; $round <= self::MAX_ROUNDS; $round++) {
                $roundsRun = $round;
                $output->writeln("   Round {$round} of " . self::MAX_ROUNDS . "...");

                $prompt = ($round === 1)
                    ? $basePrompt
                    : $this->buildDeliberationPrompt($basePrompt, $allRounds, $round);

                $responses = $this->callAllLLMs($prompt);

                $roundVerdicts = [];
                foreach ($responses as $llm => $response) {
                    if (isset($response['error'])) {
                        $output->writeln("   [!] {$llm}: API error -- {$response['error']}");
                        if (isset($allRounds[$round - 1][$llm])) {
                            $roundVerdicts[$llm] = $allRounds[$round - 1][$llm];
                            $output->writeln("       (carrying forward Round " . ($round - 1) . " verdict for {$llm})");
                        }
                        continue;
                    }
                    $parsed = $this->parseVerdict($response['text']);
                    $roundVerdicts[$llm] = $parsed;
                    if ($verboseLlm) {
                        $output->writeln("   [R{$round}:{$llm}] " . substr($response['text'], 0, 150));
                    }
                }

                $allRounds[$round] = $roundVerdicts;
                $consensus         = $this->determineConsensus($roundVerdicts);
                $passes            = $consensus['passes'];
                $flags             = $consensus['flags'];
                $total             = count($roundVerdicts);

                $output->writeln("   Round {$round} result: {$consensus['status']} (passes:{$passes} flags:{$flags} of {$total})");

                // Unanimous agreement -- stop early
                if ($passes === $total || $flags === $total) {
                    $output->writeln("   >> Unanimous -- stopping deliberation.");
                    $finalVerdicts  = $roundVerdicts;
                    $finalConsensus = $consensus;
                    break;
                }

                // Final round -- majority vote
                if ($round === self::MAX_ROUNDS) {
                    $output->writeln("   >> Max rounds reached -- applying majority vote.");
                    $finalVerdicts  = $roundVerdicts;
                    $finalConsensus = $this->determineMajority($roundVerdicts, $allRounds);
                    break;
                }

                $output->writeln("   >> No consensus -- proceeding to Round " . ($round + 1) . " with peer review.");
            }

            $this->displayResults($output, $rule, $firingPages, $finalVerdicts, $finalConsensus, $allRounds, $roundsRun);
            $this->storeEvaluation($rule, $firingPages, $finalVerdicts, $finalConsensus, $allRounds, $roundsRun);

            $totalEvaluated++;
            if (in_array($finalConsensus['status'], ['FLAGGED', 'NEEDS_HUMAN_REVIEW'])) {
                $totalFlagged++;
            }

            $output->writeln('');
        }

        $output->writeln('==============================================');
        $output->writeln("SUMMARY: {$totalEvaluated} rules evaluated | {$totalFlagged} flagged for review");
        $output->writeln('');

        if ($totalFlagged > 0) {
            $output->writeln("  Run: php bin/console app:evaluate-rule --rule=FC-RX to dig into a specific rule");
            $output->writeln("  View evaluations: SELECT * FROM rule_evaluations ORDER BY evaluated_at DESC;");
        } else {
            $output->writeln("  All evaluated rules passed LLM consensus.");
        }

        return Command::SUCCESS;
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
        $peer .= "PEER REVIEW -- Round {$prevRound} verdicts from your fellow evaluators:\n";
        $peer .= str_repeat('=', 60) . "\n";

        foreach ($prevVerdicts as $llm => $v) {
            $peer .= "\n" . strtoupper($llm) . ": {$v['verdict']} (confidence: {$v['confidence']}/10)\n";
            if ($v['summary'])  $peer .= "  Summary: {$v['summary']}\n";
            if ($v['needs_change'] === 'yes' && $v['suggested'] !== 'none') {
                $peer .= "  Suggested change: {$v['suggested']}\n";
            }
        }

        $peer .= "\n" . str_repeat('=', 60) . "\n";
        $peer .= "ROUND {$currentRound} INSTRUCTIONS:\n";

        if ($isFinal) {
            $peer .= "This is the FINAL round. Commit to your FINAL position -- no further rounds.\n";
            $peer .= "If you are changing your verdict, explain which peer argument convinced you.\n";
            $peer .= "If you are maintaining your verdict, state that clearly.\n";
        } else {
            $peer .= "Consider the peer arguments above. You may revise your verdict or maintain it.\n";
            $peer .= "If you revise, explain which peer argument convinced you and why.\n";
        }

        $peer .= "Respond in the SAME structured format as before.\n";
        $peer .= str_repeat('=', 60);

        return $basePrompt . $peer;
    }

    // ─────────────────────────────────────────────
    //  MAJORITY VOTE (after Round 3 if no unanimity)
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
            $reason = "Deadlock after {$rounds} rounds -- LLMs split evenly. Human review required.";
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

        preg_match_all('/\n(FC-R\d+)\s*\|\s*([^\n]+)\n(.*?)(?=\nFC-R\d+|\nRESULTS VERIFICATION|\z)/s', $content, $matches, PREG_SET_ORDER);

        foreach ($matches as $match) {
            $ruleText         = trim($match[3]);
            $triggerCondition = '';
            $diagnosis        = '';

            if (preg_match('/Trigger Condition:\s*([^\n]+)/', $ruleText, $m)) $triggerCondition = trim($m[1]);
            if (preg_match('/Diagnosis:\s*([^\n]+)/',         $ruleText, $m)) $diagnosis        = trim($m[1]);

            $rules[] = [
                'id'                => trim($match[1]),
                'name'              => trim($match[2]),
                'full_text'         => $ruleText,
                'trigger_condition' => $triggerCondition,
                'diagnosis'         => $diagnosis,
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
            $t4 = self::TIER4_URLS;

            $query = match($rule['id']) {
                'FC-R1'  => "SELECT url, page_type, h1, title_tag, word_count, has_central_entity, central_entity_count FROM page_crawl_snapshots WHERE has_central_entity IS NOT TRUE AND is_noindex IS NOT TRUE AND {$af} AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R2'  => "SELECT url, page_type, h1, title_tag FROM page_crawl_snapshots WHERE (page_type IS NULL OR page_type NOT IN ('core','outer')) AND is_noindex IS NOT TRUE AND {$af} LIMIT 10",
                'FC-R3'  => "SELECT url, page_type, word_count, h1, title_tag FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count < 500 AND is_noindex IS NOT TRUE LIMIT 10",
                'FC-R5'  => "SELECT url, page_type, has_core_link, core_links_found FROM page_crawl_snapshots WHERE page_type = 'outer' AND has_core_link IS NOT TRUE AND is_noindex IS NOT TRUE AND {$af} AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R6'  => "SELECT url, page_type, word_count, h2s, schema_types FROM page_crawl_snapshots WHERE page_type = 'core' AND word_count < 800 AND is_noindex IS NOT TRUE LIMIT 10",
                'FC-R7'  => "SELECT url, page_type, h1, title_tag, h1_matches_title FROM page_crawl_snapshots WHERE (h1_matches_title IS NOT TRUE OR h1 IS NULL OR h1 = '') AND is_noindex IS NOT TRUE AND {$af} AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R8'  => "SELECT url, page_type, h2s, word_count FROM page_crawl_snapshots WHERE page_type = 'core' AND (h2s IS NULL OR h2s = '[]' OR h2s = '') AND is_noindex IS NOT TRUE AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R9'  => "SELECT url, page_type, schema_types, h1 FROM page_crawl_snapshots WHERE page_type = 'core' AND (schema_types IS NULL OR schema_types = '[]' OR schema_types = '') AND is_noindex IS NOT TRUE AND url NOT LIKE '%//' AND url NOT IN ({$t4}) LIMIT 10",
                'FC-R10' => "SELECT p.url, p.page_type, p.has_core_link, g.impressions FROM page_crawl_snapshots p JOIN gsc_snapshots g ON g.page LIKE CONCAT('%', p.url) WHERE p.page_type = 'outer' AND p.has_core_link IS NOT TRUE AND g.impressions >= 100 AND g.date_range = '28d' ORDER BY g.impressions DESC LIMIT 10",
                default  => null,
            };

            if (!$query) return [];
            return $this->db->fetchAllAssociative($query);
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  BUILD BASE PROMPT
    // ─────────────────────────────────────────────

    private function buildPrompt(array $rule, array $firingPages): string
    {
        $pageList = '';
        foreach (array_slice($firingPages, 0, 5) as $page) {
            $pageList .= "\n- URL: " . ($page['url'] ?? 'n/a');
            foreach ($page as $key => $val) {
                if ($key === 'url' || in_array($key, ['internal_links','crawled_at'])) continue;
                $pageList .= " | {$key}: " . (is_null($val) ? 'NULL' : $val);
            }
        }

        $total = count($firingPages);

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

CURRENT FIRING DATA ({$total} pages triggering this rule):
{$pageList}

YOUR EVALUATION TASK:
1. Is this rule firing correctly given the data above? (yes/no)
2. Are there false positives -- pages being flagged that shouldn't be? (yes/no, explain)
3. Are there false negatives -- pages NOT being flagged that should be? (yes/no, explain)
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
    //  CALL ALL THREE LLMs IN PARALLEL
    // ─────────────────────────────────────────────

    private function callAllLLMs(string $prompt): array
    {
        $claudeKey = $_ENV['ANTHROPIC_API_KEY'] ?? '';
        $openaiKey = $_ENV['OPENAI_API_KEY']    ?? '';
        $geminiKey = $_ENV['GEMINI_API_KEY']    ?? '';

        $handles = [];
        $mh      = curl_multi_init();

        if ($claudeKey) {
            $ch = curl_init('https://api.anthropic.com/v1/messages');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'claude-sonnet-4-6', 'max_tokens' => 1024, 'messages' => [['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'x-api-key: ' . $claudeKey, 'anthropic-version: 2023-06-01'],
                CURLOPT_TIMEOUT        => 45,
            ]);
            $handles['claude'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($openaiKey) {
            $ch = curl_init('https://api.openai.com/v1/chat/completions');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'gpt-4o', 'max_tokens' => 1024, 'messages' => [['role' => 'system', 'content' => 'You are an expert SEO architect. Be concise and precise.'], ['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'Authorization: Bearer ' . $openaiKey],
                CURLOPT_TIMEOUT        => 45,
            ]);
            $handles['gpt4o'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($geminiKey) {
            $ch = curl_init("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={$geminiKey}");
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['contents' => [['parts' => [['text' => $prompt]]]], 'generationConfig' => ['maxOutputTokens' => 1024]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
                CURLOPT_TIMEOUT        => 45,
            ]);
            $handles['gemini'] = $ch;
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
                'claude' => isset($decoded['content'][0]['text'])                              ? ['text' => $decoded['content'][0]['text']]                              : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'gpt4o'  => isset($decoded['choices'][0]['message']['content'])                ? ['text' => $decoded['choices'][0]['message']['content']]                : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'gemini' => isset($decoded['candidates'][0]['content']['parts'][0]['text'])    ? ['text' => $decoded['candidates'][0]['content']['parts'][0]['text']]    : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                default  => ['error' => 'Unknown LLM'],
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

        // ── Primary: structured format ──────────────────────────────────────────
        if (preg_match('/VERDICT\s*:\s*(PASS|FLAG)/i',                        $text, $m)) $verdict    = strtoupper(trim($m[1]));
        if (preg_match('/SUMMARY\s*:\s*(.+)/i',                               $text, $m)) $summary    = trim($m[1]);
        if (preg_match('/NEEDS_ADJUSTMENT\s*:\s*(yes|no)\s*[—\-–]\s*(.+)/i', $text, $m)) {
            $needsChange = strtolower(trim($m[1]));
            $suggested   = trim($m[2]);
        }

        // ── Confidence: runs always (not just in fallback block) ────────────────
        // Handles structured "CONFIDENCE: 8", prose "confidence of 8/10", and
        // Round-2 narrative responses like "I have high confidence (8/10) that..."
        if (preg_match('/CONFIDENCE\s*:\s*(\d+)/i',                $text, $m)) $confidence = (int) $m[1];
        if ($confidence === 0 && preg_match('/(\d+)\s*\/\s*10/i',  $text, $m)) $confidence = (int) $m[1];
        if ($confidence === 0 && preg_match('/confidence[^.]{0,30}?(\d+)/i', $text, $m)) $confidence = (int) $m[1];

        // ── Fallback: handles Gemini semi-structured / Claude prose responses ───
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

        // ── Summary fallback: skip structured field lines (KEY: value format) ──
        // Prevents Gemini's "FIRING_CORRECTLY: no" from leaking into the summary
        if (!$summary) {
            $lines = preg_split('/\r?\n/', strip_tags($text));
            foreach ($lines as $line) {
                $line = trim($line);
                // Skip blank lines and structured field lines (e.g. "FIRING_CORRECTLY: yes")
                if ($line === '' || preg_match('/^[A-Z_]+\s*:\s*/i', $line)) continue;
                // Skip lines that are just filler phrases
                if (strlen($line) < 20) continue;
                $summary = $line;
                break;
            }
            // Last resort: first 120 chars of raw text
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
    //  DISPLAY RESULTS
    // ─────────────────────────────────────────────

    private function displayResults(OutputInterface $output, array $rule, array $firingPages, array $verdicts, array $consensus, array $allRounds, int $roundsRun): void
    {
        $icon = $consensus['status'] === 'VALIDATED' ? '[PASS]' : '[FLAG]';
        $output->writeln('');
        $output->writeln("  {$icon} Final Consensus: {$consensus['status']} (avg confidence: {$consensus['avg_conf']}/10) after {$roundsRun} round(s)");
        $output->writeln("  " . (($consensus['majority'] ?? false) ? '[MAJORITY] ' : '') . $consensus['reason']);
        $output->writeln('');
        $output->writeln('  Final LLM Verdicts:');

        foreach ($verdicts as $llm => $v) {
            $vIcon   = $v['verdict'] === 'PASS' ? '[PASS]' : '[FLAG]';
            $changed = '';
            if ($roundsRun > 1 && isset($allRounds[1][$llm]) && $allRounds[1][$llm]['verdict'] !== $v['verdict']) {
                $changed = " [changed from {$allRounds[1][$llm]['verdict']} in R1]";
            }
            $output->writeln("    {$vIcon} " . strtoupper($llm) . ": {$v['verdict']} (confidence: {$v['confidence']}/10){$changed}");
            if ($v['summary'])                                              $output->writeln("       -> " . $v['summary']);
            if ($v['needs_change'] === 'yes' && $v['suggested'] !== 'none') $output->writeln("       SUGGESTED: " . $v['suggested']);
        }

        if ($roundsRun > 1) {
            $output->writeln('');
            $output->writeln('  Deliberation history:');
            for ($r = 1; $r <= $roundsRun; $r++) {
                $parts = [];
                foreach (($allRounds[$r] ?? []) as $llm => $v) { $parts[] = strtoupper($llm) . ':' . $v['verdict']; }
                $output->writeln("    Round {$r}: " . implode(' | ', $parts));
            }
        }

        if (in_array($consensus['status'], ['FLAGGED', 'NEEDS_HUMAN_REVIEW'])) {
            $output->writeln('');
            $output->writeln('  -- ACTION REQUIRED ------------------------------------------');
            if ($consensus['status'] === 'NEEDS_HUMAN_REVIEW') {
                $output->writeln('  DEADLOCK: LLMs split after ' . self::MAX_ROUNDS . ' rounds. Human review required.');
            } else {
                $output->writeln('  Review this rule before relying on its output.');
            }
            $output->writeln('  Check rule_evaluations table for full details.');
            $output->writeln('  -------------------------------------------------------------');
        }
    }

    // ─────────────────────────────────────────────
    //  STORE EVALUATION
    // ─────────────────────────────────────────────

    private function storeEvaluation(array $rule, array $firingPages, array $verdicts, array $consensus, array $allRounds, int $roundsRun): void
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
                'rounds_run'       => $roundsRun,
                'round_history'    => json_encode($allRounds),
                'evaluated_at'     => date('Y-m-d H:i:s'),
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
                    consensus_status  VARCHAR(30),
                    avg_confidence    NUMERIC(4,1),
                    consensus_reason  TEXT,
                    rounds_run        INT DEFAULT 1,
                    round_history     TEXT,
                    evaluated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
            $this->db->executeStatement("ALTER TABLE rule_evaluations ADD COLUMN IF NOT EXISTS rounds_run INT DEFAULT 1");
            $this->db->executeStatement("ALTER TABLE rule_evaluations ADD COLUMN IF NOT EXISTS round_history TEXT");
        } catch (\Exception $e) {
            // May already exist
        }
    }
}
