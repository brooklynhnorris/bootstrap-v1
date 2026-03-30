<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:check-live-rank', description: 'Real-time SERP position check via ValueSERP — validates if DDT actually ranks for target queries')]
class CheckLiveRankCommand extends Command
{
    private const DOMAIN = 'doubledtrailers.com';
    private const API_URL = 'https://api.valueserp.com/search';

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('task', null, InputOption::VALUE_OPTIONAL, 'Check rank for a specific task ID')
            ->addOption('query', null, InputOption::VALUE_OPTIONAL, 'Check rank for a specific query')
            ->addOption('url', null, InputOption::VALUE_OPTIONAL, 'Check rank for a specific URL path (e.g. /bumper-pull-horse-trailers/)')
            ->addOption('verify-outcomes', null, InputOption::VALUE_NONE, 'Run as part of verify-outcomes pipeline — check all recently completed tasks')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Max queries to check', 20);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $apiKey = $_ENV['VALUESERP_API_KEY'] ?? '';

        if (!$apiKey) {
            $output->writeln('[ERROR] VALUESERP_API_KEY env var required.');
            $output->writeln('  Sign up at https://app.valueserp.com/ and add the key to .env.local');
            return Command::FAILURE;
        }

        $this->ensureSchema();

        $output->writeln('');
        $output->writeln('+==========================================+');
        $output->writeln('|   LOGIRI — Live SERP Position Checker    |');
        $output->writeln('|   Powered by ValueSERP                   |');
        $output->writeln('+==========================================+');
        $output->writeln('');

        $limit = (int) ($input->getOption('limit') ?? 20);

        // ── Mode: Single query check ──
        if ($query = $input->getOption('query')) {
            $url = $input->getOption('url') ?: null;
            $result = $this->checkSingleQuery($apiKey, $query, $output);
            if ($result) {
                $this->displayResult($output, $query, $result, $url);
            }
            return Command::SUCCESS;
        }

        // ── Mode: Single task check ──
        if ($taskId = $input->getOption('task')) {
            return $this->checkTask($apiKey, (int) $taskId, $output);
        }

        // ── Mode: Verify outcomes pipeline ──
        if ($input->getOption('verify-outcomes')) {
            return $this->checkAllVerifiedTasks($apiKey, $limit, $output);
        }

        // ── Default: Check top GSC queries for DDT ──
        $output->writeln("Checking live SERP positions for top DDT queries...");
        return $this->checkTopQueries($apiKey, $limit, $output);
    }

    // ─────────────────────────────────────────────
    //  CHECK SINGLE QUERY
    // ─────────────────────────────────────────────

    private function checkSingleQuery(string $apiKey, string $query, OutputInterface $output): ?array
    {
        $output->writeln("Checking: \"{$query}\"...");

        $params = [
            'api_key'  => $apiKey,
            'q'        => $query,
            'location' => 'United States',
            'gl'       => 'us',
            'hl'       => 'en',
            'num'      => 100,
            'output'   => 'json',
        ];

        $url = self::API_URL . '?' . http_build_query($params);

        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => 30,
            CURLOPT_CONNECTTIMEOUT => 10,
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error    = curl_error($ch);
        curl_close($ch);

        if ($error || $httpCode !== 200) {
            $output->writeln("  [ERROR] API call failed: {$error} (HTTP {$httpCode})");
            return null;
        }

        $data = json_decode($response, true);
        if (!$data || !isset($data['organic_results'])) {
            $output->writeln("  [ERROR] No organic results in response.");
            return null;
        }

        // Find DDT in results
        $ddtPosition = null;
        $ddtUrl = null;
        $ddtSnippet = null;
        $totalResults = count($data['organic_results']);

        foreach ($data['organic_results'] as $result) {
            $link = $result['link'] ?? '';
            if (str_contains(strtolower($link), self::DOMAIN)) {
                $ddtPosition = $result['position'] ?? null;
                $ddtUrl = $link;
                $ddtSnippet = $result['snippet'] ?? '';
                break;
            }
        }

        // Also check for AI overview mentions
        $aiOverviewMention = false;
        if (isset($data['ai_overview'])) {
            $aiText = json_encode($data['ai_overview']);
            $aiOverviewMention = str_contains(strtolower($aiText), 'double d') || str_contains(strtolower($aiText), self::DOMAIN);
        }

        // People Also Ask
        $paa = [];
        if (isset($data['related_questions'])) {
            foreach (array_slice($data['related_questions'], 0, 5) as $q) {
                $paa[] = $q['question'] ?? '';
            }
        }

        $resultData = [
            'query'              => $query,
            'ddt_position'       => $ddtPosition,
            'ddt_url'            => $ddtUrl,
            'ddt_snippet'        => $ddtSnippet,
            'total_results'      => $totalResults,
            'ai_overview_mention'=> $aiOverviewMention,
            'people_also_ask'    => $paa,
            'top_3'              => array_map(fn($r) => [
                'position' => $r['position'] ?? null,
                'domain'   => parse_url($r['link'] ?? '', PHP_URL_HOST),
                'title'    => $r['title'] ?? '',
            ], array_slice($data['organic_results'], 0, 3)),
            'checked_at'         => date('Y-m-d H:i:s'),
        ];

        // Store result
        $this->storeSerpCheck($resultData);

        return $resultData;
    }

    // ─────────────────────────────────────────────
    //  CHECK A SPECIFIC TASK
    // ─────────────────────────────────────────────

    private function checkTask(string $apiKey, int $taskId, OutputInterface $output): int
    {
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$taskId]);
        if (!$task) {
            $output->writeln("[ERROR] Task {$taskId} not found.");
            return Command::FAILURE;
        }

        $output->writeln("Task: {$task['title']}");

        // Extract URL from task title
        $url = '';
        if (preg_match('#(/[a-z0-9\-/]+/)#', $task['title'], $m)) {
            $url = $m[1];
        }

        // Find relevant queries from GSC for this URL
        $queries = [];
        if ($url) {
            try {
                $queries = $this->db->fetchFirstColumn(
                    "SELECT DISTINCT query FROM gsc_snapshots WHERE page LIKE :url AND date_range = '28d' AND query != '__PAGE_AGGREGATE__' ORDER BY impressions DESC LIMIT 5",
                    ['url' => '%' . ltrim($url, '/')]
                );
            } catch (\Exception $e) {}
        }

        if (empty($queries)) {
            // Fall back to extracting keywords from the task title
            $title = preg_replace('/^\[[^\]]+\]\s*/', '', $task['title']);
            $title = preg_replace('/\s*—\s*\/[^\/]+\/$/', '', $title);
            if ($title) $queries[] = $title;
        }

        $output->writeln("  URL: {$url}");
        $output->writeln("  Queries to check: " . implode(', ', $queries));
        $output->writeln('');

        foreach ($queries as $query) {
            $result = $this->checkSingleQuery($apiKey, $query, $output);
            if ($result) {
                $this->displayResult($output, $query, $result, $url);
            }
        }

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  CHECK ALL VERIFIED TASKS (for verify pipeline)
    // ─────────────────────────────────────────────

    private function checkAllVerifiedTasks(string $apiKey, int $limit, OutputInterface $output): int
    {
        // Get recently completed tasks that have been verified
        $tasks = $this->db->fetchAllAssociative(
            "SELECT id, title, rule_id, recheck_result FROM tasks
             WHERE status = 'done' AND recheck_verified = TRUE
             ORDER BY completed_at DESC LIMIT ?",
            [$limit]
        );

        $output->writeln("Checking live SERP for {$limit} recently verified tasks...");

        $checked = 0;
        foreach ($tasks as $task) {
            // Extract URL
            $url = '';
            if (preg_match('#(/[a-z0-9\-/]+/)#', $task['title'], $m)) {
                $url = $m[1];
            }
            if (!$url) continue;

            // Get top query for this URL from GSC
            try {
                $query = $this->db->fetchOne(
                    "SELECT query FROM gsc_snapshots WHERE page LIKE :url AND date_range = '28d' AND query != '__PAGE_AGGREGATE__' ORDER BY impressions DESC LIMIT 1",
                    ['url' => '%' . ltrim($url, '/')]
                );
            } catch (\Exception $e) { continue; }

            if (!$query) continue;

            $result = $this->checkSingleQuery($apiKey, $query, $output);
            if ($result) {
                $this->displayResult($output, $query, $result, $url);

                // Update task with live rank data
                try {
                    $this->db->executeStatement(
                        "UPDATE tasks SET live_rank = :rank, live_rank_checked_at = NOW() WHERE id = :id",
                        ['rank' => $result['ddt_position'] ?? 0, 'id' => $task['id']]
                    );
                } catch (\Exception $e) {}
            }

            $checked++;
            // Rate limit: ~1 req/sec to be safe
            usleep(500000);
        }

        $output->writeln('');
        $output->writeln("Checked {$checked} live SERP positions.");
        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  CHECK TOP GSC QUERIES
    // ─────────────────────────────────────────────

    private function checkTopQueries(string $apiKey, int $limit, OutputInterface $output): int
    {
        try {
            $queries = $this->db->fetchAllAssociative(
                "SELECT query, page, impressions, position FROM gsc_snapshots
                 WHERE date_range = '28d' AND query != '__PAGE_AGGREGATE__'
                 AND impressions > 100
                 ORDER BY impressions DESC LIMIT ?",
                [$limit]
            );
        } catch (\Exception $e) {
            $output->writeln("[ERROR] Could not fetch GSC queries: " . $e->getMessage());
            return Command::FAILURE;
        }

        foreach ($queries as $q) {
            $result = $this->checkSingleQuery($apiKey, $q['query'], $output);
            if ($result) {
                $gscPos = round((float) $q['position'], 1);
                $livePos = $result['ddt_position'] ?? 'Not found';
                $output->writeln("  GSC position: {$gscPos} | Live: {$livePos}");
                $output->writeln('');
            }
            usleep(500000); // Rate limit
        }

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  DISPLAY RESULT
    // ─────────────────────────────────────────────

    private function displayResult(OutputInterface $output, string $query, array $result, ?string $targetUrl = null): void
    {
        $pos = $result['ddt_position'];
        $icon = $pos ? ($pos <= 10 ? '[PAGE 1]' : ($pos <= 30 ? '[TOP 30]' : '[' . $pos . ']')) : '[NOT FOUND]';

        $output->writeln("  {$icon} \"{$query}\"");

        if ($pos) {
            $output->writeln("    DDT Position: #{$pos}");
            $output->writeln("    Ranking URL: {$result['ddt_url']}");
            if ($targetUrl && !str_contains($result['ddt_url'] ?? '', $targetUrl)) {
                $output->writeln("    ⚠ WRONG URL RANKING — expected {$targetUrl}");
            }
        } else {
            $output->writeln("    DDT not found in top 100 results for this query.");
        }

        if ($result['ai_overview_mention']) {
            $output->writeln("    ✦ DDT mentioned in AI Overview!");
        }

        // Top 3 competitors
        if (!empty($result['top_3'])) {
            $output->writeln("    Top 3: " . implode(' | ', array_map(fn($r) => "#{$r['position']} {$r['domain']}", $result['top_3'])));
        }

        $output->writeln('');
    }

    // ─────────────────────────────────────────────
    //  STORE SERP CHECK
    // ─────────────────────────────────────────────

    private function storeSerpCheck(array $result): void
    {
        try {
            $this->db->insert('live_serp_checks', [
                'query'               => $result['query'],
                'ddt_position'        => $result['ddt_position'],
                'ddt_url'             => $result['ddt_url'],
                'ddt_snippet'         => substr($result['ddt_snippet'] ?? '', 0, 500),
                'total_results'       => $result['total_results'],
                'ai_overview_mention' => $result['ai_overview_mention'] ? 'TRUE' : 'FALSE',
                'top_3_json'          => json_encode($result['top_3']),
                'paa_json'            => json_encode($result['people_also_ask']),
                'checked_at'          => $result['checked_at'],
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
                CREATE TABLE IF NOT EXISTS live_serp_checks (
                    id                   SERIAL PRIMARY KEY,
                    query                TEXT NOT NULL,
                    ddt_position         INT DEFAULT NULL,
                    ddt_url              TEXT DEFAULT NULL,
                    ddt_snippet          TEXT DEFAULT NULL,
                    total_results        INT DEFAULT 0,
                    ai_overview_mention  BOOLEAN DEFAULT FALSE,
                    top_3_json           JSONB DEFAULT '[]',
                    paa_json             JSONB DEFAULT '[]',
                    checked_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");

            // Add live_rank columns to tasks table
            $this->db->executeStatement("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS live_rank INT DEFAULT NULL");
            $this->db->executeStatement("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS live_rank_checked_at TIMESTAMP DEFAULT NULL");
        } catch (\Exception $e) {
            // May already exist
        }
    }
}

    
