<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:fetch-dataforseo', description: 'Fetch keyword rankings, search volumes, and competitor data from DataForSEO (replaces SEMrush)')]
class FetchDataForSeoCommand extends Command
{
    private const DOMAIN = 'doubledtrailers.com';
    private const API_URL = 'https://api.dataforseo.com/v3/';

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('skip-keywords', null, InputOption::VALUE_NONE, 'Skip ranked keywords fetch')
            ->addOption('skip-competitors', null, InputOption::VALUE_NONE, 'Skip competitor analysis')
            ->addOption('skip-volumes', null, InputOption::VALUE_NONE, 'Skip keyword search volume fetch')
            ->addOption('skip-serp', null, InputOption::VALUE_NONE, 'Skip live SERP position checks')
            ->addOption('skip-backlinks', null, InputOption::VALUE_NONE, 'Skip backlink summary')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Max keywords to fetch', 500)
            ->addOption('serp-limit', null, InputOption::VALUE_OPTIONAL, 'Max SERP queries to check live', 20);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $login    = $_ENV['DATAFORSEO_LOGIN'] ?? '';
        $password = $_ENV['DATAFORSEO_PASSWORD'] ?? '';

        if (!$login || !$password) {
            $output->writeln('[ERROR] DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD env vars required.');
            $output->writeln('  Sign up at https://app.dataforseo.com/ and add credentials to .env.local');
            return Command::FAILURE;
        }

        $this->ensureSchema();

        $output->writeln('');
        $output->writeln('+==========================================+');
        $output->writeln('|    LOGIRI — DataForSEO Data Fetcher      |');
        $output->writeln('|    Keywords · SERP · Backlinks · Gaps     |');
        $output->writeln('+==========================================+');
        $output->writeln('');

        $limit     = (int) ($input->getOption('limit') ?? 500);
        $serpLimit = (int) ($input->getOption('serp-limit') ?? 20);
        $totalCost = 0;

        // ── 1. Ranked Keywords: what is DDT ranking for? ──
        if (!$input->getOption('skip-keywords')) {
            $output->writeln('Fetching ranked keywords for ' . self::DOMAIN . '...');
            $keywordsData = $this->fetchRankedKeywords($login, $password, $limit);
            if ($keywordsData) {
                $saved = $this->saveKeywordData($keywordsData, 'ranked');
                $output->writeln("  Saved {$saved} ranked keywords.");
                $totalCost += 0.01 + ($saved * 0.0001);
            } else {
                $output->writeln('  [WARN] No ranked keywords returned.');
            }
        }

        // ── 2. Keyword Search Volumes: enrich with volumes ──
        if (!$input->getOption('skip-volumes')) {
            $output->writeln('Fetching search volumes for top queries from GSC...');
            $volumeData = $this->fetchSearchVolumes($login, $password);
            if ($volumeData) {
                $saved = $this->saveVolumeData($volumeData);
                $output->writeln("  Enriched {$saved} keywords with search volume data.");
                $totalCost += 0.075;
            } else {
                $output->writeln('  [WARN] No volume data returned.');
            }
        }

        // ── 3. Competitor Keywords: what do competitors rank for that DDT doesn't? ──
        if (!$input->getOption('skip-competitors')) {
            $output->writeln('Fetching competitor keyword gaps...');
            $competitors = ['featherlite.com', 'sundowner.com'];
            foreach ($competitors as $comp) {
                $output->writeln("  Analyzing {$comp}...");
                $gapData = $this->fetchCompetitorGap($login, $password, $comp, $limit);
                if ($gapData) {
                    $saved = $this->saveCompetitorData($gapData, $comp);
                    $output->writeln("    Found {$saved} keyword gaps vs {$comp}.");
                    $totalCost += 0.01 + ($saved * 0.0001);
                }
            }
        }

        // ── 4. Live SERP Checks: verify real-time positions ──
        if (!$input->getOption('skip-serp')) {
            $output->writeln("Checking live SERP positions (up to {$serpLimit} queries)...");
            $serpResults = $this->checkLiveSerpPositions($login, $password, $serpLimit, $output);
            $totalCost += $serpResults * 0.002;
        }

        // ── 5. Backlink Summary ──
        if (!$input->getOption('skip-backlinks')) {
            $output->writeln('Fetching backlink summary...');
            $blData = $this->fetchBacklinkSummary($login, $password);
            if ($blData) {
                $this->saveBacklinkData($blData);
                $output->writeln("  Backlink profile saved.");
                $totalCost += 0.02;
            }
        }

        // ── 6. Domain Overview ──
        $output->writeln('Fetching domain overview...');
        $overview = $this->fetchDomainOverview($login, $password);
        if ($overview) {
            $this->saveDomainOverview($overview);
            $output->writeln("  Domain metrics saved.");
            $totalCost += 0.01;
        }

        $output->writeln('');
        $output->writeln(sprintf('DataForSEO fetch complete. Estimated cost: $%.4f', $totalCost));
        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  RANKED KEYWORDS
    // ─────────────────────────────────────────────

    private function fetchRankedKeywords(string $login, string $password, int $limit): ?array
    {
        $payload = [[
            'target'        => self::DOMAIN,
            'location_name' => 'United States',
            'language_name' => 'English',
            'limit'         => $limit,
            'order_by'      => ['keyword_data.keyword_info.search_volume,desc'],
            'filters'       => [
                ['ranked_serp_element.serp_item.rank_absolute', '<=', 100]
            ],
        ]];

        return $this->callApi($login, $password, 'dataforseo_labs/google/ranked_keywords/live', $payload);
    }

    // ─────────────────────────────────────────────
    //  SEARCH VOLUMES (enrich GSC queries)
    // ─────────────────────────────────────────────

    private function fetchSearchVolumes(string $login, string $password): ?array
    {
        // Get top queries from GSC that we want volumes for
        try {
            $queries = $this->db->fetchFirstColumn(
                "SELECT query FROM (SELECT query, MAX(impressions) as max_imp FROM gsc_snapshots WHERE date_range = '28d' AND query != '__PAGE_AGGREGATE__' GROUP BY query ORDER BY max_imp DESC LIMIT 200) sub"
            );
        } catch (\Exception $e) {
            return null;
        }

        if (empty($queries)) return null;

        // Use dataforseo_labs/google/keyword_overview/live
        // Returns keyword_info.search_volume, keyword_info.cpc, keyword_info.competition
        // $0.01 per request + $0.0001 per result
        $chunks = array_chunk($queries, 100);
        $allResults = [];

        foreach ($chunks as $chunk) {
            $payload = [[
                'keywords'      => $chunk,
                'location_name' => 'United States',
                'language_name' => 'English',
            ]];

            $result = $this->callApi($login, $password, 'dataforseo_labs/google/keyword_overview/live', $payload);
            if ($result) {
                $allResults = array_merge($allResults, $result);
            }
        }

        return !empty($allResults) ? $allResults : null;
    }

    // ─────────────────────────────────────────────
    //  COMPETITOR GAP ANALYSIS
    // ─────────────────────────────────────────────

    private function fetchCompetitorGap(string $login, string $password, string $competitor, int $limit): ?array
    {
        $payload = [[
            'target1'       => self::DOMAIN,
            'target2'       => $competitor,
            'location_name' => 'United States',
            'language_name' => 'English',
            'limit'         => min($limit, 200),
            'intersections' => [
                'target1_not_target2' => false,   // keywords comp has but DDT doesn't
                'target2_not_target1' => true,
            ],
            'order_by'      => ['first_tag_keyword_data.keyword_info.search_volume,desc'],
        ]];

        return $this->callApi($login, $password, 'dataforseo_labs/google/domain_intersection/live', $payload);
    }

    // ─────────────────────────────────────────────
    //  LIVE SERP POSITION CHECKS (replaces ValueSERP)
    // ─────────────────────────────────────────────

    private function checkLiveSerpPositions(string $login, string $password, int $limit, OutputInterface $output): int
    {
        // Get top queries from recently verified tasks + high-impression GSC queries
        $queries = [];

        // Priority 1: queries associated with recently verified tasks
        try {
            $taskQueries = $this->db->fetchAllAssociative(
                "SELECT DISTINCT g.query, g.page, g.impressions, g.position
                 FROM gsc_snapshots g
                 JOIN tasks t ON g.page LIKE CONCAT('%', SUBSTRING(t.title FROM '/[a-z0-9\-/]+/'), '%')
                 WHERE t.status = 'done' AND t.recheck_verified = TRUE
                 AND g.date_range = '28d' AND g.query != '__PAGE_AGGREGATE__'
                 ORDER BY g.impressions DESC LIMIT " . intval($limit / 2)
            );
            foreach ($taskQueries as $tq) {
                $queries[] = ['query' => $tq['query'], 'gsc_page' => $tq['page'], 'gsc_position' => $tq['position']];
            }
        } catch (\Exception $e) {}

        // Priority 2: fill remaining with top impression queries
        $remaining = $limit - count($queries);
        if ($remaining > 0) {
            try {
                $existingQueries = array_column($queries, 'query');
                $topQueries = $this->db->fetchAllAssociative(
                    "SELECT query, page, impressions, position FROM gsc_snapshots
                     WHERE date_range = '28d' AND query != '__PAGE_AGGREGATE__'
                     AND impressions > 50
                     ORDER BY impressions DESC LIMIT " . intval($remaining)
                );
                foreach ($topQueries as $tq) {
                    if (!in_array($tq['query'], $existingQueries)) {
                        $queries[] = ['query' => $tq['query'], 'gsc_page' => $tq['page'], 'gsc_position' => $tq['position']];
                    }
                }
            } catch (\Exception $e) {}
        }

        $checked = 0;
        foreach ($queries as $q) {
            $query = $q['query'];

            // Use DataForSEO SERP API (live/advanced) — $0.002 per request
            $payload = [[
                'keyword'       => $query,
                'location_name' => 'United States',
                'language_name' => 'English',
                'device'        => 'desktop',
                'os'            => 'windows',
                'depth'         => 100,
            ]];

            $result = $this->callApi($login, $password, 'serp/google/organic/live/advanced', $payload);
            if (!$result) continue;

            $items = $result[0]['items'] ?? [];
            $ddtPosition = null;
            $ddtUrl = null;
            $top3 = [];

            foreach ($items as $item) {
                if ($item['type'] !== 'organic') continue;
                $link = $item['url'] ?? '';
                $pos = $item['rank_absolute'] ?? null;

                if (count($top3) < 3) {
                    $top3[] = ['position' => $pos, 'domain' => $item['domain'] ?? '', 'title' => substr($item['title'] ?? '', 0, 60)];
                }

                if (str_contains(strtolower($link), self::DOMAIN)) {
                    $ddtPosition = $pos;
                    $ddtUrl = $link;
                }
            }

            // Check for AI overview
            $aiMention = false;
            foreach ($items as $item) {
                if ($item['type'] === 'ai_overview') {
                    $text = json_encode($item);
                    $aiMention = str_contains(strtolower($text), 'double d') || str_contains(strtolower($text), self::DOMAIN);
                    break;
                }
            }

            // People Also Ask
            $paa = [];
            foreach ($items as $item) {
                if ($item['type'] === 'people_also_ask') {
                    foreach ($item['items'] ?? [] as $paaItem) {
                        $paa[] = $paaItem['title'] ?? '';
                    }
                    break;
                }
            }

            // Store
            try {
                $this->db->insert('live_serp_checks', [
                    'query'               => $query,
                    'ddt_position'        => $ddtPosition,
                    'ddt_url'             => $ddtUrl,
                    'gsc_position'        => round((float) ($q['gsc_position'] ?? 0), 1),
                    'position_delta'      => $ddtPosition && $q['gsc_position'] ? round($ddtPosition - (float) $q['gsc_position'], 1) : null,
                    'total_results'       => count($items),
                    'ai_overview_mention' => $aiMention ? 'TRUE' : 'FALSE',
                    'top_3_json'          => json_encode($top3),
                    'paa_json'            => json_encode(array_slice($paa, 0, 5)),
                    'checked_at'          => date('Y-m-d H:i:s'),
                ]);
            } catch (\Exception $e) {}

            // Display
            $gscPos = round((float) ($q['gsc_position'] ?? 0), 1);
            $liveStr = $ddtPosition ? "#{$ddtPosition}" : 'NOT FOUND';
            $deltaStr = '';
            if ($ddtPosition && $gscPos > 0) {
                $delta = round($ddtPosition - $gscPos, 1);
                $deltaStr = $delta > 0 ? " (↓{$delta} vs GSC)" : ($delta < 0 ? " (↑" . abs($delta) . " vs GSC)" : " (= GSC)");
            }
            $output->writeln("  [{$liveStr}{$deltaStr}] \"{$query}\"" . ($aiMention ? ' ✦AI' : ''));

            $checked++;
            usleep(300000); // Rate limit
        }

        $output->writeln("  Checked {$checked} live SERP positions.");
        return $checked;
    }

    // ─────────────────────────────────────────────
    //  BACKLINK SUMMARY
    // ─────────────────────────────────────────────

    private function fetchBacklinkSummary(string $login, string $password): ?array
    {
        $payload = [[
            'target' => self::DOMAIN,
        ]];

        return $this->callApi($login, $password, 'backlinks/summary/live', $payload);
    }

    private function saveBacklinkData(array $result): void
    {
        $data = $result[0] ?? $result;
        if (!$data) return;

        try {
            $this->db->executeStatement(
                "INSERT INTO backlink_snapshots (domain, total_backlinks, referring_domains, dofollow, nofollow, domain_rank, fetched_at)
                 VALUES (:domain, :total, :ref_domains, :dofollow, :nofollow, :rank, NOW())
                 ON CONFLICT (domain, DATE(fetched_at)) DO UPDATE SET
                    total_backlinks = EXCLUDED.total_backlinks,
                    referring_domains = EXCLUDED.referring_domains,
                    dofollow = EXCLUDED.dofollow,
                    nofollow = EXCLUDED.nofollow,
                    domain_rank = EXCLUDED.domain_rank",
                [
                    'domain'      => self::DOMAIN,
                    'total'       => $data['backlinks'] ?? 0,
                    'ref_domains' => $data['referring_domains'] ?? 0,
                    'dofollow'    => $data['referring_links_types']['dofollow'] ?? 0,
                    'nofollow'    => $data['referring_links_types']['nofollow'] ?? 0,
                    'rank'        => $data['rank'] ?? 0,
                ]
            );
        } catch (\Exception $e) {}
    }

    // ─────────────────────────────────────────────
    //  DOMAIN OVERVIEW
    // ─────────────────────────────────────────────

    private function fetchDomainOverview(string $login, string $password): ?array
    {
        $payload = [[
            'target'        => self::DOMAIN,
            'location_name' => 'United States',
            'language_name' => 'English',
        ]];

        return $this->callApi($login, $password, 'dataforseo_labs/google/domain_rank_overview/live', $payload);
    }

    // ─────────────────────────────────────────────
    //  API CALLER
    // ─────────────────────────────────────────────

    private function callApi(string $login, string $password, string $endpoint, array $payload): ?array
    {
        $ch = curl_init(self::API_URL . $endpoint);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => json_encode($payload),
            CURLOPT_HTTPHEADER     => [
                'Content-Type: application/json',
                'Authorization: Basic ' . base64_encode("{$login}:{$password}"),
            ],
            CURLOPT_TIMEOUT        => 120,
            CURLOPT_CONNECTTIMEOUT => 15,
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error    = curl_error($ch);
        curl_close($ch);

        if ($error || $httpCode !== 200) {
            return null;
        }

        $data = json_decode($response, true);
        if (($data['status_code'] ?? 0) !== 20000) {
            return null;
        }

        return $data['tasks'][0]['result'] ?? null;
    }

    // ─────────────────────────────────────────────
    //  DATA STORAGE
    // ─────────────────────────────────────────────

    private function saveKeywordData(array $result, string $source): int
    {
        $saved = 0;
        $items = $result[0]['items'] ?? $result;
        if (!is_array($items)) return 0;

        $snapshotId = date('Y-m-d_H-i');

        foreach ($items as $item) {
            $kd = $item['keyword_data'] ?? $item;
            $keyword = $kd['keyword'] ?? ($kd['keyword_data']['keyword'] ?? '');
            if (!$keyword) continue;

            $kwInfo = $kd['keyword_info'] ?? ($kd['keyword_data']['keyword_info'] ?? []);
            $serpItem = $item['ranked_serp_element']['serp_item'] ?? [];

            try {
                $this->db->executeStatement(
                    "INSERT INTO dataforseo_keywords (keyword, search_volume, cpc, competition, position, url, snapshot_id, source, fetched_at)
                     VALUES (:keyword, :volume, :cpc, :competition, :position, :url, :snapshot, :source, NOW())
                     ON CONFLICT (keyword, snapshot_id) DO UPDATE SET
                        search_volume = EXCLUDED.search_volume,
                        cpc = EXCLUDED.cpc,
                        competition = EXCLUDED.competition,
                        position = EXCLUDED.position,
                        url = EXCLUDED.url",
                    [
                        'keyword'     => $keyword,
                        'volume'      => $kwInfo['search_volume'] ?? 0,
                        'cpc'         => $kwInfo['cpc'] ?? 0,
                        'competition' => $kwInfo['competition'] ?? 0,
                        'position'    => $serpItem['rank_absolute'] ?? 0,
                        'url'         => $serpItem['relative_url'] ?? '',
                        'snapshot'    => $snapshotId,
                        'source'      => $source,
                    ]
                );
                $saved++;
            } catch (\Exception $e) {
                // Non-fatal
            }
        }

        // Also update the semrush_snapshots table for backwards compatibility
        try {
            $totalKeywords = count($items);
            $totalTraffic = array_sum(array_map(fn($i) => ($i['keyword_data']['keyword_info']['search_volume'] ?? 0), $items));
            $this->db->executeStatement(
                "INSERT INTO semrush_snapshots (organic_keywords, organic_traffic, fetched_at) VALUES (:kw, :traffic, NOW())",
                ['kw' => $totalKeywords, 'traffic' => $totalTraffic]
            );
        } catch (\Exception $e) {}

        return $saved;
    }

    private function saveVolumeData(array $results): int
    {
        $saved = 0;
        foreach ($results as $result) {
            $items = $result['items'] ?? [];
            foreach ($items as $item) {
                // bulk_keyword_difficulty returns: keyword, keyword_data.keyword_info.search_volume, keyword_difficulty
                $keyword = $item['keyword'] ?? '';
                if (!$keyword) continue;

                $kwInfo = $item['keyword_data']['keyword_info'] ?? $item['keyword_info'] ?? [];
                $volume = $kwInfo['search_volume'] ?? $item['search_volume'] ?? 0;
                $cpc = $kwInfo['cpc'] ?? $item['cpc'] ?? 0;
                $comp = $kwInfo['competition'] ?? $item['competition'] ?? 0;
                $difficulty = $item['keyword_difficulty'] ?? 0;

                try {
                    // Upsert into dataforseo_keywords
                    $snapshotId = date('Y-m-d_H-i');
                    $this->db->executeStatement(
                        "INSERT INTO dataforseo_keywords (keyword, search_volume, cpc, competition, keyword_difficulty, snapshot_id, source, fetched_at)
                         VALUES (:keyword, :vol, :cpc, :comp, :diff, :snapshot, 'volume', NOW())
                         ON CONFLICT (keyword, snapshot_id) DO UPDATE SET
                            search_volume = EXCLUDED.search_volume,
                            cpc = EXCLUDED.cpc,
                            competition = EXCLUDED.competition,
                            keyword_difficulty = EXCLUDED.keyword_difficulty",
                        [
                            'keyword'  => $keyword,
                            'vol'      => $volume,
                            'cpc'      => $cpc,
                            'comp'     => $comp,
                            'diff'     => $difficulty,
                            'snapshot' => $snapshotId,
                        ]
                    );
                    $saved++;
                } catch (\Exception $e) {}
            }
        }
        return $saved;
    }

    private function saveCompetitorData(array $result, string $competitor): int
    {
        $saved = 0;
        $items = $result[0]['items'] ?? $result;
        if (!is_array($items)) return 0;

        foreach ($items as $item) {
            $keyword = $item['keyword'] ?? ($item['keyword_data']['keyword'] ?? '');
            if (!$keyword) continue;

            $kwInfo = $item['keyword_info'] ?? ($item['keyword_data']['keyword_info'] ?? []);

            try {
                $this->db->executeStatement(
                    "INSERT INTO competitor_keyword_gaps (keyword, search_volume, competitor_domain, competitor_position, ddt_position, fetched_at)
                     VALUES (:keyword, :vol, :comp, :comp_pos, :ddt_pos, NOW())
                     ON CONFLICT (keyword, competitor_domain) DO UPDATE SET
                        search_volume = EXCLUDED.search_volume,
                        competitor_position = EXCLUDED.competitor_position,
                        ddt_position = EXCLUDED.ddt_position,
                        fetched_at = NOW()",
                    [
                        'keyword'  => $keyword,
                        'vol'      => $kwInfo['search_volume'] ?? 0,
                        'comp'     => $competitor,
                        'comp_pos' => $item['ranked_serp_element']['serp_item']['rank_absolute'] ?? 0,
                        'ddt_pos'  => 0, // DDT doesn't rank for this keyword
                    ]
                );
                $saved++;
            } catch (\Exception $e) {}
        }
        return $saved;
    }

    private function saveDomainOverview(array $result): void
    {
        $items = $result[0] ?? $result;
        if (!$items) return;

        // Store as a semrush_snapshots row for backwards compatibility
        try {
            $this->db->executeStatement(
                "INSERT INTO semrush_snapshots (organic_keywords, organic_traffic, fetched_at) VALUES (:kw, :traffic, NOW())",
                [
                    'kw'      => $items['organic']['count'] ?? $items['metrics']['organic']['count'] ?? 0,
                    'traffic' => $items['organic']['etv'] ?? $items['metrics']['organic']['etv'] ?? 0,
                ]
            );
        } catch (\Exception $e) {}
    }

    // ─────────────────────────────────────────────
    //  ENSURE DB SCHEMA
    // ─────────────────────────────────────────────

    private function ensureSchema(): void
    {
        try {
            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS dataforseo_keywords (
                    id              SERIAL PRIMARY KEY,
                    keyword         TEXT NOT NULL,
                    search_volume   INT DEFAULT 0,
                    cpc             NUMERIC(8,2) DEFAULT 0,
                    competition     NUMERIC(5,4) DEFAULT 0,
                    keyword_difficulty INT DEFAULT 0,
                    position        INT DEFAULT 0,
                    url             TEXT DEFAULT '',
                    snapshot_id     VARCHAR(20) NOT NULL,
                    source          VARCHAR(30) DEFAULT 'ranked',
                    fetched_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(keyword, snapshot_id)
                )
            ");
            $this->db->executeStatement("ALTER TABLE dataforseo_keywords ADD COLUMN IF NOT EXISTS keyword_difficulty INT DEFAULT 0");

            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS competitor_keyword_gaps (
                    id                   SERIAL PRIMARY KEY,
                    keyword              TEXT NOT NULL,
                    search_volume        INT DEFAULT 0,
                    competitor_domain    VARCHAR(100) NOT NULL,
                    competitor_position  INT DEFAULT 0,
                    ddt_position         INT DEFAULT 0,
                    fetched_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(keyword, competitor_domain)
                )
            ");

            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS live_serp_checks (
                    id                   SERIAL PRIMARY KEY,
                    query                TEXT NOT NULL,
                    ddt_position         INT DEFAULT NULL,
                    ddt_url              TEXT DEFAULT NULL,
                    gsc_position         NUMERIC(5,1) DEFAULT NULL,
                    position_delta       NUMERIC(5,1) DEFAULT NULL,
                    total_results        INT DEFAULT 0,
                    ai_overview_mention  BOOLEAN DEFAULT FALSE,
                    top_3_json           JSONB DEFAULT '[]',
                    paa_json             JSONB DEFAULT '[]',
                    checked_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");

            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS backlink_snapshots (
                    id                SERIAL PRIMARY KEY,
                    domain            VARCHAR(100) NOT NULL,
                    total_backlinks   INT DEFAULT 0,
                    referring_domains INT DEFAULT 0,
                    dofollow          INT DEFAULT 0,
                    nofollow          INT DEFAULT 0,
                    domain_rank       INT DEFAULT 0,
                    fetched_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(domain, DATE(fetched_at))
                )
            ");

            // Add live_rank columns to tasks table
            $this->db->executeStatement("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS live_rank INT DEFAULT NULL");
            $this->db->executeStatement("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS live_rank_checked_at TIMESTAMP DEFAULT NULL");

            // Ensure semrush_snapshots exists for backwards compat
            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS semrush_snapshots (
                    id               SERIAL PRIMARY KEY,
                    organic_keywords INT DEFAULT 0,
                    organic_traffic  INT DEFAULT 0,
                    fetched_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
        } catch (\Exception $e) {
            // Tables may already exist
        }
    }
}

    

