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
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Max keywords to fetch', 500);
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
        $output->writeln('+==========================================+');
        $output->writeln('');

        $limit = (int) ($input->getOption('limit') ?? 500);

        // ── 1. Ranked Keywords: what is DDT ranking for? ──
        if (!$input->getOption('skip-keywords')) {
            $output->writeln('Fetching ranked keywords for ' . self::DOMAIN . '...');
            $keywordsData = $this->fetchRankedKeywords($login, $password, $limit);
            if ($keywordsData) {
                $saved = $this->saveKeywordData($keywordsData, 'ranked');
                $output->writeln("  Saved {$saved} ranked keywords.");
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
            } else {
                $output->writeln('  [WARN] No volume data returned.');
            }
        }

        // ── 3. Competitor Keywords: what do competitors rank for that DDT doesn't? ──
        if (!$input->getOption('skip-competitors')) {
            $output->writeln('Fetching competitor keyword gaps...');
            $competitors = ['featherlite.com', 'sundowner.com', 'brantzmfg.com'];
            foreach ($competitors as $comp) {
                $output->writeln("  Analyzing {$comp}...");
                $gapData = $this->fetchCompetitorGap($login, $password, $comp, $limit);
                if ($gapData) {
                    $saved = $this->saveCompetitorData($gapData, $comp);
                    $output->writeln("    Found {$saved} keyword gaps vs {$comp}.");
                }
            }
        }

        // ── 4. Domain Overview: organic traffic estimate, authority ──
        $output->writeln('Fetching domain overview...');
        $overview = $this->fetchDomainOverview($login, $password);
        if ($overview) {
            $this->saveDomainOverview($overview);
            $output->writeln("  Domain metrics saved.");
        }

        $output->writeln('');
        $output->writeln('DataForSEO fetch complete.');
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
                "SELECT DISTINCT query FROM gsc_snapshots WHERE date_range = '28d' AND query != '__PAGE_AGGREGATE__' ORDER BY impressions DESC LIMIT 200"
            );
        } catch (\Exception $e) {
            return null;
        }

        if (empty($queries)) return null;

        // DataForSEO allows up to 1000 keywords per request
        $chunks = array_chunk($queries, 100);
        $allResults = [];

        foreach ($chunks as $chunk) {
            $payload = [[
                'keywords'      => $chunk,
                'location_name' => 'United States',
                'language_name' => 'English',
            ]];

            $result = $this->callApi($login, $password, 'keywords_data/google_ads/search_volume/live', $payload);
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
                $keyword = $item['keyword'] ?? '';
                if (!$keyword) continue;

                try {
                    $this->db->executeStatement(
                        "UPDATE dataforseo_keywords SET
                            search_volume = :vol,
                            cpc = :cpc,
                            competition = :comp
                         WHERE keyword = :keyword
                         AND snapshot_id = (SELECT MAX(snapshot_id) FROM dataforseo_keywords WHERE keyword = :keyword2)",
                        [
                            'vol'      => $item['search_volume'] ?? 0,
                            'cpc'      => $item['cpc'] ?? 0,
                            'comp'     => $item['competition'] ?? 0,
                            'keyword'  => $keyword,
                            'keyword2' => $keyword,
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
                    position        INT DEFAULT 0,
                    url             TEXT DEFAULT '',
                    snapshot_id     VARCHAR(20) NOT NULL,
                    source          VARCHAR(30) DEFAULT 'ranked',
                    fetched_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(keyword, snapshot_id)
                )
            ");

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

    
