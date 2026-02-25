<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:fetch-google-ads', description: 'Fetch Google Ads campaign, keyword, and spend data')]
class FetchGoogleAdsCommand extends Command
{
    private ?OutputInterface $output = null;
    private bool $debug = false;

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('debug', null, InputOption::VALUE_NONE, 'Print API response snippets when no data or on error');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $clientId       = $_ENV['GOOGLE_CLIENT_ID'] ?? '';
        $clientSecret   = $_ENV['GOOGLE_CLIENT_SECRET'] ?? '';
        $refreshToken   = $_ENV['GOOGLE_REFRESH_TOKEN'] ?? '';
        $developerToken = $_ENV['GOOGLE_ADS_DEVELOPER_TOKEN'] ?? '';
        $customerId     = $_ENV['GOOGLE_ADS_CUSTOMER_ID'] ?? ''; // e.g. 123-456-7890 or 1234567890

        if (!$clientId || !$clientSecret || !$refreshToken) {
            $output->writeln('Missing Google OAuth credentials. Set GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN.');
            return Command::FAILURE;
        }

        if (!$developerToken || !$customerId) {
            $output->writeln('Missing Ads credentials. Set GOOGLE_ADS_DEVELOPER_TOKEN and GOOGLE_ADS_CUSTOMER_ID.');
            return Command::FAILURE;
        }

        // Strip dashes from customer ID
        $customerId = str_replace('-', '', $customerId);

        $this->output = $output;
        $this->debug = (bool) $input->getOption('debug');

        $output->writeln('Google Ads fetch (with diagnostics).');
        // ── Get access token ──
        $output->writeln('Getting Google access token...');
        $tokenResponse = file_get_contents('https://oauth2.googleapis.com/token', false, stream_context_create([
            'http' => [
                'method'        => 'POST',
                'header'        => 'Content-Type: application/x-www-form-urlencoded',
                'content'       => http_build_query([
                    'client_id'     => $clientId,
                    'client_secret' => $clientSecret,
                    'refresh_token' => $refreshToken,
                    'grant_type'    => 'refresh_token',
                ]),
                'ignore_errors' => true,
            ],
        ]));

        $tokenData = json_decode($tokenResponse, true);
        if (!isset($tokenData['access_token'])) {
            $output->writeln('Failed to get access token: ' . ($tokenData['error_description'] ?? 'Unknown error'));
            return Command::FAILURE;
        }

        $accessToken = $tokenData['access_token'];
        $output->writeln('Got access token.');

        // ── Ensure schema ──
        $this->ensureSchema($output);

        // ── Clear old data ──
        $this->db->executeStatement("DELETE FROM google_ads_snapshots");
        $output->writeln('Cleared old Google Ads data.');

        $totalRows = 0;

        // ── Fetch 1: Campaign performance (30 days) ──
        $output->writeln('Fetching campaign performance (30 days)...');
        $campaigns = $this->fetchCampaigns($accessToken, $developerToken, $customerId, 30);
        foreach ($campaigns as $row) {
            $m = $row['metrics'] ?? [];
            $this->db->insert('google_ads_snapshots', [
                'data_type'    => 'campaign',
                'campaign_id'  => $row['campaign']['id'] ?? '',
                'campaign_name'=> $row['campaign']['name'] ?? '',
                'ad_group_id'  => null,
                'ad_group_name'=> null,
                'keyword'      => null,
                'match_type'   => null,
                'impressions'  => (int) ($m['impressions'] ?? 0),
                'clicks'       => (int) ($m['clicks'] ?? 0),
                'cost_micros'  => (int) round((float) ($m['costMicros'] ?? 0)),
                'conversions'  => round((float) ($m['conversions'] ?? 0), 2),
                'ctr'          => round((float) ($m['ctr'] ?? 0), 4),
                'average_cpc'  => (int) round((float) ($m['averageCpc'] ?? 0)),
                'status'       => $row['campaign']['status'] ?? '',
                'date_range'   => '30d',
                'fetched_at'   => date('Y-m-d H:i:s'),
            ]);
            $totalRows++;
        }
        $output->writeln("  Saved " . count($campaigns) . " campaign rows.");

        // ── Fetch 2: Keyword performance (30 days) ──
        $output->writeln('Fetching keyword performance (30 days)...');
        $keywords = $this->fetchKeywords($accessToken, $developerToken, $customerId, 30);
        foreach ($keywords as $row) {
            $m = $row['metrics'] ?? [];
            $this->db->insert('google_ads_snapshots', [
                'data_type'    => 'keyword',
                'campaign_id'  => $row['campaign']['id'] ?? '',
                'campaign_name'=> $row['campaign']['name'] ?? '',
                'ad_group_id'  => $row['adGroup']['id'] ?? '',
                'ad_group_name'=> $row['adGroup']['name'] ?? '',
                'keyword'      => $row['adGroupCriterion']['keyword']['text'] ?? '',
                'match_type'   => $row['adGroupCriterion']['keyword']['matchType'] ?? '',
                'impressions'  => (int) ($m['impressions'] ?? 0),
                'clicks'       => (int) ($m['clicks'] ?? 0),
                'cost_micros'  => (int) round((float) ($m['costMicros'] ?? 0)),
                'conversions'  => round((float) ($m['conversions'] ?? 0), 2),
                'ctr'          => round((float) ($m['ctr'] ?? 0), 4),
                'average_cpc'  => (int) round((float) ($m['averageCpc'] ?? 0)),
                'status'       => $row['adGroupCriterion']['status'] ?? '',
                'date_range'   => '30d',
                'fetched_at'   => date('Y-m-d H:i:s'),
            ]);
            $totalRows++;
        }
        $output->writeln("  Saved " . count($keywords) . " keyword rows.");

        // ── Fetch 3: Search terms (30 days) ──
        $output->writeln('Fetching search term report (30 days)...');
        $searchTerms = $this->fetchSearchTerms($accessToken, $developerToken, $customerId, 30);
        foreach ($searchTerms as $row) {
            $m = $row['metrics'] ?? [];
            $this->db->insert('google_ads_snapshots', [
                'data_type'    => 'search_term',
                'campaign_id'  => $row['campaign']['id'] ?? '',
                'campaign_name'=> $row['campaign']['name'] ?? '',
                'ad_group_id'  => $row['adGroup']['id'] ?? '',
                'ad_group_name'=> $row['adGroup']['name'] ?? '',
                'keyword'      => $row['searchTermView']['searchTerm'] ?? '',
                'match_type'   => null,
                'impressions'  => (int) ($m['impressions'] ?? 0),
                'clicks'       => (int) ($m['clicks'] ?? 0),
                'cost_micros'  => (int) round((float) ($m['costMicros'] ?? 0)),
                'conversions'  => round((float) ($m['conversions'] ?? 0), 2),
                'ctr'          => round((float) ($m['ctr'] ?? 0), 4),
                'average_cpc'  => (int) round((float) ($m['averageCpc'] ?? 0)),
                'status'       => null,
                'date_range'   => '30d',
                'fetched_at'   => date('Y-m-d H:i:s'),
            ]);
            $totalRows++;
        }
        $output->writeln("  Saved " . count($searchTerms) . " search term rows.");

        // ── Fetch 4: Daily spend (last 90 days for trend) ──
        $output->writeln('Fetching daily spend trend (90 days)...');
        $dailySpend = $this->fetchDailySpend($accessToken, $developerToken, $customerId, 90);
        foreach ($dailySpend as $row) {
            $m = $row['metrics'] ?? [];
            $this->db->insert('google_ads_snapshots', [
                'data_type'    => 'daily_spend',
                'campaign_id'  => null,
                'campaign_name'=> null,
                'ad_group_id'  => null,
                'ad_group_name'=> null,
                'keyword'      => null,
                'match_type'   => null,
                'impressions'  => (int) ($m['impressions'] ?? 0),
                'clicks'       => (int) ($m['clicks'] ?? 0),
                'cost_micros'  => (int) round((float) ($m['costMicros'] ?? 0)),
                'conversions'  => round((float) ($m['conversions'] ?? 0), 2),
                'ctr'          => round((float) ($m['ctr'] ?? 0), 4),
                'average_cpc'  => (int) round((float) ($m['averageCpc'] ?? 0)),
                'status'       => null,
                'date_range'   => $row['segments']['date'] ?? '',
                'fetched_at'   => date('Y-m-d H:i:s'),
            ]);
            $totalRows++;
        }
        $output->writeln("  Saved " . count($dailySpend) . " daily spend rows.");

        $output->writeln("Done! Total: {$totalRows} Google Ads rows saved to database.");

        return Command::SUCCESS;
    }

    // ── Campaign performance query ──
    private function fetchCampaigns(string $token, string $devToken, string $customerId, int $days): array
    {
        $startDate = date('Y-m-d', strtotime("-{$days} days"));
        $endDate   = date('Y-m-d', strtotime('-1 day'));

        $query = "
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.ctr,
                metrics.average_cpc
            FROM campaign
            WHERE segments.date BETWEEN '{$startDate}' AND '{$endDate}'
              AND campaign.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
            LIMIT 1000
        ";

        return $this->runGaqlQuery($token, $devToken, $customerId, $query, 'campaign');
    }

    // ── Keyword performance query ──
    private function fetchKeywords(string $token, string $devToken, string $customerId, int $days): array
    {
        $startDate = date('Y-m-d', strtotime("-{$days} days"));
        $endDate   = date('Y-m-d', strtotime('-1 day'));

        $query = "
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.status,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.ctr,
                metrics.average_cpc
            FROM keyword_view
            WHERE segments.date BETWEEN '{$startDate}' AND '{$endDate}'
              AND ad_group_criterion.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
            LIMIT 5000
        ";

        return $this->runGaqlQuery($token, $devToken, $customerId, $query, 'keyword');
    }

    // ── Search terms query ──
    private function fetchSearchTerms(string $token, string $devToken, string $customerId, int $days): array
    {
        $startDate = date('Y-m-d', strtotime("-{$days} days"));
        $endDate   = date('Y-m-d', strtotime('-1 day'));

        $query = "
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                search_term_view.search_term,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.ctr,
                metrics.average_cpc
            FROM search_term_view
            WHERE segments.date BETWEEN '{$startDate}' AND '{$endDate}'
            ORDER BY metrics.clicks DESC
            LIMIT 5000
        ";

        return $this->runGaqlQuery($token, $devToken, $customerId, $query, 'search_term');
    }

    // ── Daily spend trend query ──
    private function fetchDailySpend(string $token, string $devToken, string $customerId, int $days): array
    {
        $startDate = date('Y-m-d', strtotime("-{$days} days"));
        $endDate   = date('Y-m-d', strtotime('-1 day'));

        $query = "
            SELECT
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.ctr,
                metrics.average_cpc
            FROM customer
            WHERE segments.date BETWEEN '{$startDate}' AND '{$endDate}'
            ORDER BY segments.date ASC
        ";

        return $this->runGaqlQuery($token, $devToken, $customerId, $query, 'daily_spend');
    }

    // ── Core GAQL request ──
    private function runGaqlQuery(string $token, string $devToken, string $customerId, string $query, string $label = 'query'): array
    {
        $url = "https://googleads.googleapis.com/v23/customers/{$customerId}/googleAds:searchStream";

        $response = @file_get_contents($url, false, stream_context_create([
            'http' => [
                'method'        => 'POST',
                'header'        => implode("\r\n", [
                    "Content-Type: application/json",
                    "Authorization: Bearer {$token}",
                    "developer-token: {$devToken}",
                ]),
                'content'       => json_encode(['query' => trim($query)]),
                'ignore_errors' => true,
            ],
        ]));

        $statusLine = $http_response_header[0] ?? '';
        $statusCode = (int) preg_replace('/^HTTP\/\S+\s+(\d+).*$/i', '$1', $statusLine);

        if ($response === false) {
            if ($this->output) {
                $this->output->writeln("<comment>[{$label}] Request failed (no response body). Status: {$statusLine}</comment>");
            }
            return [];
        }

        // Non-2xx: try to parse API error and show it
        if ($statusCode >= 400) {
            $err = json_decode($response, true);
            $msg = isset($err['error']['message'])
                ? $err['error']['message']
                : (isset($err['error']['status']) ? ($err['error']['status'] . ' – ' . ($err['error']['message'] ?? $response)) : $response);
            if ($this->output) {
                $this->output->writeln("<error>[{$label}] API error (HTTP {$statusCode}): {$msg}</error>");
                if ($this->debug) {
                    $this->output->writeln('[DEBUG] Raw: ' . substr($response, 0, 800));
                }
            }
            return [];
        }

        // Single JSON error object (e.g. API returns 200 but body is {"error": ...})
        $single = json_decode($response, true);
        if (is_array($single) && isset($single['error'])) {
            $msg = $single['error']['message'] ?? json_encode($single['error']);
            if ($this->output) {
                $this->output->writeln("<error>[{$label}] API error: {$msg}</error>");
                if ($this->debug) {
                    $this->output->writeln('[DEBUG] Raw: ' . substr($response, 0, 800));
                }
            }
            return [];
        }

        // searchStream can return: (1) single JSON array [{ "results": [...] }, ...], or (2) NDJSON lines
        $results = [];
        $full = json_decode($response, true);
        if (is_array($full)) {
            // Single JSON array of batches
            foreach ($full as $batch) {
                if (is_array($batch) && isset($batch['results'])) {
                    $results = array_merge($results, $batch['results']);
                }
                if (is_array($batch) && isset($batch['error'])) {
                    $msg = $batch['error']['message'] ?? json_encode($batch['error']);
                    if ($this->output) {
                        $this->output->writeln("<error>[{$label}] Stream error: {$msg}</error>");
                    }
                    return [];
                }
            }
        } else {
            // Fallback: newline-delimited JSON (one batch per line)
            foreach (explode("\n", trim($response)) as $line) {
                $line = trim($line);
                if (!$line || $line === '[' || $line === ']') continue;
                $line = ltrim($line, ',');
                $decoded = json_decode($line, true);
                if (isset($decoded['results'])) {
                    $results = array_merge($results, $decoded['results']);
                }
                if (is_array($decoded ?? null) && isset($decoded['error'])) {
                    $msg = $decoded['error']['message'] ?? json_encode($decoded['error']);
                    if ($this->output) {
                        $this->output->writeln("<error>[{$label}] Stream error: {$msg}</error>");
                    }
                    return [];
                }
            }
        }

        // When 0 rows, always show diagnostic so we can see why (HTTP status + response snippet or "empty")
        if (count($results) === 0 && $this->output) {
            $len = strlen($response);
            $status = trim($statusLine);
            if ($len > 0) {
                $snippet = substr($response, 0, 800);
                $this->output->writeln("[{$label}] 0 rows. HTTP: {$status} | Response ({$len} bytes): " . $snippet . ($len > 800 ? '...' : ''));
            } else {
                $this->output->writeln("[{$label}] 0 rows. HTTP: {$status} | Response: (empty body)");
            }
        }

        return $results;
    }

    // ── Schema setup ──
    private function ensureSchema(OutputInterface $output): void
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        );

        if (!in_array('google_ads_snapshots', $tables)) {
            $output->writeln('Creating google_ads_snapshots table...');
            $this->db->executeStatement("
                CREATE TABLE google_ads_snapshots (
                    id            SERIAL PRIMARY KEY,
                    data_type     VARCHAR(30)  NOT NULL,
                    campaign_id   VARCHAR(30)  DEFAULT NULL,
                    campaign_name VARCHAR(255) DEFAULT NULL,
                    ad_group_id   VARCHAR(30)  DEFAULT NULL,
                    ad_group_name VARCHAR(255) DEFAULT NULL,
                    keyword       TEXT         DEFAULT NULL,
                    match_type    VARCHAR(20)  DEFAULT NULL,
                    impressions   BIGINT       DEFAULT 0,
                    clicks        BIGINT       DEFAULT 0,
                    cost_micros   BIGINT       DEFAULT 0,
                    conversions   DECIMAL(10,2) DEFAULT 0,
                    ctr           DECIMAL(8,4) DEFAULT 0,
                    average_cpc   BIGINT       DEFAULT 0,
                    status        VARCHAR(30)  DEFAULT NULL,
                    date_range    VARCHAR(30)  DEFAULT NULL,
                    fetched_at    TIMESTAMP    DEFAULT NULL
                )
            ");
            $this->db->executeStatement("CREATE INDEX idx_data_type ON google_ads_snapshots (data_type)");
            $this->db->executeStatement("CREATE INDEX idx_campaign_id ON google_ads_snapshots (campaign_id)");
            $this->db->executeStatement("CREATE INDEX idx_date_range ON google_ads_snapshots (date_range)");
            $output->writeln('Table created.');
        }
    }
}
