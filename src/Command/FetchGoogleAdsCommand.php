<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:fetch-google-ads', description: 'Fetch Google Ads campaign, keyword, and spend data')]
class FetchGoogleAdsCommand extends Command
{
    public function __construct(private Connection $db)
    {
        parent::__construct();
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
            $this->db->insert('google_ads_snapshots', [
                'data_type'    => 'campaign',
                'campaign_id'  => $row['campaign']['id'] ?? '',
                'campaign_name'=> $row['campaign']['name'] ?? '',
                'ad_group_id'  => null,
                'ad_group_name'=> null,
                'keyword'      => null,
                'match_type'   => null,
                'impressions'  => $row['metrics']['impressions'] ?? 0,
                'clicks'       => $row['metrics']['clicks'] ?? 0,
                'cost_micros'  => $row['metrics']['costMicros'] ?? 0,
                'conversions'  => round($row['metrics']['conversions'] ?? 0, 2),
                'ctr'          => round(($row['metrics']['ctr'] ?? 0), 4),
                'average_cpc'  => $row['metrics']['averageCpc'] ?? 0,
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
            $this->db->insert('google_ads_snapshots', [
                'data_type'    => 'keyword',
                'campaign_id'  => $row['campaign']['id'] ?? '',
                'campaign_name'=> $row['campaign']['name'] ?? '',
                'ad_group_id'  => $row['adGroup']['id'] ?? '',
                'ad_group_name'=> $row['adGroup']['name'] ?? '',
                'keyword'      => $row['adGroupCriterion']['keyword']['text'] ?? '',
                'match_type'   => $row['adGroupCriterion']['keyword']['matchType'] ?? '',
                'impressions'  => $row['metrics']['impressions'] ?? 0,
                'clicks'       => $row['metrics']['clicks'] ?? 0,
                'cost_micros'  => $row['metrics']['costMicros'] ?? 0,
                'conversions'  => round($row['metrics']['conversions'] ?? 0, 2),
                'ctr'          => round(($row['metrics']['ctr'] ?? 0), 4),
                'average_cpc'  => $row['metrics']['averageCpc'] ?? 0,
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
            $this->db->insert('google_ads_snapshots', [
                'data_type'    => 'search_term',
                'campaign_id'  => $row['campaign']['id'] ?? '',
                'campaign_name'=> $row['campaign']['name'] ?? '',
                'ad_group_id'  => $row['adGroup']['id'] ?? '',
                'ad_group_name'=> $row['adGroup']['name'] ?? '',
                'keyword'      => $row['searchTermView']['searchTerm'] ?? '',
                'match_type'   => null,
                'impressions'  => $row['metrics']['impressions'] ?? 0,
                'clicks'       => $row['metrics']['clicks'] ?? 0,
                'cost_micros'  => $row['metrics']['costMicros'] ?? 0,
                'conversions'  => round($row['metrics']['conversions'] ?? 0, 2),
                'ctr'          => round(($row['metrics']['ctr'] ?? 0), 4),
                'average_cpc'  => $row['metrics']['averageCpc'] ?? 0,
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
            $this->db->insert('google_ads_snapshots', [
                'data_type'    => 'daily_spend',
                'campaign_id'  => null,
                'campaign_name'=> null,
                'ad_group_id'  => null,
                'ad_group_name'=> null,
                'keyword'      => null,
                'match_type'   => null,
                'impressions'  => $row['metrics']['impressions'] ?? 0,
                'clicks'       => $row['metrics']['clicks'] ?? 0,
                'cost_micros'  => $row['metrics']['costMicros'] ?? 0,
                'conversions'  => round($row['metrics']['conversions'] ?? 0, 2),
                'ctr'          => round(($row['metrics']['ctr'] ?? 0), 4),
                'average_cpc'  => $row['metrics']['averageCpc'] ?? 0,
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

        return $this->runGaqlQuery($token, $devToken, $customerId, $query);
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

        return $this->runGaqlQuery($token, $devToken, $customerId, $query);
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

        return $this->runGaqlQuery($token, $devToken, $customerId, $query);
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

        return $this->runGaqlQuery($token, $devToken, $customerId, $query);
    }

    // ── Core GAQL request ──
    private function runGaqlQuery(string $token, string $devToken, string $customerId, string $query): array
    {
        $url = "https://googleads.googleapis.com/v17/customers/{$customerId}/googleAds:searchStream";

        $response = file_get_contents($url, false, stream_context_create([
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

        if ($response === false) {
            return [];
        }

        // searchStream returns newline-delimited JSON objects (one per batch)
        $results = [];
        foreach (explode("\n", trim($response)) as $line) {
            $line = trim($line);
            if (!$line || $line === '[' || $line === ']') continue;
            // Strip leading comma if present
            $line = ltrim($line, ',');
            $decoded = json_decode($line, true);
            if (isset($decoded['results'])) {
                $results = array_merge($results, $decoded['results']);
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
