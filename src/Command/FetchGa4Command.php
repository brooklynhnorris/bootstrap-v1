<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:fetch-ga4', description: 'Fetch expanded GA4 data with engagement metrics')]
class FetchGa4Command extends Command
{
    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $clientId     = $_ENV['GOOGLE_CLIENT_ID'] ?? '';
        $clientSecret = $_ENV['GOOGLE_CLIENT_SECRET'] ?? '';
        $refreshToken = $_ENV['GOOGLE_REFRESH_TOKEN'] ?? '';
        $propertyId   = $_ENV['GA4_PROPERTY_ID'] ?? '';

        if (!$clientId || !$clientSecret || !$refreshToken || !$propertyId) {
            $output->writeln('Missing Google OAuth credentials or GA4_PROPERTY_ID.');
            return Command::FAILURE;
        }

        // Get access token
        $output->writeln('Getting Google access token...');
        $tokenResponse = file_get_contents('https://oauth2.googleapis.com/token', false, stream_context_create([
            'http' => [
                'method' => 'POST',
                'header' => 'Content-Type: application/x-www-form-urlencoded',
                'content' => http_build_query([
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

        // Ensure expanded table schema
        $this->ensureSchema();

        // Clear old data
        $this->db->executeStatement("DELETE FROM ga4_snapshots");
        $output->writeln('Cleared old GA4 data.');

        $totalRows = 0;

        // ── Fetch 1: Current 28-day page metrics with engagement ──
        $output->writeln('Fetching current 28-day page data with engagement metrics...');
        $currentRows = $this->fetchGA4Pages($accessToken, $propertyId, 28, 0);
        foreach ($currentRows as $row) {
            $this->db->insert('ga4_snapshots', [
                'page_path'             => $row['page_path'],
                'sessions'              => $row['sessions'],
                'pageviews'             => $row['pageviews'],
                'bounce_rate'           => $row['bounce_rate'],
                'avg_engagement_time'   => $row['avg_engagement_time'],
                'engaged_sessions'      => $row['engaged_sessions'],
                'conversions'           => $row['conversions'],
                'date_range'            => '28d',
                'fetched_at'            => date('Y-m-d H:i:s'),
            ]);
            $totalRows++;
        }
        $output->writeln("  Saved " . count($currentRows) . " current period rows.");

        // ── Fetch 2: Previous 28-day page metrics (for WoW/MoM comparison) ──
        $output->writeln('Fetching previous 28-day page data (comparison period)...');
        $previousRows = $this->fetchGA4Pages($accessToken, $propertyId, 28, 28);
        $countPrev = 0;
        foreach ($previousRows as $row) {
            $this->db->insert('ga4_snapshots', [
                'page_path'             => $row['page_path'],
                'sessions'              => $row['sessions'],
                'pageviews'             => $row['pageviews'],
                'bounce_rate'           => $row['bounce_rate'],
                'avg_engagement_time'   => $row['avg_engagement_time'],
                'engaged_sessions'      => $row['engaged_sessions'],
                'conversions'           => $row['conversions'],
                'date_range'            => '28d_previous',
                'fetched_at'            => date('Y-m-d H:i:s'),
            ]);
            $countPrev++;
            $totalRows++;
        }
        $output->writeln("  Saved {$countPrev} previous period rows.");

        // ── Fetch 3: Landing page data with conversions ──
        $output->writeln('Fetching landing page + conversion data...');
        $landingRows = $this->fetchGA4LandingPages($accessToken, $propertyId, 28);
        $countLanding = 0;
        foreach ($landingRows as $row) {
            $this->db->insert('ga4_snapshots', [
                'page_path'             => $row['page_path'],
                'sessions'              => $row['sessions'],
                'pageviews'             => 0,
                'bounce_rate'           => $row['bounce_rate'],
                'avg_engagement_time'   => $row['avg_engagement_time'],
                'engaged_sessions'      => $row['engaged_sessions'],
                'conversions'           => $row['conversions'],
                'date_range'            => '28d_landing',
                'fetched_at'            => date('Y-m-d H:i:s'),
            ]);
            $countLanding++;
            $totalRows++;
        }
        $output->writeln("  Saved {$countLanding} landing page rows.");

        $output->writeln("Done! Total: {$totalRows} GA4 rows saved to database.");

        return Command::SUCCESS;
    }

    private function fetchGA4Pages(string $token, string $propertyId, int $days, int $offset): array
    {
        $endDate   = date('Y-m-d', strtotime('-' . ($offset + 1) . ' days'));
        $startDate = date('Y-m-d', strtotime('-' . ($offset + $days) . ' days'));

        $requestBody = [
            'dateRanges' => [['startDate' => $startDate, 'endDate' => $endDate]],
            'dimensions' => [['name' => 'pagePath']],
            'metrics'    => [
                ['name' => 'sessions'],
                ['name' => 'screenPageViews'],
                ['name' => 'bounceRate'],
                ['name' => 'averageSessionDuration'],
                ['name' => 'engagedSessions'],
                ['name' => 'conversions'],
            ],
            'orderBys' => [['metric' => ['metricName' => 'sessions'], 'desc' => true]],
            'limit' => 500,
        ];

        $response = file_get_contents(
            "https://analyticsdata.googleapis.com/v1beta/properties/{$propertyId}:runReport",
            false,
            stream_context_create([
                'http' => [
                    'method'        => 'POST',
                    'header'        => "Content-Type: application/json\r\nAuthorization: Bearer {$token}",
                    'content'       => json_encode($requestBody),
                    'ignore_errors' => true,
                ],
            ])
        );

        $data = json_decode($response, true);
        $results = [];

        foreach ($data['rows'] ?? [] as $row) {
            $results[] = [
                'page_path'           => $row['dimensionValues'][0]['value'] ?? '/',
                'sessions'            => intval($row['metricValues'][0]['value'] ?? 0),
                'pageviews'           => intval($row['metricValues'][1]['value'] ?? 0),
                'bounce_rate'         => round(floatval($row['metricValues'][2]['value'] ?? 0), 4),
                'avg_engagement_time' => round(floatval($row['metricValues'][3]['value'] ?? 0), 1),
                'engaged_sessions'    => intval($row['metricValues'][4]['value'] ?? 0),
                'conversions'         => intval($row['metricValues'][5]['value'] ?? 0),
            ];
        }

        return $results;
    }

    private function fetchGA4LandingPages(string $token, string $propertyId, int $days): array
    {
        $endDate   = date('Y-m-d', strtotime('-1 day'));
        $startDate = date('Y-m-d', strtotime("-{$days} days"));

        $requestBody = [
            'dateRanges' => [['startDate' => $startDate, 'endDate' => $endDate]],
            'dimensions' => [['name' => 'landingPage']],
            'metrics'    => [
                ['name' => 'sessions'],
                ['name' => 'bounceRate'],
                ['name' => 'averageSessionDuration'],
                ['name' => 'engagedSessions'],
                ['name' => 'conversions'],
            ],
            'orderBys' => [['metric' => ['metricName' => 'sessions'], 'desc' => true]],
            'limit' => 200,
        ];

        $response = file_get_contents(
            "https://analyticsdata.googleapis.com/v1beta/properties/{$propertyId}:runReport",
            false,
            stream_context_create([
                'http' => [
                    'method'        => 'POST',
                    'header'        => "Content-Type: application/json\r\nAuthorization: Bearer {$token}",
                    'content'       => json_encode($requestBody),
                    'ignore_errors' => true,
                ],
            ])
        );

        $data = json_decode($response, true);
        $results = [];

        foreach ($data['rows'] ?? [] as $row) {
            $results[] = [
                'page_path'           => $row['dimensionValues'][0]['value'] ?? '/',
                'sessions'            => intval($row['metricValues'][0]['value'] ?? 0),
                'bounce_rate'         => round(floatval($row['metricValues'][1]['value'] ?? 0), 4),
                'avg_engagement_time' => round(floatval($row['metricValues'][2]['value'] ?? 0), 1),
                'engaged_sessions'    => intval($row['metricValues'][3]['value'] ?? 0),
                'conversions'         => intval($row['metricValues'][4]['value'] ?? 0),
            ];
        }

        return $results;
    }

    private function ensureSchema(): void
    {
        $cols = $this->db->fetchFirstColumn(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'ga4_snapshots'"
        );
        if (!in_array('avg_engagement_time', $cols)) {
            $this->db->executeStatement("ALTER TABLE ga4_snapshots ADD COLUMN avg_engagement_time FLOAT DEFAULT 0");
        }
        if (!in_array('engaged_sessions', $cols)) {
            $this->db->executeStatement("ALTER TABLE ga4_snapshots ADD COLUMN engaged_sessions INT DEFAULT 0");
        }
        if (!in_array('date_range', $cols)) {
            $this->db->executeStatement("ALTER TABLE ga4_snapshots ADD COLUMN date_range VARCHAR(20) DEFAULT '28d'");
        }
    }
}