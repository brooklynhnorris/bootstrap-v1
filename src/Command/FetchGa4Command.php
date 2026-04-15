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

        $output->writeln('Getting Google access token...');
        $tokenUrl = 'https://oauth2.googleapis.com/token';
        $tokenBody = http_build_query([
            'client_id'     => $clientId,
            'client_secret' => $clientSecret,
            'refresh_token' => $refreshToken,
            'grant_type'    => 'refresh_token',
        ]);
        $tokenResponse = file_get_contents($tokenUrl, false, stream_context_create([
            'http' => [
                'method' => 'POST',
                'header' => 'Content-Type: application/x-www-form-urlencoded',
                'content' => $tokenBody,
                'ignore_errors' => true,
            ],
        ]));

        $tokenHttpStatus = $this->parseHttpStatus($http_response_header ?? []);
        if ($tokenHttpStatus !== null && $tokenHttpStatus >= 400) {
            $output->writeln("[DEBUG] Token request failed with HTTP {$tokenHttpStatus}");
            $output->writeln("[DEBUG] Token request URL: {$tokenUrl}");
            $output->writeln('[DEBUG] Token request body: ' . $tokenBody);
            $output->writeln('[DEBUG] Token raw response: ' . ($tokenResponse !== false ? $tokenResponse : '(empty/false)'));
        }

        $tokenData = json_decode($tokenResponse, true);
        if (!isset($tokenData['access_token'])) {
            $output->writeln('Failed to get access token: ' . ($tokenData['error_description'] ?? 'Unknown error'));
            $output->writeln('[DEBUG] Token request URL: ' . $tokenUrl);
            $output->writeln('[DEBUG] Token request body: ' . $tokenBody);
            $output->writeln('[DEBUG] Token HTTP status: ' . ($tokenHttpStatus ?? 'unknown'));
            $output->writeln('[DEBUG] Token raw response: ' . ($tokenResponse !== false ? $tokenResponse : '(empty/false)'));
            return Command::FAILURE;
        }

        $accessToken = $tokenData['access_token'];
        $output->writeln('Got access token.');

        $this->ensureSchema();

        $output->writeln('Fetching current 28-day page data with engagement metrics...');
        $currentRows = $this->fetchGA4Pages($accessToken, $propertyId, 28, 0, $output);
        $output->writeln('  Retrieved ' . count($currentRows) . ' current period rows.');

        $output->writeln('Fetching previous 28-day page data (comparison period)...');
        $previousRows = $this->fetchGA4Pages($accessToken, $propertyId, 28, 28, $output);
        $output->writeln('  Retrieved ' . count($previousRows) . ' previous period rows.');

        $output->writeln('Fetching landing page + conversion data...');
        $landingRows = $this->fetchGA4LandingPages($accessToken, $propertyId, 28, $output);
        $output->writeln('  Retrieved ' . count($landingRows) . ' landing page rows.');

        $totalRows = count($currentRows) + count($previousRows) + count($landingRows);

        $this->db->beginTransaction();
        try {
            $this->db->executeStatement('DELETE FROM ga4_snapshots');
            $output->writeln('Cleared old GA4 data.');

            $this->insertSnapshotRows($currentRows, '28d');
            $this->insertSnapshotRows($previousRows, '28d_previous');
            $this->insertSnapshotRows($landingRows, '28d_landing');

            $this->db->commit();
        } catch (\Throwable $e) {
            $this->db->rollBack();
            $output->writeln('Failed to store GA4 snapshot data: ' . $e->getMessage());
            return Command::FAILURE;
        }

        $output->writeln("Done! Total: {$totalRows} GA4 rows saved to database.");

        return Command::SUCCESS;
    }

    /**
     * @param array<string, mixed> $headers
     */
    private function parseHttpStatus(array $headers): ?int
    {
        if (isset($headers[0]) && preg_match('#HTTP/\d\.\d (\d{3})#', $headers[0], $m)) {
            return (int) $m[1];
        }
        return null;
    }

    private function fetchGA4Pages(string $token, string $propertyId, int $days, int $offset, OutputInterface $output): array
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

        $url = "https://analyticsdata.googleapis.com/v1beta/properties/{$propertyId}:runReport";
        $response = file_get_contents(
            $url,
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

        $httpStatus = $this->parseHttpStatus($http_response_header ?? []);
        $data = json_decode($response !== false ? $response : '{}', true);
        $rows = $data['rows'] ?? [];

        if ($httpStatus !== null && $httpStatus >= 400) {
            $output->writeln('[DEBUG] GA4 [pages] request failed with HTTP ' . $httpStatus);
            $output->writeln('[DEBUG] GA4 [pages] request URL: ' . $url);
            $output->writeln('[DEBUG] GA4 [pages] request body: ' . json_encode($requestBody));
            $output->writeln('[DEBUG] GA4 [pages] raw response: ' . ($response !== false ? $response : '(empty/false)'));
        } elseif (count($rows) === 0) {
            $output->writeln('[DEBUG] GA4 [pages] returned 0 rows. Request URL: ' . $url);
            $output->writeln('[DEBUG] GA4 [pages] request body: ' . json_encode($requestBody));
            $output->writeln('[DEBUG] GA4 [pages] HTTP status: ' . ($httpStatus ?? 'unknown'));
            $output->writeln('[DEBUG] GA4 [pages] raw response: ' . ($response !== false ? $response : '(empty/false)'));
        }

        $results = [];
        foreach ($rows as $row) {
            $results[] = [
                'page_path'           => $row['dimensionValues'][0]['value'] ?? '/',
                'sessions'            => (int) ($row['metricValues'][0]['value'] ?? 0),
                'pageviews'           => (int) ($row['metricValues'][1]['value'] ?? 0),
                'bounce_rate'         => round((float) ($row['metricValues'][2]['value'] ?? 0), 4),
                'avg_engagement_time' => round((float) ($row['metricValues'][3]['value'] ?? 0), 1),
                'engaged_sessions'    => (int) ($row['metricValues'][4]['value'] ?? 0),
                'conversions'         => (int) ($row['metricValues'][5]['value'] ?? 0),
            ];
        }

        return $results;
    }

    private function fetchGA4LandingPages(string $token, string $propertyId, int $days, OutputInterface $output): array
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

        $url = "https://analyticsdata.googleapis.com/v1beta/properties/{$propertyId}:runReport";
        $response = file_get_contents(
            $url,
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

        $httpStatus = $this->parseHttpStatus($http_response_header ?? []);
        $data = json_decode($response !== false ? $response : '{}', true);
        $rows = $data['rows'] ?? [];

        if ($httpStatus !== null && $httpStatus >= 400) {
            $output->writeln('[DEBUG] GA4 [landing] request failed with HTTP ' . $httpStatus);
            $output->writeln('[DEBUG] GA4 [landing] request URL: ' . $url);
            $output->writeln('[DEBUG] GA4 [landing] request body: ' . json_encode($requestBody));
            $output->writeln('[DEBUG] GA4 [landing] raw response: ' . ($response !== false ? $response : '(empty/false)'));
        } elseif (count($rows) === 0) {
            $output->writeln('[DEBUG] GA4 [landing] returned 0 rows. Request URL: ' . $url);
            $output->writeln('[DEBUG] GA4 [landing] request body: ' . json_encode($requestBody));
            $output->writeln('[DEBUG] GA4 [landing] HTTP status: ' . ($httpStatus ?? 'unknown'));
            $output->writeln('[DEBUG] GA4 [landing] raw response: ' . ($response !== false ? $response : '(empty/false)'));
        }

        $results = [];
        foreach ($rows as $row) {
            $results[] = [
                'page_path'           => $row['dimensionValues'][0]['value'] ?? '/',
                'pageviews'           => 0,
                'sessions'            => (int) ($row['metricValues'][0]['value'] ?? 0),
                'bounce_rate'         => round((float) ($row['metricValues'][1]['value'] ?? 0), 4),
                'avg_engagement_time' => round((float) ($row['metricValues'][2]['value'] ?? 0), 1),
                'engaged_sessions'    => (int) ($row['metricValues'][3]['value'] ?? 0),
                'conversions'         => (int) ($row['metricValues'][4]['value'] ?? 0),
            ];
        }

        return $results;
    }

    private function insertSnapshotRows(array $rows, string $dateRange): void
    {
        foreach ($rows as $row) {
            $this->db->insert('ga4_snapshots', [
                'page_path'             => $row['page_path'],
                'sessions'              => $row['sessions'],
                'pageviews'             => $row['pageviews'] ?? 0,
                'bounce_rate'           => $row['bounce_rate'],
                'avg_engagement_time'   => $row['avg_engagement_time'],
                'engaged_sessions'      => $row['engaged_sessions'],
                'conversions'           => $row['conversions'],
                'date_range'            => $dateRange,
                'fetched_at'            => date('Y-m-d H:i:s'),
            ]);
        }
    }

    private function ensureSchema(): void
    {
        $cols = $this->db->fetchFirstColumn(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'ga4_snapshots'"
        );
        if (!in_array('avg_engagement_time', $cols, true)) {
            $this->db->executeStatement('ALTER TABLE ga4_snapshots ADD COLUMN avg_engagement_time FLOAT DEFAULT 0');
        }
        if (!in_array('engaged_sessions', $cols, true)) {
            $this->db->executeStatement('ALTER TABLE ga4_snapshots ADD COLUMN engaged_sessions INT DEFAULT 0');
        }
        if (!in_array('date_range', $cols, true)) {
            $this->db->executeStatement("ALTER TABLE ga4_snapshots ADD COLUMN date_range VARCHAR(20) DEFAULT '28d'");
        }
    }
}
