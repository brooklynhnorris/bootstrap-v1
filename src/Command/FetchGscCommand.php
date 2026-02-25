<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:fetch-gsc', description: 'Fetch expanded GSC data for full audit')]
class FetchGscCommand extends Command
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
        $siteUrl      = $_ENV['GSC_SITE_URL'] ?? 'https://doubledtrailers.com';

        if (!$clientId || !$clientSecret || !$refreshToken) {
            $output->writeln('Missing Google OAuth credentials. Set GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN.');
            return Command::FAILURE;
        }

        // Get access token
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

        // Ensure expanded table schema
        $this->ensureSchema();

        // Clear old data
        $this->db->executeStatement("DELETE FROM gsc_snapshots");
        $output->writeln('Cleared old GSC data.');

        $totalRows = 0;

        // ── Fetch 1: Full query+page data (28 days) - up to 25K rows ──
        $output->writeln('Fetching 28-day query+page data (up to 25K rows)...');
        $rows28d = $this->fetchGscData($accessToken, $siteUrl, 28, 'query', 25000, $output, '28d query+page');
        foreach ($rows28d as $row) {
            $this->db->insert('gsc_snapshots', [
                'query'       => $row['keys'][0] ?? '',
                'page'        => $row['keys'][1] ?? '',
                'clicks'      => $row['clicks'] ?? 0,
                'impressions' => $row['impressions'] ?? 0,
                'ctr'         => round($row['ctr'] ?? 0, 4),
                'position'    => round($row['position'] ?? 0, 1),
                'date_range'  => '28d',
                'fetched_at'  => date('Y-m-d H:i:s'),
            ]);
            $totalRows++;
        }
        $output->writeln("  Saved {$totalRows} rows (28d query+page).");

        // ── Fetch 2: 90-day query+page data for trend comparison ──
        $output->writeln('Fetching 90-day query+page data (up to 25K rows)...');
        $rows90d = $this->fetchGscData($accessToken, $siteUrl, 90, 'query', 25000, $output, '90d query+page');
        $count90 = 0;
        foreach ($rows90d as $row) {
            $this->db->insert('gsc_snapshots', [
                'query'       => $row['keys'][0] ?? '',
                'page'        => $row['keys'][1] ?? '',
                'clicks'      => $row['clicks'] ?? 0,
                'impressions' => $row['impressions'] ?? 0,
                'ctr'         => round($row['ctr'] ?? 0, 4),
                'position'    => round($row['position'] ?? 0, 1),
                'date_range'  => '90d',
                'fetched_at'  => date('Y-m-d H:i:s'),
            ]);
            $count90++;
            $totalRows++;
        }
        $output->writeln("  Saved {$count90} rows (90d query+page).");

        // ── Fetch 3: Page-level metrics (no query dimension) for page performance ──
        $output->writeln('Fetching page-level aggregate data (28d)...');
        $pageRows = $this->fetchGscPages($accessToken, $siteUrl, 28, 5000, $output);
        $countPages = 0;
        foreach ($pageRows as $row) {
            $this->db->insert('gsc_snapshots', [
                'query'       => '__PAGE_AGGREGATE__',
                'page'        => $row['keys'][0] ?? '',
                'clicks'      => $row['clicks'] ?? 0,
                'impressions' => $row['impressions'] ?? 0,
                'ctr'         => round($row['ctr'] ?? 0, 4),
                'position'    => round($row['position'] ?? 0, 1),
                'date_range'  => '28d_page',
                'fetched_at'  => date('Y-m-d H:i:s'),
            ]);
            $countPages++;
            $totalRows++;
        }
        $output->writeln("  Saved {$countPages} page-level rows.");

        // ── Fetch 4: Branded queries (containing "double d") ──
        $output->writeln('Fetching branded query data...');
        $brandedRows = $this->fetchGscBranded($accessToken, $siteUrl, 28, $output);
        $countBranded = 0;
        foreach ($brandedRows as $row) {
            $this->db->insert('gsc_snapshots', [
                'query'       => $row['keys'][0] ?? '',
                'page'        => $row['keys'][1] ?? '',
                'clicks'      => $row['clicks'] ?? 0,
                'impressions' => $row['impressions'] ?? 0,
                'ctr'         => round($row['ctr'] ?? 0, 4),
                'position'    => round($row['position'] ?? 0, 1),
                'date_range'  => '28d_branded',
                'fetched_at'  => date('Y-m-d H:i:s'),
            ]);
            $countBranded++;
            $totalRows++;
        }
        $output->writeln("  Saved {$countBranded} branded query rows.");

        $output->writeln("Done! Total: {$totalRows} GSC rows saved to database.");

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

    private function fetchGscData(string $token, string $siteUrl, int $days, string $type, int $limit, OutputInterface $output, string $label): array
    {
        $endDate   = date('Y-m-d', strtotime('-1 day'));
        $startDate = date('Y-m-d', strtotime("-{$days} days"));

        $allRows = [];
        $startRow = 0;
        $batchSize = 25000; // GSC API max per request
        $gscUrl = "https://www.googleapis.com/webmasters/v3/sites/" . urlencode($siteUrl) . "/searchAnalytics/query";

        do {
            $requestBody = [
                'startDate'          => $startDate,
                'endDate'            => $endDate,
                'dimensions'         => ['query', 'page'],
                'rowLimit'           => min($batchSize, $limit - $startRow),
                'startRow'           => $startRow,
                'dataState'          => 'final',
            ];

            $response = file_get_contents(
                $gscUrl,
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
                $output->writeln("[DEBUG] GSC [{$label}] request failed with HTTP {$httpStatus}");
                $output->writeln("[DEBUG] GSC [{$label}] request URL: {$gscUrl}");
                $output->writeln('[DEBUG] GSC [' . $label . '] request body: ' . json_encode($requestBody));
                $output->writeln('[DEBUG] GSC [' . $label . '] raw response: ' . ($response !== false ? $response : '(empty/false)'));
            } elseif (count($rows) === 0 && $startRow === 0) {
                $output->writeln("[DEBUG] GSC [{$label}] returned 0 rows. Request URL: {$gscUrl}");
                $output->writeln('[DEBUG] GSC [' . $label . '] request body: ' . json_encode($requestBody));
                $output->writeln('[DEBUG] GSC [' . $label . '] HTTP status: ' . ($httpStatus ?? 'unknown'));
                $output->writeln('[DEBUG] GSC [' . $label . '] raw response: ' . ($response !== false ? $response : '(empty/false)'));
            }

            $allRows = array_merge($allRows, $rows);
            $startRow += count($rows);

        } while (count($rows) === $batchSize && $startRow < $limit);

        return $allRows;
    }

    private function fetchGscPages(string $token, string $siteUrl, int $days, int $limit, OutputInterface $output): array
    {
        $endDate   = date('Y-m-d', strtotime('-1 day'));
        $startDate = date('Y-m-d', strtotime("-{$days} days"));
        $url = "https://www.googleapis.com/webmasters/v3/sites/" . urlencode($siteUrl) . "/searchAnalytics/query";
        $requestBody = [
            'startDate'  => $startDate,
            'endDate'    => $endDate,
            'dimensions' => ['page'],
            'rowLimit'   => $limit,
            'dataState'  => 'final',
        ];

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
            $output->writeln('[DEBUG] GSC [page-level] request failed with HTTP ' . $httpStatus);
            $output->writeln('[DEBUG] GSC [page-level] request URL: ' . $url);
            $output->writeln('[DEBUG] GSC [page-level] request body: ' . json_encode($requestBody));
            $output->writeln('[DEBUG] GSC [page-level] raw response: ' . ($response !== false ? $response : '(empty/false)'));
        } elseif (count($rows) === 0) {
            $output->writeln('[DEBUG] GSC [page-level] returned 0 rows. Request URL: ' . $url);
            $output->writeln('[DEBUG] GSC [page-level] request body: ' . json_encode($requestBody));
            $output->writeln('[DEBUG] GSC [page-level] HTTP status: ' . ($httpStatus ?? 'unknown'));
            $output->writeln('[DEBUG] GSC [page-level] raw response: ' . ($response !== false ? $response : '(empty/false)'));
        }

        return $rows;
    }

    private function fetchGscBranded(string $token, string $siteUrl, int $days, OutputInterface $output): array
    {
        $endDate   = date('Y-m-d', strtotime('-1 day'));
        $startDate = date('Y-m-d', strtotime("-{$days} days"));
        $url = "https://www.googleapis.com/webmasters/v3/sites/" . urlencode($siteUrl) . "/searchAnalytics/query";
        $requestBody = [
            'startDate'            => $startDate,
            'endDate'              => $endDate,
            'dimensions'           => ['query', 'page'],
            'dimensionFilterGroups' => [[
                'filters' => [[
                    'dimension'  => 'query',
                    'operator'   => 'contains',
                    'expression' => 'double d',
                ]],
            ]],
            'rowLimit'   => 5000,
            'dataState'  => 'final',
        ];

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
            $output->writeln('[DEBUG] GSC [branded] request failed with HTTP ' . $httpStatus);
            $output->writeln('[DEBUG] GSC [branded] request URL: ' . $url);
            $output->writeln('[DEBUG] GSC [branded] request body: ' . json_encode($requestBody));
            $output->writeln('[DEBUG] GSC [branded] raw response: ' . ($response !== false ? $response : '(empty/false)'));
        } elseif (count($rows) === 0) {
            $output->writeln('[DEBUG] GSC [branded] returned 0 rows. Request URL: ' . $url);
            $output->writeln('[DEBUG] GSC [branded] request body: ' . json_encode($requestBody));
            $output->writeln('[DEBUG] GSC [branded] HTTP status: ' . ($httpStatus ?? 'unknown'));
            $output->writeln('[DEBUG] GSC [branded] raw response: ' . ($response !== false ? $response : '(empty/false)'));
        }

        return $rows;
    }

    private function ensureSchema(): void
    {
        // Add date_range column if missing
        $cols = $this->db->fetchFirstColumn(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'gsc_snapshots'"
        );
        if (!in_array('date_range', $cols)) {
            $this->db->executeStatement("ALTER TABLE gsc_snapshots ADD COLUMN date_range VARCHAR(20) DEFAULT '28d'");
        }
    }
}
