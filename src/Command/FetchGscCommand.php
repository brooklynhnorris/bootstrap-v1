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

        $snapshotId = date('Y-m-d_H-i');
        try {
            $batches = $this->db->fetchFirstColumn(
                "SELECT DISTINCT snapshot_id FROM gsc_snapshots WHERE snapshot_id IS NOT NULL ORDER BY snapshot_id DESC"
            );
            if (count($batches) >= 4) {
                $keepBatches = array_slice($batches, 0, 3);
                $placeholders = implode(',', array_fill(0, count($keepBatches), '?'));
                $this->db->executeStatement(
                    "DELETE FROM gsc_snapshots WHERE snapshot_id IS NOT NULL AND snapshot_id NOT IN ({$placeholders})",
                    $keepBatches
                );
                $output->writeln('Pruned old GSC snapshots (keeping last 4 batches).');
            }
        } catch (\Exception $e) {
            $this->db->executeStatement("DELETE FROM gsc_snapshots");
            $output->writeln('Cleared old GSC data (first run with history tracking).');
        }

        $totalRows = 0;

        $totalRows += $this->storeQueryPageWindow($accessToken, $siteUrl, 28, 1, ['28d'], 25000, $output, $snapshotId);
        $totalRows += $this->storeQueryPageWindow($accessToken, $siteUrl, 56, 29, ['28d_prior'], 25000, $output, $snapshotId);
        $totalRows += $this->storeQueryPageWindow($accessToken, $siteUrl, 30, 1, ['last_30_days'], 25000, $output, $snapshotId);
        $totalRows += $this->storeQueryPageWindow($accessToken, $siteUrl, 60, 31, ['prior_30_days'], 25000, $output, $snapshotId);
        $totalRows += $this->storeQueryPageWindow($accessToken, $siteUrl, 90, 1, ['90d', 'last_90_days'], 25000, $output, $snapshotId);
        $totalRows += $this->storeQueryPageWindow($accessToken, $siteUrl, 180, 91, ['prior_90_days'], 25000, $output, $snapshotId);

        $output->writeln('Fetching page-level aggregate data (28d)...');
        $pageRows = $this->fetchGscWindow($accessToken, $siteUrl, 28, 1, ['page'], 5000, $output, '28d page aggregate');
        $countPages = 0;
        foreach ($pageRows as $row) {
            foreach (['28d', '28d_page'] as $rangeLabel) {
                $this->db->insert('gsc_snapshots', [
                    'query'       => '__PAGE_AGGREGATE__',
                    'page'        => $row['keys'][0] ?? '',
                    'clicks'      => $row['clicks'] ?? 0,
                    'impressions' => $row['impressions'] ?? 0,
                    'ctr'         => round($row['ctr'] ?? 0, 4),
                    'position'    => round($row['position'] ?? 0, 1),
                    'date_range'  => $rangeLabel,
                    'fetched_at'  => date('Y-m-d H:i:s'),
                    'snapshot_id' => $snapshotId,
                ]);
                $countPages++;
                $totalRows++;
            }
        }
        $output->writeln("  Saved {$countPages} page-level rows across 28d aliases.");

        $output->writeln('Fetching branded query data...');
        $brandedRows = $this->fetchGscWindow(
            $accessToken,
            $siteUrl,
            28,
            1,
            ['query', 'page'],
            5000,
            $output,
            '28d branded query+page',
            [[
                'filters' => [[
                    'dimension'  => 'query',
                    'operator'   => 'contains',
                    'expression' => 'double d',
                ]],
            ]]
        );
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
                'snapshot_id' => $snapshotId,
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

    private function storeQueryPageWindow(
        string $token,
        string $siteUrl,
        int $startDaysAgo,
        int $endDaysAgo,
        array $dateRanges,
        int $limit,
        OutputInterface $output,
        string $snapshotId
    ): int {
        $label = implode(', ', $dateRanges) . ' query+page';
        $output->writeln("Fetching {$label} data (up to {$limit} rows)...");
        $rows = $this->fetchGscWindow($token, $siteUrl, $startDaysAgo, $endDaysAgo, ['query', 'page'], $limit, $output, $label);
        $saved = 0;

        foreach ($rows as $row) {
            foreach ($dateRanges as $dateRange) {
                $this->db->insert('gsc_snapshots', [
                    'query'       => $row['keys'][0] ?? '',
                    'page'        => $row['keys'][1] ?? '',
                    'clicks'      => $row['clicks'] ?? 0,
                    'impressions' => $row['impressions'] ?? 0,
                    'ctr'         => round($row['ctr'] ?? 0, 4),
                    'position'    => round($row['position'] ?? 0, 1),
                    'date_range'  => $dateRange,
                    'fetched_at'  => date('Y-m-d H:i:s'),
                    'snapshot_id' => $snapshotId,
                ]);
                $saved++;
            }
        }

        $output->writeln("  Saved {$saved} rows ({$label}).");

        return $saved;
    }

    private function fetchGscWindow(
        string $token,
        string $siteUrl,
        int $startDaysAgo,
        int $endDaysAgo,
        array $dimensions,
        int $limit,
        OutputInterface $output,
        string $label,
        ?array $dimensionFilterGroups = null
    ): array {
        $endDate   = date('Y-m-d', strtotime("-{$endDaysAgo} days"));
        $startDate = date('Y-m-d', strtotime("-{$startDaysAgo} days"));

        $allRows = [];
        $startRow = 0;
        $batchSize = min(25000, max(1, $limit));
        $gscUrl = "https://www.googleapis.com/webmasters/v3/sites/" . urlencode($siteUrl) . "/searchAnalytics/query";

        do {
            $requestBody = [
                'startDate'  => $startDate,
                'endDate'    => $endDate,
                'dimensions' => $dimensions,
                'rowLimit'   => min($batchSize, $limit - $startRow),
                'startRow'   => $startRow,
                'dataState'  => 'final',
            ];

            if ($dimensionFilterGroups !== null) {
                $requestBody['dimensionFilterGroups'] = $dimensionFilterGroups;
            }

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

    private function ensureSchema(): void
    {
        $cols = $this->db->fetchFirstColumn(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'gsc_snapshots'"
        );
        if (!in_array('date_range', $cols, true)) {
            $this->db->executeStatement("ALTER TABLE gsc_snapshots ADD COLUMN date_range VARCHAR(50) DEFAULT '28d'");
        }
        if (!in_array('snapshot_id', $cols, true)) {
            $this->db->executeStatement("ALTER TABLE gsc_snapshots ADD COLUMN snapshot_id VARCHAR(20) DEFAULT NULL");
        }

        $this->db->executeStatement("CREATE INDEX IF NOT EXISTS idx_gsc_snapshot_id ON gsc_snapshots (snapshot_id)");
        $this->db->executeStatement("CREATE INDEX IF NOT EXISTS idx_gsc_date_query_page ON gsc_snapshots (date_range, query, page)");
        $this->db->executeStatement("CREATE INDEX IF NOT EXISTS idx_gsc_date_page ON gsc_snapshots (date_range, page)");
    }
}
