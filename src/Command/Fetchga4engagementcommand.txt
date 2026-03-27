<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Google\Client as GoogleClient;
use Google\Service\AnalyticsData;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Fetch extended GA4 engagement metrics (bounce rate, session duration, etc.)
 * 
 * Supports rules: USE-004 (bounce/dwell), USE-005, CI-03, CI-05 (period comparison)
 * 
 * This command extends your existing GA4 integration to pull engagement metrics
 * that help identify pages with poor user experience signals.
 * 
 * Prerequisites:
 * - GA4 OAuth already configured (you have this from existing GA4 setup)
 * - Same credentials.json and token.json used by FetchGa4Command
 * 
 * Usage:
 *   php bin/console app:fetch-ga4-engagement              # Last 28 days
 *   php bin/console app:fetch-ga4-engagement --days=90    # Custom period
 *   php bin/console app:fetch-ga4-engagement --compare    # Include period comparison
 */
#[AsCommand(name: 'app:fetch-ga4-engagement', description: 'Fetch GA4 engagement metrics (bounce rate, session duration)')]
class FetchGa4EngagementCommand extends Command
{
    private string $propertyId;
    private string $credentialsPath;
    private string $tokenPath;

    public function __construct(private Connection $db)
    {
        parent::__construct();
        
        // Use same paths as your existing GA4 setup
        $this->propertyId = $_ENV['GA4_PROPERTY_ID'] ?? getenv('GA4_PROPERTY_ID') ?? '350837592';
        $this->credentialsPath = dirname(__DIR__, 2) . '/credentials.json';
        $this->tokenPath = dirname(__DIR__, 2) . '/token.json';
    }

    protected function configure(): void
    {
        $this
            ->addOption('days', null, InputOption::VALUE_OPTIONAL, 'Number of days to fetch', 28)
            ->addOption('compare', null, InputOption::VALUE_NONE, 'Include period-over-period comparison')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Max rows to fetch', 500);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $days = (int) $input->getOption('days');
        $compare = (bool) $input->getOption('compare');
        $limit = (int) $input->getOption('limit');

        $this->ensureSchema();

        // Initialize Google Client
        $client = $this->getAuthenticatedClient($output);
        if (!$client) {
            return Command::FAILURE;
        }

        $analytics = new AnalyticsData($client);
        $property = 'properties/' . $this->propertyId;

        $output->writeln("Fetching GA4 engagement metrics for last {$days} days...");
        $output->writeln("Property: {$this->propertyId}\n");

        // Fetch current period
        $startDate = date('Y-m-d', strtotime("-{$days} days"));
        $endDate = date('Y-m-d', strtotime('-1 day'));

        $currentData = $this->fetchEngagementData($analytics, $property, $startDate, $endDate, $limit, $output);
        
        if ($currentData === null) {
            $output->writeln('<error>Failed to fetch current period data</error>');
            return Command::FAILURE;
        }

        $output->writeln("Fetched " . count($currentData) . " pages for current period");

        // Fetch comparison period if requested
        $priorData = [];
        if ($compare) {
            $priorStartDate = date('Y-m-d', strtotime("-" . ($days * 2) . " days"));
            $priorEndDate = date('Y-m-d', strtotime("-" . ($days + 1) . " days"));
            
            $output->writeln("\nFetching comparison period ({$priorStartDate} to {$priorEndDate})...");
            $priorData = $this->fetchEngagementData($analytics, $property, $priorStartDate, $priorEndDate, $limit, $output);
            
            if ($priorData) {
                $output->writeln("Fetched " . count($priorData) . " pages for comparison period");
            }
        }

        // Store data
        $stored = $this->storeEngagementData($currentData, $priorData, $days, $output);
        $output->writeln("\nStored engagement data for {$stored} pages");

        // Output summary
        $this->outputSummary($output);

        return Command::SUCCESS;
    }

    private function fetchEngagementData(
        AnalyticsData $analytics, 
        string $property, 
        string $startDate, 
        string $endDate,
        int $limit,
        OutputInterface $output
    ): ?array {
        try {
            $response = $analytics->properties->runReport($property, [
                'dateRanges' => [
                    ['startDate' => $startDate, 'endDate' => $endDate]
                ],
                'dimensions' => [
                    ['name' => 'pagePath'],
                ],
                'metrics' => [
                    ['name' => 'sessions'],
                    ['name' => 'screenPageViews'],
                    ['name' => 'bounceRate'],
                    ['name' => 'averageSessionDuration'],
                    ['name' => 'engagementRate'],
                    ['name' => 'engagedSessions'],
                    ['name' => 'userEngagementDuration'],
                    ['name' => 'scrolledUsers'],  // Users who scrolled 90%+
                ],
                'orderBys' => [
                    ['metric' => ['metricName' => 'sessions'], 'desc' => true]
                ],
                'limit' => $limit,
            ]);

            $data = [];
            foreach ($response->getRows() ?? [] as $row) {
                $dims = $row->getDimensionValues();
                $mets = $row->getMetricValues();

                $pagePath = $dims[0]->getValue();
                
                // Normalize path
                $pagePath = '/' . trim(parse_url($pagePath, PHP_URL_PATH) ?? $pagePath, '/');
                if ($pagePath !== '/') $pagePath .= '/';

                $data[$pagePath] = [
                    'sessions' => (int) $mets[0]->getValue(),
                    'pageviews' => (int) $mets[1]->getValue(),
                    'bounce_rate' => (float) $mets[2]->getValue(),
                    'avg_session_duration' => (float) $mets[3]->getValue(),
                    'engagement_rate' => (float) $mets[4]->getValue(),
                    'engaged_sessions' => (int) $mets[5]->getValue(),
                    'user_engagement_duration' => (float) $mets[6]->getValue(),
                    'scrolled_users' => (int) $mets[7]->getValue(),
                ];
            }

            return $data;

        } catch (\Exception $e) {
            $output->writeln('<error>GA4 API Error: ' . $e->getMessage() . '</error>');
            return null;
        }
    }

    private function storeEngagementData(array $currentData, array $priorData, int $days, OutputInterface $output): int
    {
        $snapshotDate = date('Y-m-d');
        $stored = 0;

        foreach ($currentData as $pagePath => $metrics) {
            // Calculate deltas if we have prior data
            $priorMetrics = $priorData[$pagePath] ?? null;
            
            $bounceRateDelta = null;
            $sessionsDelta = null;
            $engagementRateDelta = null;
            
            if ($priorMetrics) {
                $bounceRateDelta = $metrics['bounce_rate'] - $priorMetrics['bounce_rate'];
                $sessionsDelta = $metrics['sessions'] - $priorMetrics['sessions'];
                $engagementRateDelta = $metrics['engagement_rate'] - $priorMetrics['engagement_rate'];
            }

            // Calculate scroll depth percentage (scrolled_users / sessions)
            $scrollDepthPct = $metrics['sessions'] > 0 
                ? ($metrics['scrolled_users'] / $metrics['sessions']) * 100 
                : 0;

            // Delete existing record for this URL + snapshot date
            $this->db->executeStatement(
                'DELETE FROM ga4_engagement WHERE url = ? AND snapshot_date = ?',
                [$pagePath, $snapshotDate]
            );

            $this->db->insert('ga4_engagement', [
                'url' => $pagePath,
                'snapshot_date' => $snapshotDate,
                'period_days' => $days,
                'sessions' => $metrics['sessions'],
                'pageviews' => $metrics['pageviews'],
                'bounce_rate' => $metrics['bounce_rate'],
                'avg_session_duration' => $metrics['avg_session_duration'],
                'engagement_rate' => $metrics['engagement_rate'],
                'engaged_sessions' => $metrics['engaged_sessions'],
                'scroll_depth_pct' => $scrollDepthPct,
                // Period comparison deltas
                'bounce_rate_delta' => $bounceRateDelta,
                'sessions_delta' => $sessionsDelta,
                'engagement_rate_delta' => $engagementRateDelta,
                // Flags for rule triggers
                'high_bounce' => $metrics['bounce_rate'] > 0.7 ? 1 : 0,  // >70% bounce
                'low_engagement' => $metrics['engagement_rate'] < 0.4 ? 1 : 0,  // <40% engagement
                'low_scroll_depth' => $scrollDepthPct < 25 ? 1 : 0,  // <25% scroll to bottom
            ]);

            $stored++;
        }

        return $stored;
    }

    private function outputSummary(OutputInterface $output): void
    {
        $stats = $this->db->fetchAssociative("
            SELECT 
                COUNT(*) as total_pages,
                AVG(bounce_rate) as avg_bounce_rate,
                AVG(engagement_rate) as avg_engagement_rate,
                AVG(avg_session_duration) as avg_duration,
                SUM(CASE WHEN high_bounce = TRUE THEN 1 ELSE 0 END) as high_bounce_pages,
                SUM(CASE WHEN low_engagement = TRUE THEN 1 ELSE 0 END) as low_engagement_pages,
                SUM(CASE WHEN low_scroll_depth = TRUE THEN 1 ELSE 0 END) as low_scroll_pages
            FROM ga4_engagement
            WHERE snapshot_date = ?
        ", [date('Y-m-d')]);

        if ($stats && $stats['total_pages'] > 0) {
            $output->writeln("\n=== GA4 Engagement Summary ===");
            $output->writeln(sprintf("  Total pages: %d", $stats['total_pages']));
            $output->writeln(sprintf("  Avg Bounce Rate: %.1f%%", $stats['avg_bounce_rate'] * 100));
            $output->writeln(sprintf("  Avg Engagement Rate: %.1f%%", $stats['avg_engagement_rate'] * 100));
            $output->writeln(sprintf("  Avg Session Duration: %.1fs", $stats['avg_duration']));
            $output->writeln("\n  Problem Pages:");
            $output->writeln(sprintf("    High bounce (>70%%): %d pages", $stats['high_bounce_pages']));
            $output->writeln(sprintf("    Low engagement (<40%%): %d pages", $stats['low_engagement_pages']));
            $output->writeln(sprintf("    Low scroll depth (<25%%): %d pages", $stats['low_scroll_pages']));
        }
    }

    private function getAuthenticatedClient(OutputInterface $output): ?GoogleClient
    {
        if (!file_exists($this->credentialsPath)) {
            $output->writeln("<error>credentials.json not found at {$this->credentialsPath}</error>");
            return null;
        }

        $client = new GoogleClient();
        $client->setAuthConfig($this->credentialsPath);
        $client->setScopes([
            'https://www.googleapis.com/auth/analytics.readonly',
        ]);
        $client->setAccessType('offline');

        // Load existing token
        if (file_exists($this->tokenPath)) {
            $token = json_decode(file_get_contents($this->tokenPath), true);
            $client->setAccessToken($token);

            // Refresh if expired
            if ($client->isAccessTokenExpired()) {
                if ($client->getRefreshToken()) {
                    $newToken = $client->fetchAccessTokenWithRefreshToken($client->getRefreshToken());
                    if (isset($newToken['access_token'])) {
                        file_put_contents($this->tokenPath, json_encode($newToken));
                        $output->writeln("Token refreshed successfully");
                    }
                } else {
                    $output->writeln("<error>Token expired and no refresh token available</error>");
                    return null;
                }
            }
        } else {
            $output->writeln("<error>token.json not found. Run OAuth flow first.</error>");
            return null;
        }

        return $client;
    }

    private function ensureSchema(): void
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        );

        if (!in_array('ga4_engagement', $tables)) {
            $this->db->executeStatement("
                CREATE TABLE ga4_engagement (
                    id                      SERIAL PRIMARY KEY,
                    url                     TEXT NOT NULL,
                    snapshot_date           DATE NOT NULL,
                    period_days             INT DEFAULT 28,
                    -- Core metrics
                    sessions                INT DEFAULT 0,
                    pageviews               INT DEFAULT 0,
                    bounce_rate             DECIMAL(5,4) DEFAULT NULL,
                    avg_session_duration    DECIMAL(10,2) DEFAULT NULL,
                    engagement_rate         DECIMAL(5,4) DEFAULT NULL,
                    engaged_sessions        INT DEFAULT 0,
                    scroll_depth_pct        DECIMAL(5,2) DEFAULT NULL,
                    -- Period comparison deltas
                    bounce_rate_delta       DECIMAL(5,4) DEFAULT NULL,
                    sessions_delta          INT DEFAULT NULL,
                    engagement_rate_delta   DECIMAL(5,4) DEFAULT NULL,
                    -- Rule trigger flags
                    high_bounce             BOOLEAN DEFAULT FALSE,
                    low_engagement          BOOLEAN DEFAULT FALSE,
                    low_scroll_depth        BOOLEAN DEFAULT FALSE,
                    UNIQUE(url, snapshot_date)
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_ga4eng_url ON ga4_engagement (url)');
            $this->db->executeStatement('CREATE INDEX idx_ga4eng_date ON ga4_engagement (snapshot_date)');
            $this->db->executeStatement('CREATE INDEX idx_ga4eng_bounce ON ga4_engagement (high_bounce)');
        }
    }
}