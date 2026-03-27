<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Fetch Core Web Vitals data from PageSpeed Insights API
 * 
 * Supports rules: CWV-R3 (CLS), CWV-R4 (INP), CWV-R6 (page weight), CWV-R8 (TTFB), TECH-R7
 * 
 * Setup:
 * 1. Go to https://console.cloud.google.com/apis/credentials
 * 2. Create an API key (no OAuth needed for PSI)
 * 3. Enable "PageSpeed Insights API" at https://console.cloud.google.com/apis/library/pagespeedonline.googleapis.com
 * 4. Add the API key to your .env file: PSI_API_KEY=your_key_here
 * 
 * Usage:
 *   php bin/console app:fetch-cwv                    # Fetch CWV for all core pages
 *   php bin/console app:fetch-cwv --url=/gooseneck-horse-trailers/  # Single URL
 *   php bin/console app:fetch-cwv --limit=10        # Limit number of pages
 */
#[AsCommand(name: 'app:fetch-cwv', description: 'Fetch Core Web Vitals from PageSpeed Insights API')]
class FetchCoreWebVitalsCommand extends Command
{
    private string $baseUrl = 'https://doubledtrailers.com';
    private string $psiEndpoint = 'https://www.googleapis.com/pagespeedonline/v5/runPagespeed';
    
    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('url', null, InputOption::VALUE_OPTIONAL, 'Fetch CWV for a single URL path')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Max URLs to process', 50)
            ->addOption('strategy', null, InputOption::VALUE_OPTIONAL, 'mobile or desktop', 'mobile');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $apiKey = $_ENV['PSI_API_KEY'] ?? getenv('PSI_API_KEY');
        if (empty($apiKey)) {
            $output->writeln('<error>PSI_API_KEY not set in environment. See command help for setup instructions.</error>');
            return Command::FAILURE;
        }

        $this->ensureSchema();

        $singleUrl = $input->getOption('url');
        $limit = (int) $input->getOption('limit');
        $strategy = $input->getOption('strategy');

        // Get URLs to process
        if ($singleUrl) {
            $urls = [$singleUrl];
        } else {
            // Fetch core pages from page_crawl_snapshots
            $urls = $this->db->fetchFirstColumn(
                "SELECT url FROM page_crawl_snapshots WHERE page_type = 'core' ORDER BY url LIMIT ?",
                [$limit]
            );
        }

        if (empty($urls)) {
            $output->writeln('No URLs found. Run app:crawl-pages first.');
            return Command::FAILURE;
        }

        $output->writeln("Fetching Core Web Vitals for " . count($urls) . " URLs (strategy: {$strategy})");
        $output->writeln("API quota: 25,000 queries/day free tier\n");

        $processed = 0;
        $failed = 0;

        foreach ($urls as $path) {
            $fullUrl = $this->buildFullUrl($path);
            $output->write("  [{$processed}/" . count($urls) . "] {$path} ... ");

            $result = $this->fetchPageSpeedData($fullUrl, $apiKey, $strategy);

            if ($result === null) {
                $output->writeln('<error>FAILED</error>');
                $failed++;
            } else {
                // Store in database
                $this->storeCwvData($path, $result, $strategy);
                
                $output->writeln(sprintf(
                    '<info>OK</info> LCP: %dms | CLS: %.3f | TBT: %dms | TTFB: %dms',
                    $result['lcp_ms'] ?? 0,
                    $result['cls_score'] ?? 0,
                    $result['tbt_ms'] ?? 0,
                    $result['ttfb_ms'] ?? 0
                ));
                $processed++;
            }

            // Rate limit: PSI allows ~1 request/second for free tier
            sleep(2);
        }

        $output->writeln("\nDone. Processed: {$processed} | Failed: {$failed}");
        
        // Summary stats
        $this->outputSummary($output);

        return Command::SUCCESS;
    }

    private function fetchPageSpeedData(string $url, string $apiKey, string $strategy): ?array
    {
        $params = http_build_query([
            'url' => $url,
            'key' => $apiKey,
            'strategy' => $strategy,
            'category' => 'performance',
        ]);

        $context = stream_context_create([
            'http' => [
                'method' => 'GET',
                'timeout' => 60, // PSI can be slow
                'ignore_errors' => true,
            ],
        ]);

        $response = @file_get_contents("{$this->psiEndpoint}?{$params}", false, $context);
        if ($response === false) {
            return null;
        }

        $data = json_decode($response, true);
        if (!$data || isset($data['error'])) {
            return null;
        }

        // Extract lab data from Lighthouse
        $lighthouse = $data['lighthouseResult'] ?? [];
        $audits = $lighthouse['audits'] ?? [];

        // Extract field data (CrUX) if available
        $fieldData = $data['loadingExperience'] ?? [];
        $metrics = $fieldData['metrics'] ?? [];

        return [
            // Lab data (always available)
            'lcp_ms' => (int) (($audits['largest-contentful-paint']['numericValue'] ?? 0)),
            'cls_score' => (float) ($audits['cumulative-layout-shift']['numericValue'] ?? 0),
            'tbt_ms' => (int) (($audits['total-blocking-time']['numericValue'] ?? 0)), // TBT is proxy for INP
            'fcp_ms' => (int) (($audits['first-contentful-paint']['numericValue'] ?? 0)),
            'ttfb_ms' => (int) (($audits['server-response-time']['numericValue'] ?? 0)),
            'speed_index' => (int) (($audits['speed-index']['numericValue'] ?? 0)),
            'performance_score' => (int) (($lighthouse['categories']['performance']['score'] ?? 0) * 100),
            
            // Page weight data
            'total_bytes' => (int) ($audits['total-byte-weight']['numericValue'] ?? 0),
            'image_bytes' => $this->extractResourceBytes($audits, 'image'),
            'script_bytes' => $this->extractResourceBytes($audits, 'script'),
            'stylesheet_bytes' => $this->extractResourceBytes($audits, 'stylesheet'),
            
            // Field data (CrUX - may be null for low-traffic pages)
            'field_lcp_ms' => $this->extractFieldMetric($metrics, 'LARGEST_CONTENTFUL_PAINT_MS'),
            'field_cls' => $this->extractFieldMetric($metrics, 'CUMULATIVE_LAYOUT_SHIFT_SCORE'),
            'field_inp_ms' => $this->extractFieldMetric($metrics, 'INTERACTION_TO_NEXT_PAINT'),
            'field_ttfb_ms' => $this->extractFieldMetric($metrics, 'EXPERIMENTAL_TIME_TO_FIRST_BYTE'),
            
            // Pass/fail assessment
            'lcp_pass' => ($audits['largest-contentful-paint']['numericValue'] ?? 9999) <= 2500,
            'cls_pass' => ($audits['cumulative-layout-shift']['numericValue'] ?? 1) <= 0.1,
            'tbt_pass' => ($audits['total-blocking-time']['numericValue'] ?? 9999) <= 200,
        ];
    }

    private function extractResourceBytes(array $audits, string $resourceType): int
    {
        $items = $audits['resource-summary']['details']['items'] ?? [];
        foreach ($items as $item) {
            if (($item['resourceType'] ?? '') === $resourceType) {
                return (int) ($item['transferSize'] ?? 0);
            }
        }
        return 0;
    }

    private function extractFieldMetric(array $metrics, string $key): ?int
    {
        if (!isset($metrics[$key]['percentile'])) {
            return null;
        }
        return (int) $metrics[$key]['percentile'];
    }

    private function storeCwvData(string $url, array $data, string $strategy): void
    {
        // Delete existing record for this URL + strategy
        $this->db->executeStatement(
            'DELETE FROM core_web_vitals WHERE url = ? AND strategy = ?',
            [$url, $strategy]
        );

        $this->db->insert('core_web_vitals', [
            'url' => $url,
            'strategy' => $strategy,
            'lcp_ms' => $data['lcp_ms'],
            'cls_score' => $data['cls_score'],
            'tbt_ms' => $data['tbt_ms'],
            'fcp_ms' => $data['fcp_ms'],
            'ttfb_ms' => $data['ttfb_ms'],
            'speed_index' => $data['speed_index'],
            'performance_score' => $data['performance_score'],
            'total_bytes' => $data['total_bytes'],
            'image_bytes' => $data['image_bytes'],
            'script_bytes' => $data['script_bytes'],
            'stylesheet_bytes' => $data['stylesheet_bytes'],
            'field_lcp_ms' => $data['field_lcp_ms'],
            'field_cls' => $data['field_cls'],
            'field_inp_ms' => $data['field_inp_ms'],
            'field_ttfb_ms' => $data['field_ttfb_ms'],
            'lcp_pass' => $data['lcp_pass'] ? 1 : 0,
            'cls_pass' => $data['cls_pass'] ? 1 : 0,
            'tbt_pass' => $data['tbt_pass'] ? 1 : 0,
            'fetched_at' => date('Y-m-d H:i:s'),
        ]);
    }

    private function outputSummary(OutputInterface $output): void
    {
        $stats = $this->db->fetchAssociative("
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN lcp_pass = TRUE THEN 1 ELSE 0 END) as lcp_passing,
                SUM(CASE WHEN cls_pass = TRUE THEN 1 ELSE 0 END) as cls_passing,
                SUM(CASE WHEN tbt_pass = TRUE THEN 1 ELSE 0 END) as tbt_passing,
                AVG(performance_score) as avg_score,
                AVG(lcp_ms) as avg_lcp,
                AVG(cls_score) as avg_cls,
                AVG(ttfb_ms) as avg_ttfb
            FROM core_web_vitals
            WHERE strategy = 'mobile'
        ");

        if ($stats && $stats['total'] > 0) {
            $output->writeln("\n=== Core Web Vitals Summary (Mobile) ===");
            $output->writeln(sprintf("  Total pages: %d", $stats['total']));
            $output->writeln(sprintf("  Avg Performance Score: %.0f/100", $stats['avg_score']));
            $output->writeln(sprintf("  LCP passing (≤2.5s): %d/%d", $stats['lcp_passing'], $stats['total']));
            $output->writeln(sprintf("  CLS passing (≤0.1): %d/%d", $stats['cls_passing'], $stats['total']));
            $output->writeln(sprintf("  TBT passing (≤200ms): %d/%d", $stats['tbt_passing'], $stats['total']));
            $output->writeln(sprintf("  Avg LCP: %dms | Avg CLS: %.3f | Avg TTFB: %dms", 
                $stats['avg_lcp'], $stats['avg_cls'], $stats['avg_ttfb']));
        }
    }

    private function buildFullUrl(string $path): string
    {
        if (str_starts_with($path, 'http')) return $path;
        return rtrim($this->baseUrl, '/') . '/' . ltrim($path, '/');
    }

    private function ensureSchema(): void
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        );

        if (!in_array('core_web_vitals', $tables)) {
            $this->db->executeStatement("
                CREATE TABLE core_web_vitals (
                    id                  SERIAL PRIMARY KEY,
                    url                 TEXT NOT NULL,
                    strategy            VARCHAR(10) NOT NULL DEFAULT 'mobile',
                    -- Lab metrics (Lighthouse)
                    lcp_ms              INT DEFAULT NULL,
                    cls_score           DECIMAL(6,4) DEFAULT NULL,
                    tbt_ms              INT DEFAULT NULL,
                    fcp_ms              INT DEFAULT NULL,
                    ttfb_ms             INT DEFAULT NULL,
                    speed_index         INT DEFAULT NULL,
                    performance_score   INT DEFAULT NULL,
                    -- Resource breakdown
                    total_bytes         INT DEFAULT NULL,
                    image_bytes         INT DEFAULT NULL,
                    script_bytes        INT DEFAULT NULL,
                    stylesheet_bytes    INT DEFAULT NULL,
                    -- Field metrics (CrUX - may be null)
                    field_lcp_ms        INT DEFAULT NULL,
                    field_cls           DECIMAL(6,4) DEFAULT NULL,
                    field_inp_ms        INT DEFAULT NULL,
                    field_ttfb_ms       INT DEFAULT NULL,
                    -- Pass/fail flags
                    lcp_pass            BOOLEAN DEFAULT FALSE,
                    cls_pass            BOOLEAN DEFAULT FALSE,
                    tbt_pass            BOOLEAN DEFAULT FALSE,
                    fetched_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(url, strategy)
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_cwv_url ON core_web_vitals (url)');
            $this->db->executeStatement('CREATE INDEX idx_cwv_strategy ON core_web_vitals (strategy)');
            $this->db->executeStatement('CREATE INDEX idx_cwv_lcp_pass ON core_web_vitals (lcp_pass)');
        }
    }
}