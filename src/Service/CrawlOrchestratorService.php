<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class CrawlOrchestratorService
{
    private string $projectDir;

    public function __construct(private Connection $db)
    {
        $this->projectDir = dirname(__DIR__, 2);
    }

    public function triggerTargetedCrawl(array $urls, bool $syncPageFacts = true): array
    {
        $urls = $this->normalizeUrls($urls);
        if ($urls === []) {
            throw new \InvalidArgumentException('At least one valid URL is required for a targeted crawl.');
        }

        $results = [];
        foreach ($urls as $url) {
            $results[] = $this->runConsole(['app:crawl-pages', '--url=' . $url]);
        }

        if ($syncPageFacts) {
            $results[] = $this->runConsole(['app:sync-page-facts']);
        }

        return [
            'mode' => 'targeted',
            'urls' => $urls,
            'sync_page_facts' => $syncPageFacts,
            'steps' => $results,
            'freshness' => $this->checkFreshness($urls),
        ];
    }

    public function triggerFullHtmlCrawl(int $limit = 250, bool $syncPageFacts = true): array
    {
        $limit = max(1, min($limit, 1000));
        $results = [
            $this->runConsole(['app:crawl-pages', '--limit=' . $limit]),
        ];

        if ($syncPageFacts) {
            $results[] = $this->runConsole(['app:sync-page-facts']);
        }

        return [
            'mode' => 'full',
            'limit' => $limit,
            'sync_page_facts' => $syncPageFacts,
            'steps' => $results,
            'freshness' => $this->checkFreshness(),
        ];
    }

    public function triggerWordPressRefresh(bool $syncPageFacts = true): array
    {
        $results = [
            $this->runConsole(['app:fetch-wordpress', '--type=all', '--no-clear']),
        ];

        if ($syncPageFacts) {
            $results[] = $this->runConsole(['app:sync-page-facts']);
        }

        return [
            'mode' => 'wordpress_refresh',
            'sync_page_facts' => $syncPageFacts,
            'steps' => $results,
            'freshness' => $this->checkFreshness(),
        ];
    }

    public function runNightlyRefresh(int $crawlLimit = 250, bool $includeHtmlCrawl = true): array
    {
        $steps = [];
        $steps[] = $this->runConsole(['app:fetch-gsc']);
        $steps[] = $this->runConsole(['app:fetch-wordpress', '--type=all', '--no-clear']);
        $steps[] = $this->runConsole(['app:sync-page-facts']);

        if ($includeHtmlCrawl) {
            $steps[] = $this->runConsole(['app:crawl-pages', '--limit=' . max(1, min($crawlLimit, 1000))]);
            $steps[] = $this->runConsole(['app:sync-page-facts']);
        }

        return [
            'mode' => 'nightly',
            'include_html_crawl' => $includeHtmlCrawl,
            'crawl_limit' => max(1, min($crawlLimit, 1000)),
            'steps' => $steps,
            'freshness' => $this->checkFreshness(),
        ];
    }

    public function runOutcomeVerification(int $minDays = 0): array
    {
        $minDays = max(0, min($minDays, 90));

        return [
            'mode' => 'verify_outcomes',
            'min_days' => $minDays,
            'step' => $this->runConsole(['app:verify-outcomes', '--min-days=' . $minDays]),
        ];
    }

    public function checkFreshness(array $urls = [], int $maxAgeHours = 24): array
    {
        $normalizedUrls = $this->normalizeUrls($urls);

        if ($normalizedUrls !== []) {
            $rows = $this->db->fetchAllAssociative(
                "SELECT url, MAX(crawled_at) AS crawled_at
                 FROM page_crawl_snapshots
                 WHERE url IN (?)
                 GROUP BY url
                 ORDER BY url",
                [$normalizedUrls],
                [\Doctrine\DBAL\ArrayParameterType::STRING]
            );
        } else {
            $rows = $this->db->fetchAllAssociative(
                "SELECT url, MAX(crawled_at) AS crawled_at
                 FROM page_crawl_snapshots
                 GROUP BY url
                 ORDER BY MAX(crawled_at) ASC
                 LIMIT 25"
            );
        }

        $byUrl = [];
        foreach ($rows as $row) {
            $byUrl[(string) $row['url']] = $row;
        }

        $details = [];
        foreach ($normalizedUrls !== [] ? $normalizedUrls : array_keys($byUrl) as $url) {
            $crawledAt = $byUrl[$url]['crawled_at'] ?? null;
            $ageHours = $this->computeAgeHours(is_string($crawledAt) ? $crawledAt : null);
            $details[] = [
                'url' => $url,
                'crawled_at' => $crawledAt,
                'age_hours' => $ageHours,
                'fresh' => $ageHours !== null ? $ageHours <= $maxAgeHours : false,
            ];
        }

        $stale = array_values(array_filter($details, static fn (array $row) => ($row['fresh'] ?? false) !== true));

        return [
            'checked_urls' => count($details),
            'max_age_hours' => $maxAgeHours,
            'stale_count' => count($stale),
            'details' => $details,
        ];
    }

    private function runConsole(array $arguments, int $timeoutSeconds = 3600): array
    {
        $phpBinary = \PHP_BINARY !== '' ? \PHP_BINARY : 'php';
        $command = array_merge([$phpBinary, $this->projectDir . DIRECTORY_SEPARATOR . 'bin' . DIRECTORY_SEPARATOR . 'console'], $arguments);
        $commandLine = $this->buildCommandLine($command);

        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open($commandLine, $descriptorSpec, $pipes, $this->projectDir);
        if (!is_resource($process)) {
            throw new \RuntimeException('Could not start console command.');
        }

        fclose($pipes[0]);

        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        $stdout = '';
        $stderr = '';
        $start = time();

        while (true) {
            $stdout .= stream_get_contents($pipes[1]) ?: '';
            $stderr .= stream_get_contents($pipes[2]) ?: '';

            $status = proc_get_status($process);
            if (!($status['running'] ?? false)) {
                break;
            }

            if ((time() - $start) > $timeoutSeconds) {
                proc_terminate($process, 9);
                foreach ($pipes as $pipe) {
                    if (is_resource($pipe)) {
                        fclose($pipe);
                    }
                }
                proc_close($process);
                throw new \RuntimeException('Console command timed out.');
            }

            usleep(100000);
        }

        $stdout .= stream_get_contents($pipes[1]) ?: '';
        $stderr .= stream_get_contents($pipes[2]) ?: '';

        fclose($pipes[1]);
        fclose($pipes[2]);

        $exitCode = proc_close($process);
        $output = trim($stdout . "\n" . $stderr);

        if ($exitCode !== 0) {
            throw new \RuntimeException($output !== '' ? $output : 'Console command failed.');
        }

        return [
            'command' => implode(' ', $arguments),
            'ok' => true,
            'exit_code' => $exitCode,
            'output' => $output,
        ];
    }

    private function buildCommandLine(array $parts): string
    {
        $escaped = array_map(static function (string $part): string {
            if (\DIRECTORY_SEPARATOR === '\\') {
                return '"' . str_replace('"', '\"', $part) . '"';
            }

            return escapeshellarg($part);
        }, $parts);

        return implode(' ', $escaped);
    }

    private function normalizeUrls(array $urls): array
    {
        $normalized = [];
        foreach ($urls as $url) {
            if (!is_string($url)) {
                continue;
            }

            $url = trim($url);
            if ($url === '') {
                continue;
            }

            if (str_starts_with($url, 'http://') || str_starts_with($url, 'https://')) {
                $path = parse_url($url, PHP_URL_PATH);
                $url = is_string($path) ? $path : '';
            }

            if ($url === '') {
                continue;
            }

            if (!str_starts_with($url, '/')) {
                $url = '/' . $url;
            }

            $url = preg_replace('#/+#', '/', $url) ?? $url;
            $normalized[] = $url === '/' ? '/' : rtrim($url, '/') . '/';
        }

        return array_values(array_unique($normalized));
    }

    private function computeAgeHours(?string $timestamp): ?float
    {
        if ($timestamp === null || trim($timestamp) === '') {
            return null;
        }

        $unix = strtotime($timestamp);
        if ($unix === false) {
            return null;
        }

        return round((time() - $unix) / 3600, 1);
    }
}
