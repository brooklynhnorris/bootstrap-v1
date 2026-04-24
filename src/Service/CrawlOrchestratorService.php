<?php

namespace App\Service;

use Doctrine\DBAL\Connection;
use Symfony\Component\Process\Process;

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
            $this->runConsole(['app:fetch-wordpress', '--type=all']),
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
        $steps[] = $this->runConsole(['app:fetch-wordpress', '--type=all']);
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

        $process = new Process($command, $this->projectDir);
        $process->setTimeout($timeoutSeconds);
        $process->run();

        $output = trim($process->getOutput() . "\n" . $process->getErrorOutput());

        if (!$process->isSuccessful()) {
            throw new \RuntimeException($output !== '' ? $output : 'Console command failed.');
        }

        return [
            'command' => implode(' ', $arguments),
            'ok' => true,
            'exit_code' => $process->getExitCode(),
            'output' => $output,
        ];
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
