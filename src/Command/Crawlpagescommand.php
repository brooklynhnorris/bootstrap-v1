<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:crawl-pages', description: 'Crawl indexed pages and extract structural SEO signals')]
class CrawlPagesCommand extends Command
{
    private string $baseUrl = 'https://doubledtrailers.com';

    private array $coreUrls = [
        '/',
        '/bumper-pull-horse-trailers/',
        '/gooseneck-horse-trailers/',
        '/living-quarters-horse-trailers/',
        '/bumper-pull-2-horse-straight-load-trailer/',
        '/one-horse-trailer-bumper-pull/',
        '/bumper-pull-safetack-2-horse-slant-load-trailer/',
        '/1-horse-bumper-pull-with-living-quarters/',
        '/bumper-pull-3-horse-slant-load-trailer/',
        '/bumper-pull-v-sport-2-horse-straight-load-trailer/',
        '/bumper-pull-townsmand-2-horse-straight-load-trailer/',
        '/gooseneck-3-horse-slant-load-trailer/',
        '/gooseneck-4-horse-slant-load-trailer/',
        '/2-plus-1-gooseneck-3-horse-straight-load-trailers/',
        '/gooseneck-2-horse-slant-load-trailer/',
        '/gooseneck-2-horse-straight-load-trailer/',
        '/trail-blazer-living-quarters-horse-trailer/',
        '/the-basics-living-quarters/',
        '/safetack-reverse-living-quarters-horse-trailer/',
        '/about/',
        '/contact/',
        '/contact-us/',
        '/get-quote/',
        '/dealers/',
        '/financing/',
        '/why-double-d/',
        '/horse-trailers/',
        '/trailer-finder/',
        '/virtual-horse-trailer-safety-inspection/',
        '/horse-trailer-safety-webinars/',
        '/join-our-mailing-list/',
        '/freebook/',
        '/book-a-video-call/',
    ];

    private array $outerPatterns = [
        '/blog/',
        '/podcast/',
        '/video/',
        '/how-to',
        '/guide',
        '/tips',
        '/news/',
    ];

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('url', null, InputOption::VALUE_OPTIONAL, 'Crawl a single URL path only (e.g. /gooseneck-horse-trailers/)')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Max number of URLs to crawl', 200)
            ->addOption('debug', null, InputOption::VALUE_NONE, 'Print extra debug output');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $singleUrl = $input->getOption('url');
        $limit     = (int) $input->getOption('limit');
        $debug     = (bool) $input->getOption('debug');

        $this->ensureSchema();

        if ($singleUrl) {
            $urls = [$singleUrl];
            $output->writeln("Single URL mode: {$singleUrl}");
        } else {
            $urls = $this->getUrlsFromGsc($limit);
            $output->writeln('Found ' . count($urls) . ' URLs from GSC snapshots.');
        }

        if (empty($urls)) {
            $output->writeln('No URLs found. Run app:fetch-gsc first.');
            return Command::FAILURE;
        }

        if (!$singleUrl) {
            $this->db->executeStatement('DELETE FROM page_crawl_snapshots');
            $output->writeln('Cleared old crawl data.');
        }

        $crawled = 0;
        $failed  = 0;
        $total   = count($urls);

        foreach ($urls as $path) {
            $fullUrl = $this->buildFullUrl($path);
            $output->writeln("[{$crawled}/{$total}] Crawling: {$fullUrl}");

            $result = $this->crawlPage($fullUrl, $path, $debug, $output);

            if ($result === null) {
                $output->writeln("  FAILED: {$fullUrl}");
                $failed++;
            } else {
                $this->db->insert('page_crawl_snapshots', $result);
                $output->writeln(
                    "  OK | H1: " . ($result['h1'] ?? '(none)') .
                    " | Words: {$result['word_count']}" .
                    " | Entity: " . ($result['has_central_entity'] ? 'YES' : 'NO') .
                    " | Core link: " . ($result['has_core_link'] ? 'YES' : 'NO') .
                    " | Type: " . ($result['page_type'] ?? 'unknown')
                );
                $crawled++;
            }

            usleep(1500000);
        }

        $output->writeln("Done. Crawled: {$crawled} | Failed: {$failed}");
        return Command::SUCCESS;
    }

    private function crawlPage(string $fullUrl, string $path, bool $debug, OutputInterface $output): ?array
    {
        $context = stream_context_create([
            'http' => [
                'method'          => 'GET',
                'header'          => "User-Agent: Mozilla/5.0 (compatible; LogiriBot/1.0)\r\nAccept: text/html",
                'timeout'         => 15,
                'follow_location' => 1,
                'max_redirects'   => 3,
                'ignore_errors'   => true,
            ],
        ]);

        $html = @file_get_contents($fullUrl, false, $context);
        if ($html === false || empty($html)) {
            return null;
        }

        $httpStatus = $this->parseHttpStatus($http_response_header ?? []);

        $dom = new \DOMDocument();
        libxml_use_internal_errors(true);
        $dom->loadHTML($html, LIBXML_NOWARNING | LIBXML_NOERROR);
        libxml_clear_errors();
        $xpath = new \DOMXPath($dom);

        // Title
        $titleNodes = $xpath->query('//title');
        $titleTag   = $titleNodes->length > 0 ? trim($titleNodes->item(0)->textContent) : null;

        // H1
        $h1Nodes = $xpath->query('//h1');
        $h1      = $h1Nodes->length > 0 ? trim($h1Nodes->item(0)->textContent) : null;

        // H2s
        $h2Nodes = $xpath->query('//h2');
        $h2s = [];
        foreach ($h2Nodes as $h2) {
            $t = trim($h2->textContent);
            if ($t) $h2s[] = $t;
        }

        // Meta description
        $metaNodes       = $xpath->query('//meta[@name="description"]/@content');
        $metaDescription = $metaNodes->length > 0 ? trim($metaNodes->item(0)->textContent) : null;

        // Canonical
        $canonicalNodes = $xpath->query('//link[@rel="canonical"]/@href');
        $canonicalUrl   = $canonicalNodes->length > 0 ? trim($canonicalNodes->item(0)->textContent) : null;

        // Noindex
        $robotsNodes = $xpath->query('//meta[@name="robots"]/@content');
        $isNoindex   = false;
        if ($robotsNodes->length > 0) {
            $isNoindex = str_contains(strtolower($robotsNodes->item(0)->textContent), 'noindex');
        }

        // Schema types
        $schemaNodes = $xpath->query('//*[@type="application/ld+json"]');
        $schemaTypes = [];
        foreach ($schemaNodes as $node) {
            $json = json_decode($node->textContent, true);
            if (isset($json['@type'])) $schemaTypes[] = $json['@type'];
        }

        // Body text + word count
        $bodyNodes = $xpath->query('//body');
        $bodyText  = '';
        if ($bodyNodes->length > 0) {
            $bodyText = preg_replace('/\s+/', ' ', strip_tags($bodyNodes->item(0)->textContent));
        }
        $wordCount = $bodyText ? str_word_count(trim($bodyText)) : 0;

        // Central entity
        $bodyLower          = strtolower($bodyText);
        $centralEntityCount = substr_count($bodyLower, 'horse trailer');
        $hasCentralEntity   = $centralEntityCount > 0;

        // Internal links + core link detection
        $linkNodes      = $xpath->query('//a[@href]');
        $internalLinks  = [];
        $coreLinksFound = [];
        $hasCoreLink    = false;

        foreach ($linkNodes as $link) {
            $href = trim($link->getAttribute('href'));
            if (str_starts_with($href, '/') || str_contains($href, 'doubledtrailers.com')) {
                $parsed = parse_url($href, PHP_URL_PATH);
                if ($parsed) {
                    $internalLinks[] = $parsed;
                    if ($this->isCoreUrl($parsed)) {
                        $hasCoreLink      = true;
                        $coreLinksFound[] = $parsed;
                    }
                }
            }
        }

        $internalLinks  = array_values(array_unique($internalLinks));
        $coreLinksFound = array_values(array_unique($coreLinksFound));

        // H1 vs title alignment (fuzzy — 60% word overlap)
        $h1MatchesTitle = false;
        if ($h1 && $titleTag) {
            $h1Words    = array_filter(explode(' ', strtolower(preg_replace('/[^a-z0-9 ]/i', '', $h1))), fn($w) => strlen($w) > 3);
            $titleLower = strtolower($titleTag);
            $matches    = count(array_filter($h1Words, fn($w) => str_contains($titleLower, $w)));
            $h1MatchesTitle = count($h1Words) > 0 && ($matches / count($h1Words)) >= 0.6;
        }

        return [
            'url'                  => $path,
            'http_status'          => $httpStatus,
            'title_tag'            => $titleTag ? substr($titleTag, 0, 500) : null,
            'h1'                   => $h1 ? substr($h1, 0, 500) : null,
            'h2s'                  => json_encode(array_slice($h2s, 0, 20)),
            'meta_description'     => $metaDescription ? substr($metaDescription, 0, 500) : null,
            'word_count'           => $wordCount,
            'has_central_entity'   => $hasCentralEntity ? 1 : 0,
            'central_entity_count' => $centralEntityCount,
            'internal_links'       => json_encode(array_slice($internalLinks, 0, 100)),
            'has_core_link'        => $hasCoreLink ? 1 : 0,
            'core_links_found'     => json_encode($coreLinksFound),
            'h1_matches_title'     => $h1MatchesTitle ? 1 : 0,
            'schema_types'         => json_encode($schemaTypes),
            'canonical_url'        => $canonicalUrl ? substr($canonicalUrl, 0, 500) : null,
            'is_noindex'           => $isNoindex ? 1 : 0,
            'page_type'            => $this->classifyPageType($path),
            'crawled_at'           => date('Y-m-d H:i:s'),
        ];
    }

    private function getUrlsFromGsc(int $limit): array
    {
        $rows = $this->db->fetchAllAssociative(
            "SELECT DISTINCT page FROM gsc_snapshots
             WHERE date_range = '28d' AND page LIKE '%doubledtrailers.com%'
             ORDER BY page LIMIT ?",
            [$limit]
        );
        $paths = [];
        foreach ($rows as $row) {
            $parsed = parse_url($row['page'], PHP_URL_PATH);
            if ($parsed) $paths[] = $parsed;
        }
        return array_unique($paths);
    }

    private function buildFullUrl(string $path): string
    {
        if (str_starts_with($path, 'http')) return $path;
        return rtrim($this->baseUrl, '/') . '/' . ltrim($path, '/');
    }

    private function isCoreUrl(string $path): bool
    {
        $n = '/' . trim($path, '/') . '/';
        $n = str_replace('//', '/', $n);
        foreach ($this->coreUrls as $core) {
            $c = '/' . trim($core, '/') . '/';
            $c = str_replace('//', '/', $c);
            if ($n === $c) return true;
        }
        return false;
    }

    private function classifyPageType(string $path): string
    {
        if ($this->isCoreUrl($path)) return 'core';
        foreach ($this->outerPatterns as $pattern) {
            if (str_contains($path, $pattern)) return 'outer';
        }
        return 'outer';
    }

    private function parseHttpStatus(array $headers): ?int
    {
        if (isset($headers[0]) && preg_match('#HTTP/\d\.?\d? (\d{3})#', $headers[0], $m)) {
            return (int) $m[1];
        }
        return null;
    }

    private function ensureSchema(): void
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        );
        if (!in_array('page_crawl_snapshots', $tables)) {
            $this->db->executeStatement("
                CREATE TABLE page_crawl_snapshots (
                    id                      SERIAL PRIMARY KEY,
                    url                     TEXT NOT NULL,
                    http_status             INT DEFAULT NULL,
                    title_tag               TEXT DEFAULT NULL,
                    h1                      TEXT DEFAULT NULL,
                    h2s                     TEXT DEFAULT NULL,
                    meta_description        TEXT DEFAULT NULL,
                    word_count              INT DEFAULT 0,
                    has_central_entity      BOOLEAN DEFAULT FALSE,
                    central_entity_count    INT DEFAULT 0,
                    internal_links          TEXT DEFAULT NULL,
                    has_core_link           BOOLEAN DEFAULT FALSE,
                    core_links_found        TEXT DEFAULT NULL,
                    h1_matches_title        BOOLEAN DEFAULT FALSE,
                    schema_types            TEXT DEFAULT NULL,
                    canonical_url           TEXT DEFAULT NULL,
                    is_noindex              BOOLEAN DEFAULT FALSE,
                    page_type               VARCHAR(20) DEFAULT NULL,
                    crawled_at              TIMESTAMP NOT NULL
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_crawl_url ON page_crawl_snapshots (url)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_page_type ON page_crawl_snapshots (page_type)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_has_core_link ON page_crawl_snapshots (has_core_link)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_central_entity ON page_crawl_snapshots (has_central_entity)');
        }
    }
}