<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:fetch-wordpress', description: 'Fetch all pages and posts from WordPress REST API with Yoast SEO data')]
class FetchWordPressCommand extends Command
{
    private string $siteUrl = 'https://doubledtrailers.com';

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
        '/why-double-d/',
        '/horse-trailers/',
    ];

    private array $utilityUrls = [
        // Contact & lead gen
        '/contact/', '/contact-us/', '/get-quote/', '/dealers/', '/financing/',
        '/join-our-mailing-list/', '/freebook/', '/book-a-video-call/',
        '/trailer-finder/', '/virtual-horse-trailer-safety-inspection/',
        '/horse-trailer-safety-webinars/',
        // Site infrastructure
        '/sitemap/', '/privacy-policy/', '/terms/',
        '/search/', '/login/', '/logout/', '/cart/', '/checkout/',
        // Thank-you, confirmation, and submit pages
        '/thank-you/', '/thank_you/', '/thanks/',
        '/confirmation/', '/confirmed/',
        '/submit/', '/submitted/',
        // Promotional / interactive tools
        '/prize-wheel/', '/review-builder/',
    ];

    private array $outerPatterns = [
        '/blog/', '/podcast/', '/video/', '/how-to', '/guide', '/tips', '/news/',
    ];

    private array $centralEntityVariants = [
        'horse trailer', 'horse trailers', 'trailer for horse', 'trailers for horse',
        'equine trailer', 'livestock trailer', 'horse hauler',
    ];

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('type', null, InputOption::VALUE_OPTIONAL, 'Content type to fetch: pages, posts, or all', 'all')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Max items to fetch (0 = all)', 0)
            ->addOption('no-clear', null, InputOption::VALUE_NONE, 'Do not clear existing crawl data before fetching')
            ->addOption('debug', null, InputOption::VALUE_NONE, 'Show extra debug output');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type    = $input->getOption('type');
        $limit   = (int) $input->getOption('limit');
        $noClear = (bool) $input->getOption('no-clear');
        $debug   = (bool) $input->getOption('debug');

        $this->ensureSchema();

        $output->writeln('');
        $output->writeln('+============================================+');
        $output->writeln('|     LOGIRI WORDPRESS CONTENT FETCHER       |');
        $output->writeln('|  Source: doubledtrailers.com WP REST API   |');
        $output->writeln('+============================================+');
        $output->writeln('');

        // ── Check if Yoast SEO is available ──
        $hasYoast = $this->checkYoast($output);

        // ── Fetch content ──
        $allItems = [];

        if ($type === 'pages' || $type === 'all') {
            $output->writeln('Fetching pages...');
            $pages = $this->fetchAllFromEndpoint('/wp-json/wp/v2/pages', $output, $debug);
            $output->writeln('  Retrieved ' . count($pages) . ' pages from WordPress.');
            $allItems = array_merge($allItems, $pages);
        }

        if ($type === 'posts' || $type === 'all') {
            $output->writeln('Fetching posts...');
            $posts = $this->fetchAllFromEndpoint('/wp-json/wp/v2/posts', $output, $debug);
            $output->writeln('  Retrieved ' . count($posts) . ' posts from WordPress.');
            $allItems = array_merge($allItems, $posts);
        }

        if (empty($allItems)) {
            $output->writeln('[ERROR] No content retrieved from WordPress API.');
            return Command::FAILURE;
        }

        if ($limit > 0) {
            $allItems = array_slice($allItems, 0, $limit);
            $output->writeln("Limited to {$limit} items.");
        }

        // ── Clear existing data ──
        if (!$noClear) {
            $this->db->executeStatement('DELETE FROM page_crawl_snapshots');
            $output->writeln('Cleared old crawl data.');
        }

        // ── Process and store each item ──
        $stored  = 0;
        $skipped = 0;
        $total   = count($allItems);

        foreach ($allItems as $i => $item) {
            $path = $this->extractPath($item);
            if (!$path) {
                $skipped++;
                continue;
            }

            $output->writeln("[{$i}/{$total}] Processing: {$path}");

            $row = $this->processItem($item, $path, $hasYoast, $debug, $output);

            if ($row) {
                // Remove any existing row for this URL (in case of --no-clear)
                $this->db->executeStatement('DELETE FROM page_crawl_snapshots WHERE url = ?', [$path]);
                $this->db->insert('page_crawl_snapshots', $row);

                $output->writeln(
                    "  OK | H1: " . ($row['h1'] ?? '(none)') .
                    " | Words: {$row['word_count']}" .
                    " | Entity: " . ($row['has_central_entity'] ? 'YES' : 'NO') .
                    " | Type: " . ($row['page_type'] ?? 'unknown')
                );
                $stored++;
            } else {
                $output->writeln("  SKIPPED: Could not process {$path}");
                $skipped++;
            }
        }

        $output->writeln('');
        $output->writeln('==============================================');
        $output->writeln("Done. Stored: {$stored} | Skipped: {$skipped} | Total from API: {$total}");
        $output->writeln('');

        // ── Summary stats ──
        $this->printSummary($output);

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  FETCH ALL ITEMS FROM A WP REST API ENDPOINT
    //  Handles pagination (max 100 per page)
    // ─────────────────────────────────────────────

    private function fetchAllFromEndpoint(string $endpoint, OutputInterface $output, bool $debug): array
    {
        $allItems = [];
        $page     = 1;
        $perPage  = 100;

        while (true) {
            $url = $this->siteUrl . $endpoint . "?per_page={$perPage}&page={$page}&status=publish";
            if ($debug) $output->writeln("  [DEBUG] Fetching: {$url}");

            $context = stream_context_create([
                'http' => [
                    'method'  => 'GET',
                    'header'  => "User-Agent: LogiriBot/1.0\r\nAccept: application/json",
                    'timeout' => 30,
                    'ignore_errors' => true,
                ],
            ]);

            $raw = @file_get_contents($url, false, $context);

            if ($raw === false) {
                $output->writeln("  [WARN] Failed to fetch page {$page} of {$endpoint}");
                break;
            }

            // Check HTTP status
            $status = $this->parseHttpStatus($http_response_header ?? []);
            if ($status === 400 || $status === 404) {
                // No more pages
                if ($debug) $output->writeln("  [DEBUG] Got HTTP {$status} -- no more pages.");
                break;
            }

            $items = json_decode($raw, true);
            if (!is_array($items) || empty($items)) {
                break;
            }

            $allItems = array_merge($allItems, $items);

            // Check total pages from response headers
            $totalPages = $this->extractHeader($http_response_header ?? [], 'X-WP-TotalPages');
            if ($debug) $output->writeln("  [DEBUG] Page {$page}" . ($totalPages ? " of {$totalPages}" : '') . " -- got " . count($items) . " items");

            if ($totalPages && $page >= (int) $totalPages) {
                break;
            }

            $page++;
            usleep(300000); // 300ms between requests to be polite
        }

        return $allItems;
    }

    // ─────────────────────────────────────────────
    //  PROCESS A SINGLE WP ITEM INTO A CRAWL ROW
    // ─────────────────────────────────────────────

    private function processItem(array $item, string $path, bool $hasYoast, bool $debug, OutputInterface $output): ?array
    {
        // ── Title from WP ──
        $wpTitle = html_entity_decode($item['title']['rendered'] ?? '', ENT_QUOTES | ENT_HTML5, 'UTF-8');
        $wpTitle = strip_tags($wpTitle);

        // ── Content body ──
        $contentHtml = $item['content']['rendered'] ?? '';
        $bodyText    = $this->htmlToCleanText($contentHtml);
        $wordCount   = str_word_count(trim($bodyText));

        // ── Extract H1 from content (first <h1> in rendered content, or use WP title) ──
        $h1 = null;
        if (preg_match('/<h1[^>]*>(.*?)<\/h1>/is', $contentHtml, $m)) {
            $h1 = trim(strip_tags($m[1]));
        }
        // Many WP themes use the post title as the rendered H1, so fall back to that
        if (!$h1) {
            $h1 = $wpTitle;
        }

        // ── Extract H2s ──
        $h2s = [];
        if (preg_match_all('/<h2[^>]*>(.*?)<\/h2>/is', $contentHtml, $m)) {
            foreach ($m[1] as $h2Raw) {
                $h2 = trim(strip_tags($h2Raw));
                if ($h2) $h2s[] = $h2;
            }
        }

        // ── Yoast SEO data ──
        $titleTag        = $wpTitle; // Default: WP title
        $metaDescription = null;
        $canonicalUrl    = null;
        $isNoindex       = false;
        $schemaTypes     = [];
        $focusKeyword    = null;

        if ($hasYoast && isset($item['yoast_head_json'])) {
            $yoast = $item['yoast_head_json'];

            // SEO title (what appears in browser tab / search results)
            if (!empty($yoast['title'])) {
                $titleTag = html_entity_decode($yoast['title'], ENT_QUOTES | ENT_HTML5, 'UTF-8');
            }

            // Meta description
            if (!empty($yoast['og_description'])) {
                $metaDescription = $yoast['og_description'];
            }
            if (!empty($yoast['description'])) {
                $metaDescription = $yoast['description'];
            }

            // Canonical
            if (!empty($yoast['canonical'])) {
                $canonicalUrl = $yoast['canonical'];
            }

            // Robots / noindex
            if (isset($yoast['robots'])) {
                $robots = $yoast['robots'];
                if (isset($robots['index']) && $robots['index'] === 'noindex') {
                    $isNoindex = true;
                }
            }

            // Schema types from Yoast's structured data
            if (isset($yoast['schema']) && isset($yoast['schema']['@graph'])) {
                foreach ($yoast['schema']['@graph'] as $schemaItem) {
                    if (isset($schemaItem['@type'])) {
                        $type = $schemaItem['@type'];
                        $schemaTypes[] = is_array($type) ? implode(',', $type) : $type;
                    }
                }
            }

            $schemaTypes = array_values(array_unique($schemaTypes));

            if ($debug) {
                $output->writeln("  [YOAST] title=" . ($titleTag ?? 'n/a') . " | desc=" . substr($metaDescription ?? 'n/a', 0, 50) . " | schema=" . implode(',', $schemaTypes));
            }
        }

        // ── Central entity check — ≥1 mention sufficient (was ≥2, caused false positives on short pages) ──
        $bodyLower          = strtolower($bodyText);
        $centralEntityCount = 0;
        foreach ($this->centralEntityVariants as $variant) {
            $centralEntityCount += substr_count($bodyLower, $variant);
        }
        $hasCentralEntity = $centralEntityCount >= 1;

        // ── Internal links from content body ──
        $internalLinks  = [];
        $coreLinksFound = [];
        $hasCoreLink    = false;

        if (preg_match_all('/<a[^>]+href=["\']([^"\']+)["\'][^>]*>/i', $contentHtml, $linkMatches)) {
            foreach ($linkMatches[1] as $href) {
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
        }

        $internalLinks  = array_values(array_unique($internalLinks));
        $coreLinksFound = array_values(array_unique($coreLinksFound));

        // ── H1 vs title alignment ──
        $h1MatchesTitle = $this->checkH1TitleMatch($h1, $titleTag);

        // ── Page type classification ──
        $pageType = $this->classifyPageType($path);

        // Postgres requires proper boolean literals — PHP true/false become empty strings via Doctrine
        $pgBool = fn(bool $v): string => $v ? 't' : 'f';

        return [
            'url'                  => $path,
            'http_status'          => 200, // If it's in the API, it's published/200
            'title_tag'            => $titleTag ? substr($titleTag, 0, 500) : null,
            'h1'                   => $h1 ? substr($h1, 0, 500) : null,
            'h2s'                  => json_encode(array_slice($h2s, 0, 20)),
            'meta_description'     => $metaDescription ? substr($metaDescription, 0, 500) : null,
            'word_count'           => $wordCount,
            'has_central_entity'   => $pgBool($hasCentralEntity),
            'central_entity_count' => $centralEntityCount,
            'internal_links'       => json_encode(array_slice($internalLinks, 0, 100)),
            'has_core_link'        => $pgBool($hasCoreLink),
            'core_links_found'     => json_encode($coreLinksFound),
            'h1_matches_title'     => $pgBool($h1MatchesTitle),
            'schema_types'         => json_encode($schemaTypes),
            'canonical_url'        => $canonicalUrl ? substr($canonicalUrl, 0, 500) : null,
            'is_noindex'           => $pgBool($isNoindex),
            'page_type'            => $pageType,
            'is_utility'           => $pgBool($this->isUtilityUrl($path)),
            'crawled_at'           => date('Y-m-d H:i:s'),
        ];
    }

    // ─────────────────────────────────────────────
    //  CHECK IF YOAST SEO IS AVAILABLE
    // ─────────────────────────────────────────────

    private function checkYoast(OutputInterface $output): bool
    {
        $url     = $this->siteUrl . '/wp-json/wp/v2/pages?per_page=1';
        $context = stream_context_create([
            'http' => [
                'method'  => 'GET',
                'header'  => "User-Agent: LogiriBot/1.0\r\nAccept: application/json",
                'timeout' => 15,
                'ignore_errors' => true,
            ],
        ]);

        $raw = @file_get_contents($url, false, $context);
        if ($raw === false) return false;

        $data = json_decode($raw, true);
        if (!is_array($data) || empty($data)) return false;

        $hasYoast = isset($data[0]['yoast_head_json']);
        $output->writeln($hasYoast
            ? '  Yoast SEO detected -- will pull SEO titles, meta descriptions, schema, canonical, robots.'
            : '  Yoast SEO not detected -- using WordPress defaults only.');

        return $hasYoast;
    }

    // ─────────────────────────────────────────────
    //  EXTRACT URL PATH FROM WP ITEM
    // ─────────────────────────────────────────────

    private function extractPath(array $item): ?string
    {
        $link = $item['link'] ?? null;
        if (!$link) return null;

        $path = parse_url($link, PHP_URL_PATH);
        if (!$path || $path === '') return null;

        // Skip non-HTML assets
        $skipExtensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.jpg', '.jpeg', '.png', '.zip'];
        foreach ($skipExtensions as $ext) {
            if (str_ends_with(strtolower($path), $ext)) return null;
        }

        return $path;
    }

    // ─────────────────────────────────────────────
    //  STRIP HTML TO CLEAN TEXT
    // ─────────────────────────────────────────────

    private function htmlToCleanText(string $html): string
    {
        // Remove scripts, styles, nav, footer
        $html = preg_replace('/<(script|style|nav|header|footer|noscript)[^>]*>.*?<\/\1>/is', '', $html);

        // Remove HTML tags
        $text = strip_tags($html);

        // Decode entities
        $text = html_entity_decode($text, ENT_QUOTES | ENT_HTML5, 'UTF-8');

        // Normalize whitespace
        $text = preg_replace('/\s+/', ' ', $text);

        return trim($text);
    }

    // ─────────────────────────────────────────────
    //  H1 / TITLE MATCH CHECK
    // ─────────────────────────────────────────────

    private function checkH1TitleMatch(?string $h1, ?string $titleTag): bool
    {
        if (!$h1 || !$titleTag) return false;

        $stopWords  = ['a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 'is', 'are', 'was', 'be'];
        $h1Clean    = strtolower(preg_replace('/[^a-z0-9 ]/i', '', $h1));
        $titleClean = strtolower(preg_replace('/[^a-z0-9 ]/i', '', $titleTag));

        $h1Words    = array_filter(explode(' ', $h1Clean), fn($w) => strlen($w) > 3 && !in_array($w, $stopWords));
        $titleWords = array_filter(explode(' ', $titleClean), fn($w) => strlen($w) > 3 && !in_array($w, $stopWords));

        if (count($h1Words) === 0 || count($titleWords) === 0) return false;

        $matches = count(array_filter($h1Words, fn($w) => str_contains($titleClean, $w)));
        $overlap = $matches / count($h1Words);

        return $overlap >= 0.70;
    }

    // ─────────────────────────────────────────────
    //  PAGE CLASSIFICATION
    // ─────────────────────────────────────────────

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

    private function isUtilityUrl(string $path): bool
    {
        $n = '/' . trim($path, '/') . '/';

        // Exact match or prefix match against known utility URLs
        foreach ($this->utilityUrls as $util) {
            $u = '/' . trim($util, '/') . '/';
            if ($n === $u || str_starts_with($n, $u)) return true;
        }

        // Substring patterns — catch URLs like /inspection-thank-you-submit/, /prize-wheel-2024/, etc.
        $utilityPatterns = [
            'thank-you', 'thank_you', 'thanks',
            '-submit', '-confirmation', '-confirmed',
            'prize-wheel', 'giveaway', 'contest',
            'review-builder', 'quiz-builder',
        ];
        foreach ($utilityPatterns as $pattern) {
            if (str_contains($n, $pattern)) return true;
        }

        return false;
    }

    private function classifyPageType(string $path): string
    {
        if ($this->isUtilityUrl($path)) return 'utility';
        if ($this->isCoreUrl($path)) return 'core';
        foreach ($this->outerPatterns as $pattern) {
            if (str_contains($path, $pattern)) return 'outer';
        }
        return 'outer';
    }

    // ─────────────────────────────────────────────
    //  PRINT SUMMARY STATS
    // ─────────────────────────────────────────────

    private function printSummary(OutputInterface $output): void
    {
        try {
            $total = $this->db->fetchOne('SELECT COUNT(*) FROM page_crawl_snapshots');
            $core  = $this->db->fetchOne("SELECT COUNT(*) FROM page_crawl_snapshots WHERE page_type = 'core'");
            $outer = $this->db->fetchOne("SELECT COUNT(*) FROM page_crawl_snapshots WHERE page_type = 'outer'");
            $util  = $this->db->fetchOne("SELECT COUNT(*) FROM page_crawl_snapshots WHERE page_type = 'utility'");
            $entity   = $this->db->fetchOne('SELECT COUNT(*) FROM page_crawl_snapshots WHERE has_central_entity = true');
            $noEntity = $this->db->fetchOne('SELECT COUNT(*) FROM page_crawl_snapshots WHERE has_central_entity IS NOT TRUE');
            $h1Match    = $this->db->fetchOne('SELECT COUNT(*) FROM page_crawl_snapshots WHERE h1_matches_title = true');
            $h1Mismatch = $this->db->fetchOne('SELECT COUNT(*) FROM page_crawl_snapshots WHERE h1_matches_title IS NOT TRUE');
            $hasSchema  = $this->db->fetchOne("SELECT COUNT(*) FROM page_crawl_snapshots WHERE schema_types IS NOT NULL AND schema_types != '[]'");

            $output->writeln('  ── DATABASE SUMMARY ──────────────────────');
            $output->writeln("  Total pages:          {$total}");
            $output->writeln("  Core:                 {$core}");
            $output->writeln("  Outer:                {$outer}");
            $output->writeln("  Utility:              {$util}");
            $output->writeln("  Central entity YES:   {$entity}");
            $output->writeln("  Central entity NO:    {$noEntity}");
            $output->writeln("  H1/Title match:       {$h1Match}");
            $output->writeln("  H1/Title mismatch:    {$h1Mismatch}");
            $output->writeln("  Has schema:           {$hasSchema}");
            $output->writeln('  ──────────────────────────────────────────');
        } catch (\Exception $e) {
            // Non-fatal
        }
    }

    // ─────────────────────────────────────────────
    //  HTTP HELPERS
    // ─────────────────────────────────────────────

    private function parseHttpStatus(array $headers): ?int
    {
        if (isset($headers[0]) && preg_match('#HTTP/\d\.?\d? (\d{3})#', $headers[0], $m)) {
            return (int) $m[1];
        }
        return null;
    }

    private function extractHeader(array $headers, string $name): ?string
    {
        foreach ($headers as $header) {
            if (stripos($header, $name . ':') === 0) {
                return trim(substr($header, strlen($name) + 1));
            }
        }
        return null;
    }

    // ─────────────────────────────────────────────
    //  ENSURE DB SCHEMA (same table as CrawlPagesCommand)
    // ─────────────────────────────────────────────

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
                    is_utility              BOOLEAN DEFAULT FALSE,
                    crawled_at              TIMESTAMP NOT NULL
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_crawl_url ON page_crawl_snapshots (url)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_page_type ON page_crawl_snapshots (page_type)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_has_core_link ON page_crawl_snapshots (has_core_link)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_central_entity ON page_crawl_snapshots (has_central_entity)');
        } else {
            try {
                $this->db->executeStatement('ALTER TABLE page_crawl_snapshots ADD COLUMN IF NOT EXISTS is_utility BOOLEAN DEFAULT FALSE');
            } catch (\Exception $e) {}
        }
    }
}

    


    
