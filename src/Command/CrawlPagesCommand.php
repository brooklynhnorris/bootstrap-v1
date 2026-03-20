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

    // Pages that should never be flagged for content rules — utility/navigational pages
    private array $utilityUrls = [
        '/contact/', '/contact-us/', '/get-quote/', '/dealers/', '/financing/',
        '/join-our-mailing-list/', '/freebook/', '/book-a-video-call/',
        '/trailer-finder/', '/sitemap/', '/privacy-policy/', '/terms/',
        '/search/', '/login/', '/logout/', '/cart/', '/checkout/',
    ];

    private array $outerPatterns = [
        '/blog/', '/podcast/', '/video/', '/how-to', '/guide', '/tips', '/news/',
    ];

    // All known central entity variations for horse trailers
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
            ->addOption('url', null, InputOption::VALUE_OPTIONAL, 'Crawl a single URL path')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Max URLs to crawl', 200)
            ->addOption('debug', null, InputOption::VALUE_NONE, 'Extra debug output');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $singleUrl = $input->getOption('url');
        $limit     = (int) $input->getOption('limit');
        $debug     = (bool) $input->getOption('debug');

        $this->ensureSchema();
        $overrides = $this->loadOverrides();

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
                $result = $this->applyOverrides($result, $overrides);
                try {
                    $this->db->executeStatement('DELETE FROM page_crawl_snapshots WHERE url = ?', [$path]);
                    $this->db->insert('page_crawl_snapshots', $result);

                    $overrideNote = isset($overrides[$path]) ? ' [OVERRIDE]' : '';
                    $output->writeln(
                        "  OK{$overrideNote} | H1: " . ($result['h1'] ?? '(none)') .
                        " | Words: {$result['word_count']}" .
                        " | Entity: " . ($result['has_central_entity'] ? 'YES' : 'NO') .
                        " | Core link: " . ($result['has_core_link'] ? 'YES' : 'NO') .
                        " | Type: " . ($result['page_type'] ?? 'unknown')
                    );
                    $crawled++;
                } catch (\Exception $e) {
                    $output->writeln("  [WARN] DB insert failed for {$path}: " . substr($e->getMessage(), 0, 100));
                    $failed++;
                }
            }

            usleep(500000);
        }

        $output->writeln("Done. Crawled: {$crawled} | Failed: {$failed} | Total: {$total}");
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
        if ($html === false || empty($html)) return null;

        $httpStatus = $this->parseHttpStatus($http_response_header ?? []);
        if ($debug) $output->writeln("  [DEBUG] HTTP {$httpStatus}");

        $dom = new \DOMDocument();
        libxml_use_internal_errors(true);
        $dom->loadHTML($html, LIBXML_NOWARNING | LIBXML_NOERROR);
        libxml_clear_errors();
        $xpath = new \DOMXPath($dom);

        // ── Title ──
        $titleNodes = $xpath->query('//title');
        $titleTag   = $titleNodes->length > 0 ? trim($titleNodes->item(0)->textContent) : null;

        // ── H1 ──
        $h1Nodes = $xpath->query('//h1');
        $h1      = $h1Nodes->length > 0 ? trim($h1Nodes->item(0)->textContent) : null;

        // ── H2s ──
        $h2Nodes = $xpath->query('//h2');
        $h2s = [];
        foreach ($h2Nodes as $h2) {
            $t = trim($h2->textContent);
            if ($t) $h2s[] = $t;
        }

        // ── Meta description ──
        $metaNodes       = $xpath->query('//meta[@name="description"]/@content');
        $metaDescription = $metaNodes->length > 0 ? trim($metaNodes->item(0)->textContent) : null;

        // ── Canonical ──
        $canonicalNodes = $xpath->query('//link[@rel="canonical"]/@href');
        $canonicalUrl   = $canonicalNodes->length > 0 ? trim($canonicalNodes->item(0)->textContent) : null;

        // ── Noindex ──
        $robotsNodes = $xpath->query('//meta[@name="robots"]/@content');
        $isNoindex   = false;
        if ($robotsNodes->length > 0) {
            $isNoindex = str_contains(strtolower($robotsNodes->item(0)->textContent), 'noindex');
        }

        // ── Schema types — handles both top-level @type and @graph arrays (Yoast/RankMath) ──
        $schemaNodes = $xpath->query('//*[@type="application/ld+json"]');
        $schemaTypes = [];
        foreach ($schemaNodes as $node) {
            $json = json_decode($node->textContent, true);
            if (!$json) continue;
            // Top-level @type
            if (isset($json['@type'])) {
                $schemaTypes[] = is_array($json['@type']) ? implode(',', $json['@type']) : $json['@type'];
            }
            // @graph array (Yoast SEO pattern)
            if (isset($json['@graph']) && is_array($json['@graph'])) {
                foreach ($json['@graph'] as $item) {
                    if (isset($item['@type'])) {
                        $schemaTypes[] = is_array($item['@type']) ? implode(',', $item['@type']) : $item['@type'];
                    }
                }
            }
        }
        $schemaTypes = array_values(array_unique($schemaTypes));

        // ── ACCURATE word count — strip nav/header/footer/aside/scripts/styles first ──
        $wordCount = 0;
        $bodyText  = '';
        $mainContentSelectors = [
            '//main',
            '//article',
            '//*[@id="content"]',
            '//*[@id="main-content"]',
            '//*[@class="entry-content"]',
            '//*[@class="page-content"]',
            '//*[@class="post-content"]',
        ];

        $contentText = '';
        foreach ($mainContentSelectors as $selector) {
            $nodes = $xpath->query($selector);
            if ($nodes->length > 0) {
                $contentText = $this->extractCleanText($nodes->item(0), $xpath);
                break;
            }
        }

        // Fallback: strip noisy elements from body
        if (empty(trim($contentText))) {
            // Remove nav, header, footer, aside, scripts, styles, forms
            $noiseSelectors = ['//nav', '//header', '//footer', '//aside',
                               '//script', '//style', '//noscript',
                               '//*[@class="menu"]', '//*[@id="menu"]',
                               '//*[@class="navigation"]', '//*[@role="navigation"]',
                               '//*[@class="sidebar"]', '//*[@id="sidebar"]',
                               '//*[@class="widget"]', '//*[contains(@class,"cookie")]',
                               '//*[@aria-label="breadcrumb"]'];
            foreach ($noiseSelectors as $sel) {
                $noiseNodes = $xpath->query($sel);
                foreach ($noiseNodes as $noiseNode) {
                    if ($noiseNode->parentNode) {
                        $noiseNode->parentNode->removeChild($noiseNode);
                    }
                }
            }
            $bodyNodes = $xpath->query('//body');
            if ($bodyNodes->length > 0) {
                $contentText = $this->extractCleanText($bodyNodes->item(0), $xpath);
            }
        }

        $bodyText  = $contentText;
        $wordCount = $bodyText ? str_word_count(trim($bodyText)) : 0;

        // ── NEW FIELDS: Tier 1 & 2 extractions ──

        // Helper: sanitize string for PostgreSQL UTF-8 compatibility
        $sanitizeUtf8 = function(?string $text): ?string {
            if ($text === null) return null;
            // Remove invalid UTF-8 sequences
            $clean = mb_convert_encoding($text, 'UTF-8', 'UTF-8');
            // Remove any remaining non-UTF-8 bytes
            $clean = preg_replace('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/u', '', $clean);
            // Remove orphaned continuation bytes
            $clean = iconv('UTF-8', 'UTF-8//IGNORE', $clean);
            return $clean ?: null;
        };

        // Body text snippet — first 500 chars for keyword checking
        $bodyTextSnippet = $bodyText ? $sanitizeUtf8(substr(trim($bodyText), 0, 500)) : null;

        // First sentence text — extract first sentence after removing leading whitespace
        $firstSentenceText = null;
        if ($bodyText) {
            $trimmed = $sanitizeUtf8(trim($bodyText));
            // Match up to first period, question mark, or exclamation followed by space or end
            if (preg_match('/^(.+?[.!?])(?:\s|$)/', $trimmed, $fsMatch)) {
                $firstSentenceText = substr(trim($fsMatch[1]), 0, 500);
            } else {
                // No sentence-ending punctuation found — take first 200 chars
                $firstSentenceText = substr($trimmed, 0, 200);
            }
        }

        // Image count — count <img> tags in body
        $imageCount = 0;
        $imgNodes = $xpath->query('//body//img');
        if ($imgNodes) {
            $imageCount = $imgNodes->length;
        }

        // Internal link count — body-only links (not nav/footer), counted after dedup
        // (calculated below after internalLinks array is built)

        // Last modified date — check HTTP headers and meta tags
        $lastModifiedDate = null;
        // Try <meta property="article:modified_time"> or <meta name="last-modified">
        $modifiedMeta = $xpath->query('//meta[@property="article:modified_time"]/@content');
        if ($modifiedMeta->length > 0) {
            $lastModifiedDate = $modifiedMeta->item(0)->nodeValue;
        }
        if (!$lastModifiedDate) {
            $modifiedMeta = $xpath->query('//meta[@property="og:updated_time"]/@content');
            if ($modifiedMeta->length > 0) {
                $lastModifiedDate = $modifiedMeta->item(0)->nodeValue;
            }
        }
        // Try <time> element with datetime attribute
        if (!$lastModifiedDate) {
            $timeNodes = $xpath->query('//time[@datetime]/@datetime');
            if ($timeNodes->length > 0) {
                $lastModifiedDate = $timeNodes->item(0)->nodeValue;
            }
        }
        // Normalize to date string
        if ($lastModifiedDate) {
            $ts = strtotime($lastModifiedDate);
            $lastModifiedDate = $ts ? date('Y-m-d', $ts) : null;
        }

        // Has FAQ section — detect FAQ blocks
        $hasFaqSection = false;
        $faqIndicators = $xpath->query('//*[contains(@class,"faq") or contains(@id,"faq")]');
        if ($faqIndicators->length > 0) {
            $hasFaqSection = true;
        }
        // Also check for FAQPage schema
        if (!$hasFaqSection && !empty($schemaTypes) && in_array('FAQPage', $schemaTypes)) {
            $hasFaqSection = true;
        }
        // Also check for H2s containing "FAQ" or "Frequently Asked"
        if (!$hasFaqSection) {
            foreach ($h2s as $h2) {
                if (stripos($h2, 'faq') !== false || stripos($h2, 'frequently asked') !== false) {
                    $hasFaqSection = true;
                    break;
                }
            }
        }

        // Has product image — check for img in top portion of body
        $hasProductImage = false;
        $mainImages = $xpath->query('//main//img[@src] | //article//img[@src] | //*[@class="entry-content"]//img[@src]');
        if ($mainImages && $mainImages->length > 0) {
            $hasProductImage = true;
        }

        // ── Central entity check — multiple variants, minimum 2 occurrences for confidence ──
        $bodyLower          = strtolower($bodyText);
        $centralEntityCount = 0;
        foreach ($this->centralEntityVariants as $variant) {
            $centralEntityCount += substr_count($bodyLower, $variant);
        }
        $hasCentralEntity = $centralEntityCount >= 2; // Require at least 2 mentions

        // ── Internal links + core link detection — BODY ONLY (excludes nav/footer) ──
        $internalLinks  = [];
        $coreLinksFound = [];
        $hasCoreLink    = false;

        // Only check links within main content areas, not nav/header/footer
        $contentLinkSelectors = ['//main//a[@href]', '//article//a[@href]',
                                  '//*[@class="entry-content"]//a[@href]',
                                  '//*[@id="content"]//a[@href]'];
        $linkNodes = null;
        foreach ($contentLinkSelectors as $sel) {
            $nodes = $xpath->query($sel);
            if ($nodes->length > 0) {
                $linkNodes = $nodes;
                break;
            }
        }

        // Fallback to all body links if no main content found
        if ($linkNodes === null || $linkNodes->length === 0) {
            $linkNodes = $xpath->query('//body//a[@href]');
        }

        if ($linkNodes) {
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
        }

        $internalLinks  = array_values(array_unique($internalLinks));
        $coreLinksFound = array_values(array_unique($coreLinksFound));
        $internalLinkCount = count($internalLinks);

        // ── H1 vs title alignment — stricter: requires 70% overlap AND checks key noun match ──
        $h1MatchesTitle = false;
        if ($h1 && $titleTag) {
            $stopWords  = ['a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 'is', 'are', 'was', 'be'];
            $h1Clean    = strtolower(preg_replace('/[^a-z0-9 ]/i', '', $h1));
            $titleClean = strtolower(preg_replace('/[^a-z0-9 ]/i', '', $titleTag));

            $h1Words    = array_filter(explode(' ', $h1Clean), fn($w) => strlen($w) > 3 && !in_array($w, $stopWords));
            $titleWords = array_filter(explode(' ', $titleClean), fn($w) => strlen($w) > 3 && !in_array($w, $stopWords));

            if (count($h1Words) > 0 && count($titleWords) > 0) {
                $matches = count(array_filter($h1Words, fn($w) => str_contains($titleClean, $w)));
                $overlap = $matches / count($h1Words);
                // 70% threshold (stricter than original 60%)
                $h1MatchesTitle = $overlap >= 0.70;
            }
        }

        // ── Page type classification ──
        $pageType = $this->classifyPageType($path);

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
            'page_type'            => $pageType,
            'is_utility'           => $this->isUtilityUrl($path) ? 1 : 0,
            'crawled_at'           => date('Y-m-d H:i:s'),
            // New Tier 1 & 2 fields
            'internal_link_count'  => $internalLinkCount,
            'body_text_snippet'    => $bodyTextSnippet,
            'first_sentence_text'  => $firstSentenceText,
            'image_count'          => $imageCount,
            'last_modified_date'   => $lastModifiedDate,
            'has_faq_section'      => $hasFaqSection ? 1 : 0,
            'has_product_image'    => $hasProductImage ? 1 : 0,
        ];
    }

    /**
     * Extract clean text from a DOM node, stripping nested nav/scripts/styles
     */
    private function extractCleanText(\DOMNode $node, \DOMXPath $xpath): string
    {
        // Clone so we don't mutate the original DOM
        $clone = $node->cloneNode(true);
        $cloneDoc = new \DOMDocument();
        $cloneDoc->appendChild($cloneDoc->importNode($clone, true));
        $cloneXpath = new \DOMXPath($cloneDoc);

        $removeSelectors = ['//script', '//style', '//nav', '//header', '//footer',
                            '//noscript', '//*[@aria-hidden="true"]'];
        foreach ($removeSelectors as $sel) {
            $nodes = $cloneXpath->query($sel);
            foreach ($nodes as $n) {
                if ($n->parentNode) $n->parentNode->removeChild($n);
            }
        }

        $text = strip_tags($cloneDoc->textContent ?? '');
        return preg_replace('/\s+/', ' ', $text);
    }

    private function loadOverrides(): array
    {
        try {
            $tables = $this->db->fetchFirstColumn(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'user_overrides'"
            );
            if (empty($tables)) return [];

            $rows = $this->db->fetchAllAssociative('SELECT url, field, override_value FROM user_overrides');
            $overrides = [];
            foreach ($rows as $row) {
                $overrides[$row['url']][$row['field']] = $row['override_value'];
            }
            return $overrides;
        } catch (\Exception $e) {
            return [];
        }
    }

    private function applyOverrides(array $result, array $overrides): array
    {
        $url = $result['url'];
        if (!isset($overrides[$url])) return $result;
        foreach ($overrides[$url] as $field => $value) {
            if (array_key_exists($field, $result)) {
                $result[$field] = $value;
            }
        }
        return $result;
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

        foreach ($this->coreUrls as $core) {
            $paths[] = $core;
        }

        return array_slice(array_unique($paths), 0, $limit);
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

    private function isUtilityUrl(string $path): bool
    {
        $n = '/' . trim($path, '/') . '/';
        foreach ($this->utilityUrls as $util) {
            $u = '/' . trim($util, '/') . '/';
            if ($n === $u || str_starts_with($n, $u)) return true;
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
                    is_utility              BOOLEAN DEFAULT FALSE,
                    internal_link_count     INT DEFAULT 0,
                    body_text_snippet       TEXT DEFAULT NULL,
                    first_sentence_text     TEXT DEFAULT NULL,
                    image_count             INT DEFAULT 0,
                    last_modified_date      DATE DEFAULT NULL,
                    has_faq_section         BOOLEAN DEFAULT FALSE,
                    has_product_image       BOOLEAN DEFAULT FALSE,
                    crawled_at              TIMESTAMP NOT NULL
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_crawl_url ON page_crawl_snapshots (url)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_page_type ON page_crawl_snapshots (page_type)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_has_core_link ON page_crawl_snapshots (has_core_link)');
            $this->db->executeStatement('CREATE INDEX idx_crawl_central_entity ON page_crawl_snapshots (has_central_entity)');
        } else {
            // Add columns if they don't exist yet
            $newCols = [
                'is_utility'          => 'BOOLEAN DEFAULT FALSE',
                'internal_link_count' => 'INT DEFAULT 0',
                'body_text_snippet'   => 'TEXT DEFAULT NULL',
                'first_sentence_text' => 'TEXT DEFAULT NULL',
                'image_count'         => 'INT DEFAULT 0',
                'last_modified_date'  => 'DATE DEFAULT NULL',
                'has_faq_section'     => 'BOOLEAN DEFAULT FALSE',
                'has_product_image'   => 'BOOLEAN DEFAULT FALSE',
            ];
            foreach ($newCols as $col => $def) {
                try {
                    $this->db->executeStatement("ALTER TABLE page_crawl_snapshots ADD COLUMN IF NOT EXISTS {$col} {$def}");
                } catch (\Exception $e) {}
            }
        }
    }
}
    
