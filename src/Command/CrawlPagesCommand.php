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
    private string $baseUrl = 'https://www.doubledtrailers.com';

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
            // Skip media assets — they're not pages and shouldn't be in page signals
            if (str_contains($path, '/wp-content/uploads/') || str_contains($path, '/wp-content/themes/') ||
                preg_match('/\.(jpg|jpeg|png|gif|webp|svg|pdf|mp4|mp3|zip|css|js)$/i', $path)) {
                continue;
            }

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
        
        // Post-crawl analysis (only on full crawls, not single URL)
        if (!$singleUrl && $crawled > 10) {
            $output->writeln("\n--- Running Post-Crawl Analysis ---");
            
            // Detect orphan pages
            $orphanCount = $this->detectOrphanPages($output);
            $output->writeln("Orphan pages detected: {$orphanCount}");
            
            // Detect cannibalization clusters
            $clusterCount = $this->detectCannibalization($output);
            $output->writeln("Cannibalization clusters detected: {$clusterCount}");
            
            // Calculate inbound link counts
            $this->calculateInboundLinks($output);
            $output->writeln("Inbound link counts updated.");
            
            // Calculate crawl depth from homepage (BFS)
            $this->calculateCrawlDepth($output);
            $output->writeln("Crawl depth calculated.");
            
            // Populate sitemap_urls table
            $this->populateSitemapUrls($output);
            $output->writeln("Sitemap URLs indexed.");
            
            // Summary stats
            $this->outputCrawlSummary($output);
        }
        
        return Command::SUCCESS;
    }
    
    /**
     * Calculate crawl depth via BFS from homepage (ILA-007)
     * Depth 0 = homepage, Depth 1 = linked from homepage, etc.
     */
    private function calculateCrawlDepth(OutputInterface $output): void
    {
        // Get all pages with their internal links
        $pages = $this->db->fetchAllAssociative(
            "SELECT url, internal_links FROM page_crawl_snapshots"
        );
        
        // Build link graph
        $linkGraph = [];
        foreach ($pages as $page) {
            $linkGraph[$page['url']] = [];
            if ($page['internal_links']) {
                $links = json_decode($page['internal_links'], true) ?? [];
                $linkGraph[$page['url']] = $links;
            }
        }
        
        // BFS from homepage
        $depths = [];
        $queue = [['/', 0]]; // Start from homepage
        $visited = ['/' => true];
        
        while (!empty($queue)) {
            [$currentUrl, $depth] = array_shift($queue);
            $depths[$currentUrl] = $depth;
            
            // Get links from this page
            $links = $linkGraph[$currentUrl] ?? [];
            foreach ($links as $link) {
                // Normalize link
                $normalizedLink = '/' . trim($link, '/') . '/';
                $normalizedLink = str_replace('//', '/', $normalizedLink);
                
                // Also try without trailing slash
                $variants = [$link, $normalizedLink, '/' . trim($link, '/')];
                
                foreach ($variants as $variant) {
                    if (!isset($visited[$variant]) && isset($linkGraph[$variant])) {
                        $visited[$variant] = true;
                        $queue[] = [$variant, $depth + 1];
                        break;
                    }
                }
            }
        }
        
        // Update crawl_depth for all pages
        foreach ($depths as $url => $depth) {
            $this->db->executeStatement(
                "UPDATE page_crawl_snapshots SET crawl_depth = ? WHERE url = ?",
                [$depth, $url]
            );
        }
        
        // Mark unreachable pages (not found in BFS) with NULL depth
        $output->writeln("  Crawl depth assigned to " . count($depths) . " pages");
        
        // Count pages at each depth level
        $depthCounts = $this->db->fetchAllKeyValue(
            "SELECT crawl_depth, COUNT(*) FROM page_crawl_snapshots WHERE crawl_depth IS NOT NULL GROUP BY crawl_depth ORDER BY crawl_depth"
        );
        foreach ($depthCounts as $d => $c) {
            $output->writeln("    Depth {$d}: {$c} pages");
        }
    }
    
    /**
     * Populate sitemap_urls table from sitemap.xml (TECH-R4)
     */
    private function populateSitemapUrls(OutputInterface $output): void
    {
        // Clear old sitemap data
        $this->db->executeStatement('DELETE FROM sitemap_urls');
        
        $sitemapUrl = $this->baseUrl . '/sitemap_index.xml';
        $context = stream_context_create([
            'http' => [
                'timeout' => 10,
                'header'  => "User-Agent: LogiriBot/1.0\r\n",
            ],
        ]);
        
        $indexXml = @file_get_contents($sitemapUrl, false, $context);
        if ($indexXml === false) {
            // Try sitemap.xml
            $sitemapUrl = $this->baseUrl . '/sitemap.xml';
            $indexXml = @file_get_contents($sitemapUrl, false, $context);
            if ($indexXml === false) {
                $output->writeln("  Could not fetch sitemap");
                return;
            }
        }
        
        $sitemapUrls = [];
        $subSitemaps = [];
        
        // Parse index sitemap
        if (preg_match_all('/<loc>([^<]+)<\/loc>/i', $indexXml, $matches)) {
            foreach ($matches[1] as $loc) {
                if (str_contains($loc, 'sitemap') && str_ends_with($loc, '.xml')) {
                    $subSitemaps[] = $loc;
                } else {
                    $sitemapUrls[] = ['url' => $loc, 'sitemap' => $sitemapUrl];
                }
            }
        }
        
        // Fetch sub-sitemaps
        foreach ($subSitemaps as $subUrl) {
            $subXml = @file_get_contents($subUrl, false, $context);
            if ($subXml === false) continue;
            
            if (preg_match_all('/<url>(.*?)<\/url>/is', $subXml, $urlMatches)) {
                foreach ($urlMatches[1] as $urlBlock) {
                    $url = null;
                    $lastmod = null;
                    $changefreq = null;
                    $priority = null;
                    
                    if (preg_match('/<loc>([^<]+)<\/loc>/i', $urlBlock, $m)) {
                        $url = $m[1];
                    }
                    if (preg_match('/<lastmod>([^<]+)<\/lastmod>/i', $urlBlock, $m)) {
                        $lastmod = $m[1];
                    }
                    if (preg_match('/<changefreq>([^<]+)<\/changefreq>/i', $urlBlock, $m)) {
                        $changefreq = $m[1];
                    }
                    if (preg_match('/<priority>([^<]+)<\/priority>/i', $urlBlock, $m)) {
                        $priority = $m[1];
                    }
                    
                    if ($url) {
                        $sitemapUrls[] = [
                            'url' => $url,
                            'sitemap' => $subUrl,
                            'lastmod' => $lastmod,
                            'changefreq' => $changefreq,
                            'priority' => $priority,
                        ];
                    }
                }
            }
            usleep(100000);
        }
        
        // Insert into sitemap_urls table
        $inserted = 0;
        foreach ($sitemapUrls as $entry) {
            $path = parse_url($entry['url'], PHP_URL_PATH) ?? $entry['url'];
            try {
                $this->db->insert('sitemap_urls', [
                    'url' => $path,
                    'sitemap_source' => $entry['sitemap'] ?? null,
                    'lastmod' => isset($entry['lastmod']) ? date('Y-m-d', strtotime($entry['lastmod'])) : null,
                    'changefreq' => $entry['changefreq'] ?? null,
                    'priority' => $entry['priority'] ?? null,
                    'fetched_at' => date('Y-m-d H:i:s'),
                ]);
                $inserted++;
            } catch (\Exception $e) {
                // Duplicate or other error, skip
            }
        }
        
        // Update in_sitemap flag on page_crawl_snapshots
        $this->db->executeStatement("
            UPDATE page_crawl_snapshots p
            SET in_sitemap = TRUE
            WHERE EXISTS (SELECT 1 FROM sitemap_urls s WHERE s.url = p.url)
        ");
        
        // PostgreSQL-specific alternative if the above doesn't work:
        $this->db->executeStatement("
            UPDATE page_crawl_snapshots 
            SET in_sitemap = 1 
            WHERE url IN (SELECT url FROM sitemap_urls)
        ");
        
        $output->writeln("  Indexed {$inserted} URLs from sitemap");
    }
    
    /**
     * Detect orphan pages (pages with zero inbound internal links)
     */
    private function detectOrphanPages(OutputInterface $output): int
    {
        // Clear old orphan data
        $this->db->executeStatement('DELETE FROM orphan_pages');
        
        // Get all crawled URLs
        $allUrls = $this->db->fetchAllAssociative(
            "SELECT url, title_tag, word_count FROM page_crawl_snapshots WHERE is_noindex = FALSE"
        );
        
        // Build a map of all internal links pointing to each URL
        $inboundLinks = [];
        $allLinksData = $this->db->fetchAllAssociative(
            "SELECT url, internal_links FROM page_crawl_snapshots WHERE internal_links IS NOT NULL"
        );
        
        foreach ($allLinksData as $row) {
            $links = json_decode($row['internal_links'], true) ?: [];
            foreach ($links as $link) {
                $normalized = '/' . trim($link, '/') . '/';
                $normalized = str_replace('//', '/', $normalized);
                if (!isset($inboundLinks[$normalized])) {
                    $inboundLinks[$normalized] = 0;
                }
                $inboundLinks[$normalized]++;
            }
        }
        
        // Find orphans (excluding homepage and utility pages)
        $orphanCount = 0;
        foreach ($allUrls as $row) {
            $url = $row['url'];
            $normalized = '/' . trim($url, '/') . '/';
            $normalized = str_replace('//', '/', $normalized);
            
            // Skip homepage
            if ($normalized === '/' || $normalized === '//') continue;
            
            // Skip utility pages
            if ($this->isUtilityUrl($url)) continue;
            
            $inbound = $inboundLinks[$normalized] ?? 0;
            
            if ($inbound === 0) {
                // Determine recommendation
                $recommendation = 'link_from_nav';
                if ($row['word_count'] < 200) {
                    $recommendation = 'noindex_or_consolidate';
                }
                
                $this->db->insert('orphan_pages', [
                    'url'            => $url,
                    'title'          => $row['title_tag'],
                    'word_count'     => $row['word_count'],
                    'recommendation' => $recommendation,
                    'detected_at'    => date('Y-m-d H:i:s'),
                ]);
                $orphanCount++;
            }
            
            // Update inbound_link_count in page_crawl_snapshots
            try {
                $this->db->executeStatement(
                    "UPDATE page_crawl_snapshots SET inbound_link_count = ? WHERE url = ?",
                    [$inbound, $url]
                );
            } catch (\Exception $e) {}
        }
        
        return $orphanCount;
    }
    
    /**
     * Detect keyword cannibalization clusters
     */
    private function detectCannibalization(OutputInterface $output): int
    {
        // Clear old clusters
        $this->db->executeStatement('DELETE FROM cannibalization_clusters');
        
        // Get all page URLs and titles
        $pages = $this->db->fetchAllAssociative(
            "SELECT url, title_tag, h1 FROM page_crawl_snapshots WHERE is_noindex = FALSE AND is_utility = FALSE"
        );
        
        // Extract target keywords from URLs and titles
        $keywordToPages = [];
        $stopWords = ['a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 
                      'is', 'are', 'was', 'be', 'your', 'you', 'our', 'we', 'how', 'what', 'why', 'double', 'd', 'trailers'];
        
        foreach ($pages as $page) {
            $url = $page['url'];
            
            // Extract keywords from URL slug
            $slug = trim($url, '/');
            $slugWords = preg_split('/[-_\/]/', $slug);
            $slugWords = array_filter($slugWords, fn($w) => strlen($w) > 2 && !in_array(strtolower($w), $stopWords));
            
            // Build keyword phrases (2-3 word combinations)
            $keywords = [];
            
            // Single important words
            foreach ($slugWords as $word) {
                if (strlen($word) > 4) {
                    $keywords[] = strtolower($word);
                }
            }
            
            // Two-word phrases from URL
            for ($i = 0; $i < count($slugWords) - 1; $i++) {
                $phrase = strtolower($slugWords[$i] . ' ' . $slugWords[$i + 1]);
                if (strlen($phrase) > 8) {
                    $keywords[] = $phrase;
                }
            }
            
            // Map keywords to pages
            foreach ($keywords as $kw) {
                if (!isset($keywordToPages[$kw])) {
                    $keywordToPages[$kw] = [];
                }
                if (!in_array($url, $keywordToPages[$kw])) {
                    $keywordToPages[$kw][] = $url;
                }
            }
        }
        
        // Find clusters (keywords with 2+ pages)
        $clusterCount = 0;
        foreach ($keywordToPages as $keyword => $urls) {
            if (count($urls) >= 2) {
                // Determine severity
                $severity = 'low';
                if (count($urls) >= 4) $severity = 'critical';
                elseif (count($urls) >= 3) $severity = 'high';
                elseif (count($urls) >= 2) $severity = 'medium';
                
                // Generate recommendation
                $recommendation = "Consolidate into strongest page or differentiate intent. Pages: " . implode(', ', array_slice($urls, 0, 5));
                
                $this->db->insert('cannibalization_clusters', [
                    'cluster_name'   => 'cluster_' . ($clusterCount + 1),
                    'target_keyword' => $keyword,
                    'page_count'     => count($urls),
                    'pages'          => json_encode($urls),
                    'severity'       => $severity,
                    'recommendation' => $recommendation,
                    'created_at'     => date('Y-m-d H:i:s'),
                ]);
                $clusterCount++;
            }
        }
        
        return $clusterCount;
    }
    
    /**
     * Calculate inbound link counts for all pages
     */
    private function calculateInboundLinks(OutputInterface $output): void
    {
        // Already done in detectOrphanPages, but this ensures it runs even if orphan detection fails
    }
    
    /**
     * Output crawl summary statistics
     */
    private function outputCrawlSummary(OutputInterface $output): void
    {
        $output->writeln("\n--- Crawl Summary ---");
        
        // Content category breakdown
        $categories = $this->db->fetchAllAssociative(
            "SELECT content_category, COUNT(*) as cnt FROM page_crawl_snapshots GROUP BY content_category ORDER BY cnt DESC"
        );
        $output->writeln("Content Categories:");
        foreach ($categories as $cat) {
            $output->writeln("  {$cat['content_category']}: {$cat['cnt']} pages");
        }
        
        // Page score distribution
        $scoreRanges = $this->db->fetchAssociative(
            "SELECT 
                COUNT(*) FILTER (WHERE page_score >= 70) as good,
                COUNT(*) FILTER (WHERE page_score >= 40 AND page_score < 70) as medium,
                COUNT(*) FILTER (WHERE page_score < 40) as poor
             FROM page_crawl_snapshots"
        );
        $output->writeln("Page Scores: Good (70+): {$scoreRanges['good']} | Medium (40-69): {$scoreRanges['medium']} | Poor (<40): {$scoreRanges['poor']}");
        
        // Critical issues
        $noH1 = $this->db->fetchOne("SELECT COUNT(*) FROM page_crawl_snapshots WHERE h1 IS NULL AND is_utility = FALSE");
        $multiH1 = $this->db->fetchOne("SELECT COUNT(*) FROM page_crawl_snapshots WHERE h1_count > 1");
        $thin = $this->db->fetchOne("SELECT COUNT(*) FROM page_crawl_snapshots WHERE content_category = 'thin'");
        $noSchema = $this->db->fetchOne("SELECT COUNT(*) FROM page_crawl_snapshots WHERE schema_types = '[]' AND page_type = 'core'");
        
        $output->writeln("Critical Issues:");
        $output->writeln("  Missing H1: {$noH1}");
        $output->writeln("  Multiple H1s: {$multiH1}");
        $output->writeln("  Thin content: {$thin}");
        $output->writeln("  Core pages without schema: {$noSchema}");
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
        // Also validates required fields per schema type
        $schemaNodes = $xpath->query('//*[@type="application/ld+json"]');
        $schemaTypes = [];
        $schemaErrors = [];
        foreach ($schemaNodes as $node) {
            $json = json_decode($node->textContent, true);
            if (!$json) {
                $schemaErrors[] = 'Invalid JSON-LD: parse error';
                continue;
            }

            // Collect all schema items (top-level and @graph)
            $items = [];
            if (isset($json['@type'])) {
                $items[] = $json;
                $schemaTypes[] = is_array($json['@type']) ? implode(',', $json['@type']) : $json['@type'];
            }
            if (isset($json['@graph']) && is_array($json['@graph'])) {
                foreach ($json['@graph'] as $item) {
                    if (isset($item['@type'])) {
                        $items[] = $item;
                        $schemaTypes[] = is_array($item['@type']) ? implode(',', $item['@type']) : $item['@type'];
                    }
                }
            }

            // Validate required fields per schema type
            foreach ($items as $item) {
                $type = is_array($item['@type']) ? $item['@type'][0] : ($item['@type'] ?? '');

                if ($type === 'Product') {
                    if (empty($item['name'])) $schemaErrors[] = 'Product: missing "name"';
                    if (empty($item['image'])) $schemaErrors[] = 'Product: missing "image"';
                    if (empty($item['brand']['name'] ?? null) && empty($item['brand'] ?? null)) $schemaErrors[] = 'Product: missing "brand.name"';
                    if (isset($item['image']) && is_string($item['image']) && !filter_var($item['image'], FILTER_VALIDATE_URL)) {
                        $schemaErrors[] = 'Product: invalid URL in "image": ' . substr($item['image'], 0, 80);
                    }
                    if (isset($item['aggregateRating'])) {
                        $ar = $item['aggregateRating'];
                        if (empty($ar['ratingValue'])) $schemaErrors[] = 'Product: missing "aggregateRating.ratingValue"';
                        if (empty($ar['ratingCount'] ?? null) && empty($ar['reviewCount'] ?? null)) {
                            $schemaErrors[] = 'Product: missing "aggregateRating.ratingCount" or "reviewCount"';
                        }
                    }
                }

                if ($type === 'Organization') {
                    if (empty($item['name'])) $schemaErrors[] = 'Organization: missing "name"';
                    if (empty($item['url'])) $schemaErrors[] = 'Organization: missing "url"';
                    if (isset($item['logo']) && is_string($item['logo']) && !filter_var($item['logo'], FILTER_VALIDATE_URL)) {
                        $schemaErrors[] = 'Organization: invalid URL in "logo": ' . substr($item['logo'], 0, 80);
                    }
                    if (isset($item['sameAs']) && is_array($item['sameAs'])) {
                        foreach ($item['sameAs'] as $sa) {
                            if (!filter_var($sa, FILTER_VALIDATE_URL)) {
                                $schemaErrors[] = 'Organization: invalid sameAs URL: ' . substr($sa, 0, 80);
                            }
                        }
                    }
                }

                if ($type === 'LocalBusiness') {
                    if (empty($item['name'])) $schemaErrors[] = 'LocalBusiness: missing "name"';
                    if (empty($item['telephone'])) $schemaErrors[] = 'LocalBusiness: missing "telephone"';
                    if (empty($item['address']['streetAddress'] ?? null)) $schemaErrors[] = 'LocalBusiness: missing "address.streetAddress"';
                }

                if ($type === 'FAQPage') {
                    if (empty($item['mainEntity']) || !is_array($item['mainEntity'])) {
                        $schemaErrors[] = 'FAQPage: missing or empty "mainEntity" array';
                    }
                }
            }
        }
        $schemaTypes = array_values(array_unique($schemaTypes));
        $schemaErrors = array_values(array_unique($schemaErrors));

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
            // Remove nav, header, footer, aside, scripts, styles, forms, breadcrumbs
            $noiseSelectors = ['//nav', '//header', '//footer', '//aside',
                               '//script', '//style', '//noscript',
                               '//*[@class="menu"]', '//*[@id="menu"]',
                               '//*[@class="navigation"]', '//*[@role="navigation"]',
                               '//*[@class="sidebar"]', '//*[@id="sidebar"]',
                               '//*[@class="widget"]', '//*[contains(@class,"cookie")]',
                               // Breadcrumb selectors - comprehensive coverage
                               '//*[@aria-label="breadcrumb"]',
                               '//*[@aria-label="Breadcrumb"]',
                               '//*[contains(@class,"breadcrumb")]',
                               '//*[contains(@class,"Breadcrumb")]',
                               '//*[contains(@class,"bread-crumb")]',
                               '//*[contains(@id,"breadcrumb")]',
                               '//*[@role="navigation" and contains(@aria-label,"bread")]',
                               '//ol[contains(@class,"breadcrumb")]',
                               '//ul[contains(@class,"breadcrumb")]',
                               '//nav[contains(@class,"breadcrumb")]',
                               '//*[@itemtype="https://schema.org/BreadcrumbList"]',
                               '//*[@itemtype="http://schema.org/BreadcrumbList"]'];
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
        $bodyTextSnippet = $bodyText ? $sanitizeUtf8(substr(trim($bodyText), 0, 5000)) : null;

        // First sentence text — extract first sentence after removing leading whitespace
        $firstSentenceText = null;
        if ($bodyText) {
            $trimmed = $sanitizeUtf8(trim($bodyText));
            
            // Strip common breadcrumb patterns from the start of text
            // Pattern: "Home > Category > Page" or "Home / Category / Page" or "Home » Page"
            $trimmed = preg_replace('/^(Home\s*[>\/»›→]\s*)+/i', '', $trimmed);
            // Pattern: site name at start followed by separator
            $trimmed = preg_replace('/^Double\s*D\s*Trailers?\s*[>\/»›→]\s*/i', '', $trimmed);
            // Pattern: generic "You are here:" prefix
            $trimmed = preg_replace('/^You\s+are\s+here:?\s*/i', '', $trimmed);
            // Pattern: multiple short words separated by > / » (breadcrumb chain)
            $trimmed = preg_replace('/^(\w{1,20}\s*[>\/»›→]\s*){2,}/u', '', $trimmed);
            $trimmed = trim($trimmed);
            
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

        // ── H1 count (multiple H1s is an issue) ──
        $h1Count = $h1Nodes->length;

        // ── Meta description and title length ──
        $metaDescLength = $metaDescription ? strlen($metaDescription) : 0;
        $titleLength = $titleTag ? strlen($titleTag) : 0;

        // ── Product links detection (links to /product/ or known product URLs) ──
        $productLinksCount = 0;
        $productPatterns = ['/safetack', '/trail-blazer', '/bumper-pull-2-horse', '/gooseneck-2-horse', 
                           '/gooseneck-3-horse', '/gooseneck-4-horse', '/one-horse-trailer',
                           '/living-quarters', '/v-sport', '/townsmand'];
        foreach ($internalLinks as $link) {
            foreach ($productPatterns as $pattern) {
                if (str_contains($link, $pattern)) {
                    $productLinksCount++;
                    break;
                }
            }
        }

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
        
        // ── Enhanced content category (Perplexity-style pyramid) ──
        $contentCategory = $this->classifyContentCategory($path, $wordCount, $pageType, $schemaTypes, $hasCentralEntity);
        
        // ── Page score (0-100) ──
        $pageScore = $this->calculatePageScore(
            $h1, $h1Count, $h1MatchesTitle, $metaDescription, $metaDescLength, 
            $wordCount, $schemaTypes, $hasCentralEntity, $internalLinkCount, 
            $imageCount, $hasFaqSection, $pageType, $contentCategory
        );

        // ══════════════════════════════════════════════════════════════════
        // NEW TIER C RULE EXTRACTIONS
        // ══════════════════════════════════════════════════════════════════
        
        // ── CTA link detection (CTA-F2) ──
        $hasCtaLink = false;
        $ctaPatterns = ['/quote', '/get-quote', '/contact', '/contact-us', '/book-a-video', '/trailer-finder'];
        foreach ($internalLinks as $link) {
            foreach ($ctaPatterns as $pattern) {
                if (str_contains($link, $pattern)) {
                    $hasCtaLink = true;
                    break 2;
                }
            }
        }
        
        // ── HowTo schema detection (SCH-007, CI-02) ──
        $hasHowtoSchema = in_array('HowTo', $schemaTypes);
        
        // ── Video embed detection with MAO-R4 three-gate system ──
        $hasVideoEmbed = false;
        $hasMainContentVideo = false;
        $videoMetadataValid = false;
        $videoTopicAligned = false;
        $videoUrls = [];
        $videoThumbnailUrl = null;
        $videoDurationSeconds = null;
        $videoUploadDate = null;
        $videoTitle = null;

        // GATE 1: Check for video embeds specifically in main content area
        $mainContentSelectors = [
            '//main//iframe[contains(@src,"youtube") or contains(@src,"vimeo") or contains(@src,"wistia")]/@src',
            '//article//iframe[contains(@src,"youtube") or contains(@src,"vimeo") or contains(@src,"wistia")]/@src',
            '//*[contains(@class,"entry-content") or contains(@class,"post-content") or contains(@class,"page-content") or contains(@class,"content-area") or contains(@id,"content")]//iframe[contains(@src,"youtube") or contains(@src,"vimeo") or contains(@src,"wistia")]/@src',
            '//main//video/@src',
            '//article//video/@src',
        ];
        // Also check anywhere on page (for has_video_embed general flag)
        $anyVideoSelectors = [
            '//iframe[contains(@src,"youtube") or contains(@src,"vimeo") or contains(@src,"wistia")]/@src',
            '//video/@src',
            '//*[@data-video-id]/@data-video-id',
        ];

        // Check main content first
        foreach ($mainContentSelectors as $sel) {
            $vidNodes = @$xpath->query($sel);
            if ($vidNodes && $vidNodes->length > 0) {
                $hasMainContentVideo = true;
                $hasVideoEmbed = true;
                foreach ($vidNodes as $v) {
                    $videoUrls[] = $v->nodeValue;
                }
            }
        }
        // Fallback: check anywhere
        if (!$hasVideoEmbed) {
            foreach ($anyVideoSelectors as $sel) {
                $vidNodes = @$xpath->query($sel);
                if ($vidNodes && $vidNodes->length > 0) {
                    $hasVideoEmbed = true;
                    foreach ($vidNodes as $v) {
                        $videoUrls[] = $v->nodeValue;
                    }
                }
            }
        }

        // GATE 2: YouTube oEmbed API pre-flight (only for main content videos)
        if ($hasMainContentVideo && !empty($videoUrls)) {
            foreach ($videoUrls as $vUrl) {
                // Extract YouTube video URL
                $ytId = null;
                if (preg_match('/youtube\.com\/embed\/([a-zA-Z0-9_-]+)/', $vUrl, $ym)) {
                    $ytId = $ym[1];
                } elseif (preg_match('/youtu\.be\/([a-zA-Z0-9_-]+)/', $vUrl, $ym)) {
                    $ytId = $ym[1];
                }
                if (!$ytId) continue;

                $oembedUrl = "https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={$ytId}&format=json";
                $ctx = stream_context_create(['http' => ['timeout' => 5, 'ignore_errors' => true]]);
                $oembedJson = @file_get_contents($oembedUrl, false, $ctx);
                if ($oembedJson) {
                    $oembed = json_decode($oembedJson, true);
                    if (!empty($oembed['title']) && !empty($oembed['thumbnail_url'])) {
                        $videoMetadataValid = true;
                        $videoTitle = $oembed['title'] ?? null;
                        $videoThumbnailUrl = $oembed['thumbnail_url'] ?? null;
                        // oEmbed doesn't provide duration/uploadDate — those need YouTube Data API
                        // For now, mark metadata as valid if we have title + thumbnail
                        break; // Use first valid video
                    }
                }
            }
        }

        // GATE 3: Topic alignment check (video title vs page H1)
        if ($videoTitle && $h1) {
            $videoWords = array_unique(array_filter(
                preg_split('/\s+/', strtolower(preg_replace('/[^a-z0-9\s]/i', '', $videoTitle))),
                fn($w) => strlen($w) > 3 // skip short words
            ));
            $h1Words = array_unique(array_filter(
                preg_split('/\s+/', strtolower(preg_replace('/[^a-z0-9\s]/i', '', $h1))),
                fn($w) => strlen($w) > 3
            ));
            $overlap = count(array_intersect($videoWords, $h1Words));
            $videoTopicAligned = $overlap >= 2;
        }
        
        // ── Author byline detection (DDT-EEAT-05) ──
        $hasAuthorByline = false;
        $authorName = null;
        $authorSelectors = [
            '//*[contains(@class,"author")]//a/text()',
            '//*[contains(@class,"byline")]//text()',
            '//meta[@name="author"]/@content',
            '//*[@rel="author"]/text()',
        ];
        foreach ($authorSelectors as $sel) {
            $authorNodes = $xpath->query($sel);
            if ($authorNodes && $authorNodes->length > 0) {
                $hasAuthorByline = true;
                $authorName = trim($authorNodes->item(0)->nodeValue);
                break;
            }
        }
        
        // ── Proprietary brand term detection (ETA-002, AIS-003, OPQ-R7) ──
        $hasZframeMention = str_contains($bodyLower, 'z-frame') || str_contains($bodyLower, 'zframe') || str_contains($bodyLower, 'z frame');
        $hasSafetackMention = str_contains($bodyLower, 'safetack') || str_contains($bodyLower, 'safe-tack') || str_contains($bodyLower, 'safe tack');
        $hasSafebumpMention = str_contains($bodyLower, 'safebump') || str_contains($bodyLower, 'safe-bump') || str_contains($bodyLower, 'safe bump');
        $hasSafekickMention = str_contains($bodyLower, 'safekick') || str_contains($bodyLower, 'safe-kick') || str_contains($bodyLower, 'safe kick');
        
        $proprietaryTermCount = 0;
        if ($hasZframeMention) $proprietaryTermCount++;
        if ($hasSafetackMention) $proprietaryTermCount++;
        if ($hasSafebumpMention) $proprietaryTermCount++;
        if ($hasSafekickMention) $proprietaryTermCount++;
        
        // ── Z-Frame definition detection (AIS-003) ──
        // Check for "Z-Frame is..." or "Z-Frame, a..." pattern
        $hasZframeDefinition = false;
        if ($hasZframeMention) {
            $defPatterns = [
                '/z-?frame\s+(is|are|refers?\s+to|means?|represents?)\s/i',
                '/z-?frame[,:]?\s+(a|an|the)\s/i',
            ];
            foreach ($defPatterns as $pat) {
                if (preg_match($pat, $bodyText)) {
                    $hasZframeDefinition = true;
                    break;
                }
            }
        }
        
        // ── Question-format H2 detection (AIS-008) ──
        $hasQuestionH2 = false;
        $questionH2Count = 0;
        foreach ($h2s as $h2) {
            if (preg_match('/^(what|why|how|when|where|who|which|can|do|does|is|are|should|will|would)\s/i', $h2) 
                || str_contains($h2, '?')) {
                $hasQuestionH2 = true;
                $questionH2Count++;
            }
        }
        
        // ── Image analysis (MAO-01, MAO-02, CWV-R7) ──
        $imagesWithoutAlt = 0;
        $imagesWithGenericAlt = 0;
        $hasLazyHeroImage = false;
        $genericAltPatterns = ['image', 'photo', 'picture', 'img', 'dsc', 'untitled', 'screenshot'];
        
        $allImgNodes = $xpath->query('//body//img');
        $imgIndex = 0;
        foreach ($allImgNodes as $img) {
            $imgIndex++;
            $alt = $img->getAttribute('alt');
            $loading = $img->getAttribute('loading');
            
            // Check for missing alt
            if (empty($alt)) {
                $imagesWithoutAlt++;
            } else {
                // Check for generic alt text
                $altLower = strtolower($alt);
                foreach ($genericAltPatterns as $generic) {
                    if ($altLower === $generic || preg_match('/^' . preg_quote($generic, '/') . '\d*$/i', $alt)) {
                        $imagesWithGenericAlt++;
                        break;
                    }
                }
            }
            
            // Check if first/hero image is lazy loaded (bad for LCP)
            if ($imgIndex <= 2 && strtolower($loading) === 'lazy') {
                $hasLazyHeroImage = true;
            }
        }
        
        // ── Mobile viewport detection (CTA-F6) ──
        $mobileViewportSet = false;
        $viewportNodes = $xpath->query('//meta[@name="viewport"]/@content');
        if ($viewportNodes && $viewportNodes->length > 0) {
            $mobileViewportSet = true;
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
            'page_type'            => $pageType,
            'is_utility'           => $this->isUtilityUrl($path) ? 1 : 0,
            'crawled_at'           => date('Y-m-d H:i:s'),
            // Tier 1 & 2 fields
            'internal_link_count'  => $internalLinkCount,
            'body_text_snippet'    => $bodyTextSnippet,
            'first_sentence_text'  => $firstSentenceText,
            'image_count'          => $imageCount,
            'last_modified_date'   => $lastModifiedDate,
            'has_faq_section'      => $hasFaqSection ? 1 : 0,
            'has_product_image'    => $hasProductImage ? 1 : 0,
            'schema_errors'        => !empty($schemaErrors) ? $sanitizeUtf8(json_encode($schemaErrors)) : null,
            // Perplexity-inspired fields
            'content_category'     => $contentCategory,
            'h1_count'             => $h1Count,
            'product_links_count'  => $productLinksCount,
            'meta_desc_length'     => $metaDescLength,
            'title_length'         => $titleLength,
            'page_score'           => $pageScore,
            // NEW: Tier C rule fields
            'has_cta_link'         => $hasCtaLink ? 1 : 0,
            'has_howto_schema'     => $hasHowtoSchema ? 1 : 0,
            'has_video_embed'      => $hasVideoEmbed ? 1 : 0,
            'video_urls'           => !empty($videoUrls) ? json_encode($videoUrls) : null,
            'has_main_content_video' => $hasMainContentVideo ? 1 : 0,
            'video_metadata_valid' => $videoMetadataValid ? 1 : 0,
            'video_topic_aligned'  => $videoTopicAligned ? 1 : 0,
            'video_thumbnail_url'  => $videoThumbnailUrl ? substr($videoThumbnailUrl, 0, 500) : null,
            'video_duration_seconds' => $videoDurationSeconds,
            'video_upload_date'    => $videoUploadDate,
            'video_title'          => $videoTitle ? substr($videoTitle, 0, 500) : null,
            'has_author_byline'    => $hasAuthorByline ? 1 : 0,
            'author_name'          => $authorName ? substr($authorName, 0, 200) : null,
            'has_zframe_mention'   => $hasZframeMention ? 1 : 0,
            'has_zframe_definition' => $hasZframeDefinition ? 1 : 0,
            'has_safetack_mention' => $hasSafetackMention ? 1 : 0,
            'has_safebump_mention' => $hasSafebumpMention ? 1 : 0,
            'has_safekick_mention' => $hasSafekickMention ? 1 : 0,
            'proprietary_term_count' => $proprietaryTermCount,
            'has_question_h2'      => $hasQuestionH2 ? 1 : 0,
            'question_h2_count'    => $questionH2Count,
            'images_without_alt'   => $imagesWithoutAlt,
            'images_with_generic_alt' => $imagesWithGenericAlt,
            'has_lazy_hero_image'  => $hasLazyHeroImage ? 1 : 0,
            'mobile_viewport_set'  => $mobileViewportSet ? 1 : 0,
        ];
    }
    
    /**
     * Classify content into Perplexity-style pyramid categories
     */
    private function classifyContentCategory(string $path, int $wordCount, string $pageType, array $schemaTypes, bool $hasCentralEntity): string
    {
        // Utility pages
        if ($this->isUtilityUrl($path)) {
            return 'utility';
        }
        
        // Macro content: pillar pages, category pages, hub pages (structural backbone)
        $macroPatterns = [
            '/^\/horse-trailer\/?$/',           // Main pillar
            '/^\/gooseneck-horse-trailers\/?$/',
            '/^\/bumper-pull-horse-trailers\/?$/',
            '/^\/living-quarters-horse-trailers\/?$/',
            '/^\/horse-trailer-safety/',
            '/^\/articles\/?$/',
            '/^\/the-horse-trailer-post-podcast\/?$/',
        ];
        foreach ($macroPatterns as $pattern) {
            if (preg_match($pattern, $path)) {
                return 'macro';
            }
        }
        
        // Thin content candidates
        $thinPatterns = ['/therapy/', '/therapeutic/', '/podcast/', '/video/', '/prize-wheel/'];
        foreach ($thinPatterns as $pattern) {
            if (str_contains($path, $pattern)) {
                if ($wordCount < 200) {
                    return 'thin';
                }
            }
        }
        
        // General thin content check
        if ($wordCount < 100) {
            return 'thin';
        }
        
        // Micro content - off-topic (lifestyle, breeds, equestrian not trailer-focused)
        $offTopicPatterns = ['/horse-breed/', '/horse-meme/', '/celebrity/', '/halloween/', '/christmas/'];
        foreach ($offTopicPatterns as $pattern) {
            if (str_contains($path, $pattern)) {
                return 'micro_offtopic';
            }
        }
        
        // If no central entity and not a product page, likely off-topic
        if (!$hasCentralEntity && $pageType !== 'core') {
            return 'micro_offtopic';
        }
        
        // Default: micro_core (product pages, comparisons, trailer-focused blog)
        return 'micro_core';
    }
    
    /**
     * Calculate a 0-100 page score (Perplexity-style)
     */
    private function calculatePageScore(
        ?string $h1, int $h1Count, bool $h1MatchesTitle, ?string $metaDesc, int $metaDescLength,
        int $wordCount, array $schemaTypes, bool $hasCentralEntity, int $internalLinkCount,
        int $imageCount, bool $hasFaqSection, string $pageType, string $contentCategory
    ): int {
        $score = 0;
        
        // H1 presence and quality (20 points max)
        if ($h1) {
            $score += 10;
            if ($h1Count === 1) $score += 5;  // Exactly one H1
            if ($h1MatchesTitle) $score += 5;
        }
        
        // Meta description (15 points max)
        if ($metaDesc) {
            $score += 5;
            if ($metaDescLength >= 120 && $metaDescLength <= 160) $score += 10;
            elseif ($metaDescLength >= 80 && $metaDescLength <= 200) $score += 5;
        }
        
        // Word count (20 points max) - varies by page type
        if ($pageType === 'core' || $contentCategory === 'macro') {
            // Product/category pages need 800-1500 words ideally
            if ($wordCount >= 800 && $wordCount <= 2000) $score += 20;
            elseif ($wordCount >= 500 && $wordCount < 800) $score += 10;
            elseif ($wordCount >= 300 && $wordCount < 500) $score += 5;
        } else {
            // Blog/outer pages can be longer
            if ($wordCount >= 1000 && $wordCount <= 3000) $score += 20;
            elseif ($wordCount >= 500 && $wordCount < 1000) $score += 15;
            elseif ($wordCount >= 300 && $wordCount < 500) $score += 10;
        }
        
        // Schema markup (15 points max)
        if (!empty($schemaTypes)) {
            $score += 5;
            if (in_array('Product', $schemaTypes)) $score += 5;
            if (in_array('FAQPage', $schemaTypes)) $score += 5;
        }
        
        // Central entity present (10 points)
        if ($hasCentralEntity) $score += 10;
        
        // Internal linking (10 points max)
        if ($internalLinkCount >= 2 && $internalLinkCount <= 10) $score += 10;
        elseif ($internalLinkCount >= 1 && $internalLinkCount < 20) $score += 5;
        
        // Images (5 points)
        if ($imageCount >= 1) $score += 5;
        
        // FAQ section (5 points)
        if ($hasFaqSection) $score += 5;
        
        return min(100, $score);
    }

    /**
     * Extract clean text from a DOM node, stripping nested nav/scripts/styles/breadcrumbs
     */
    private function extractCleanText(\DOMNode $node, \DOMXPath $xpath): string
    {
        // Clone so we don't mutate the original DOM
        $clone = $node->cloneNode(true);
        $cloneDoc = new \DOMDocument();
        $cloneDoc->appendChild($cloneDoc->importNode($clone, true));
        $cloneXpath = new \DOMXPath($cloneDoc);

        $removeSelectors = ['//script', '//style', '//nav', '//header', '//footer',
                            '//noscript', '//*[@aria-hidden="true"]',
                            // Breadcrumb removal
                            '//*[contains(@class,"breadcrumb")]',
                            '//*[contains(@id,"breadcrumb")]',
                            '//*[@aria-label="breadcrumb"]',
                            '//*[@aria-label="Breadcrumb"]',
                            '//*[@itemtype="https://schema.org/BreadcrumbList"]',
                            '//*[@itemtype="http://schema.org/BreadcrumbList"]'];
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
        $paths = [];
        
        // 1. First, try to fetch URLs from XML sitemap (most comprehensive source)
        $sitemapUrls = $this->getUrlsFromSitemap();
        if (!empty($sitemapUrls)) {
            $paths = array_merge($paths, $sitemapUrls);
        }
        
        // 2. Add URLs from GSC snapshots (pages with actual impressions)
        $rows = $this->db->fetchAllAssociative(
            "SELECT DISTINCT page FROM gsc_snapshots
             WHERE date_range = '28d' AND page LIKE '%doubledtrailers.com%'
             ORDER BY page LIMIT ?",
            [$limit]
        );
        foreach ($rows as $row) {
            $parsed = parse_url($row['page'], PHP_URL_PATH);
            if ($parsed) $paths[] = $parsed;
        }

        // 3. Add hardcoded core URLs as fallback
        foreach ($this->coreUrls as $core) {
            $paths[] = $core;
        }

        return array_slice(array_unique($paths), 0, $limit);
    }

    /**
     * Fetch all URLs from the site's XML sitemap index
     */
    private function getUrlsFromSitemap(): array
    {
        $paths = [];
        $sitemapIndexUrl = $this->baseUrl . '/sitemap_index.xml';
        
        $context = stream_context_create([
            'http' => [
                'method'  => 'GET',
                'header'  => "User-Agent: Mozilla/5.0 (compatible; LogiriBot/1.0)\r\n",
                'timeout' => 30,
            ],
        ]);
        
        $indexXml = @file_get_contents($sitemapIndexUrl, false, $context);
        if ($indexXml === false) {
            // Try alternate sitemap URL
            $indexXml = @file_get_contents($this->baseUrl . '/sitemap.xml', false, $context);
        }
        
        if ($indexXml === false) return $paths;
        
        // Parse sitemap index to find sub-sitemaps
        $subSitemaps = [];
        if (preg_match_all('/<loc>([^<]+)<\/loc>/i', $indexXml, $matches)) {
            foreach ($matches[1] as $loc) {
                if (str_contains($loc, 'sitemap') && str_ends_with($loc, '.xml')) {
                    $subSitemaps[] = $loc;
                } else {
                    // This is a direct URL in the sitemap
                    $parsed = parse_url($loc, PHP_URL_PATH);
                    if ($parsed) $paths[] = $parsed;
                }
            }
        }
        
        // Fetch each sub-sitemap
        foreach ($subSitemaps as $subUrl) {
            $subXml = @file_get_contents($subUrl, false, $context);
            if ($subXml === false) continue;
            
            if (preg_match_all('/<loc>([^<]+)<\/loc>/i', $subXml, $matches)) {
                foreach ($matches[1] as $loc) {
                    $parsed = parse_url($loc, PHP_URL_PATH);
                    if ($parsed) $paths[] = $parsed;
                }
            }
            usleep(100000); // 100ms delay between sitemap fetches
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
                    schema_errors           TEXT DEFAULT NULL,
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
                'schema_errors'       => 'TEXT DEFAULT NULL',
                // Perplexity-inspired columns
                'content_category'    => "VARCHAR(30) DEFAULT NULL",
                'h1_count'            => 'INT DEFAULT 0',
                'inbound_link_count'  => 'INT DEFAULT 0',
                'has_js_only_links'   => 'BOOLEAN DEFAULT FALSE',
                'product_links_count' => 'INT DEFAULT 0',
                'boilerplate_score'   => 'INT DEFAULT 0',
                'meta_desc_length'    => 'INT DEFAULT 0',
                'title_length'        => 'INT DEFAULT 0',
                'page_score'          => 'INT DEFAULT 0',
                // NEW: Tier C rule support columns
                'crawl_depth'         => 'INT DEFAULT NULL',           // ILA-007: clicks from homepage
                'redirect_chain'      => 'TEXT DEFAULT NULL',          // TECH-R5: redirect hops
                'redirect_count'      => 'INT DEFAULT 0',              // TECH-R5: number of redirects
                'final_url'           => 'TEXT DEFAULT NULL',          // TECH-R5: after redirects
                'has_cta_link'        => 'BOOLEAN DEFAULT FALSE',      // CTA-F2: link to /quote/ or /contact/
                'has_howto_schema'    => 'BOOLEAN DEFAULT FALSE',      // SCH-007, CI-02
                'has_video_embed'     => 'BOOLEAN DEFAULT FALSE',      // SCH-006, MAO-04
                'video_urls'          => 'TEXT DEFAULT NULL',          // SCH-006: YouTube/Vimeo embeds
                'has_main_content_video' => 'BOOLEAN DEFAULT FALSE',  // MAO-R4 Gate 1: video in main content area
                'video_metadata_valid' => 'BOOLEAN DEFAULT FALSE',    // MAO-R4 Gate 2: oEmbed returned title+thumbnail
                'video_topic_aligned' => 'BOOLEAN DEFAULT FALSE',     // MAO-R4 Gate 3: video title overlaps page H1
                'video_thumbnail_url' => 'TEXT DEFAULT NULL',         // MAO-R4: for schema generation
                'video_duration_seconds' => 'INT DEFAULT NULL',       // MAO-R4: for schema generation (needs YT Data API)
                'video_upload_date'   => 'TEXT DEFAULT NULL',         // MAO-R4: ISO 8601 (needs YT Data API)
                'video_title'         => 'TEXT DEFAULT NULL',         // MAO-R4: from oEmbed, for topic alignment
                'has_author_byline'   => 'BOOLEAN DEFAULT FALSE',      // DDT-EEAT-05
                'author_name'         => 'TEXT DEFAULT NULL',          // DDT-EEAT-05
                'has_zframe_mention'  => 'BOOLEAN DEFAULT FALSE',      // AIS-003, OPQ-R7
                'has_zframe_definition' => 'BOOLEAN DEFAULT FALSE',    // AIS-003: "Z-Frame is..."
                'has_safetack_mention'  => 'BOOLEAN DEFAULT FALSE',    // ETA-002
                'has_safebump_mention'  => 'BOOLEAN DEFAULT FALSE',    // ETA-002
                'has_safekick_mention'  => 'BOOLEAN DEFAULT FALSE',    // ETA-002
                'proprietary_term_count' => 'INT DEFAULT 0',           // ETA-002: total brand terms
                'has_question_h2'     => 'BOOLEAN DEFAULT FALSE',      // AIS-008: question-format H2s
                'question_h2_count'   => 'INT DEFAULT 0',              // AIS-008
                'total_image_size_kb' => 'INT DEFAULT NULL',           // MAO-02: aggregate image weight
                'largest_image_kb'    => 'INT DEFAULT NULL',           // MAO-02
                'images_without_alt'  => 'INT DEFAULT 0',              // MAO-01, MAO-02
                'images_with_generic_alt' => 'INT DEFAULT 0',          // MAO-01
                'has_lazy_hero_image' => 'BOOLEAN DEFAULT FALSE',      // CWV-R7
                'mobile_viewport_set' => 'BOOLEAN DEFAULT FALSE',      // CTA-F6
                'in_sitemap'          => 'BOOLEAN DEFAULT FALSE',      // TECH-R4
            ];
            foreach ($newCols as $col => $def) {
                try {
                    $this->db->executeStatement("ALTER TABLE page_crawl_snapshots ADD COLUMN IF NOT EXISTS {$col} {$def}");
                } catch (\Exception $e) {}
            }
        }
        
        // Create cannibalization_clusters table for keyword overlap detection
        if (!in_array('cannibalization_clusters', $tables)) {
            $this->db->executeStatement("
                CREATE TABLE cannibalization_clusters (
                    id              SERIAL PRIMARY KEY,
                    cluster_name    TEXT NOT NULL,
                    target_keyword  TEXT NOT NULL,
                    page_count      INT DEFAULT 0,
                    pages           TEXT DEFAULT NULL,
                    severity        VARCHAR(20) DEFAULT 'medium',
                    recommendation  TEXT DEFAULT NULL,
                    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_cannibal_keyword ON cannibalization_clusters (target_keyword)');
        }
        
        // Create orphan_pages table for tracking pages with zero inbound links
        if (!in_array('orphan_pages', $tables)) {
            $this->db->executeStatement("
                CREATE TABLE orphan_pages (
                    id              SERIAL PRIMARY KEY,
                    url             TEXT NOT NULL,
                    title           TEXT DEFAULT NULL,
                    word_count      INT DEFAULT 0,
                    recommendation  VARCHAR(50) DEFAULT NULL,
                    detected_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_orphan_url ON orphan_pages (url)');
        }
        
        // NEW: Create page_image_assets table for MAO rules
        if (!in_array('page_image_assets', $tables)) {
            $this->db->executeStatement("
                CREATE TABLE page_image_assets (
                    id              SERIAL PRIMARY KEY,
                    page_url        TEXT NOT NULL,
                    image_src       TEXT NOT NULL,
                    alt_text        TEXT DEFAULT NULL,
                    alt_text_length INT DEFAULT 0,
                    file_size_kb    INT DEFAULT NULL,
                    image_format    VARCHAR(20) DEFAULT NULL,
                    width           INT DEFAULT NULL,
                    height          INT DEFAULT NULL,
                    is_lazy_loaded  BOOLEAN DEFAULT FALSE,
                    is_hero_image   BOOLEAN DEFAULT FALSE,
                    has_brand_term  BOOLEAN DEFAULT FALSE,
                    crawled_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_image_page ON page_image_assets (page_url)');
            $this->db->executeStatement('CREATE INDEX idx_image_src ON page_image_assets (image_src)');
        }
        
        // NEW: Create redirect_log table for TECH-R5
        if (!in_array('redirect_log', $tables)) {
            $this->db->executeStatement("
                CREATE TABLE redirect_log (
                    id              SERIAL PRIMARY KEY,
                    source_url      TEXT NOT NULL,
                    final_url       TEXT NOT NULL,
                    redirect_count  INT DEFAULT 0,
                    redirect_chain  TEXT DEFAULT NULL,
                    status_codes    TEXT DEFAULT NULL,
                    detected_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_redirect_source ON redirect_log (source_url)');
        }
        
        // NEW: Create sitemap_urls table for TECH-R4
        if (!in_array('sitemap_urls', $tables)) {
            $this->db->executeStatement("
                CREATE TABLE sitemap_urls (
                    id              SERIAL PRIMARY KEY,
                    url             TEXT NOT NULL UNIQUE,
                    sitemap_source  TEXT DEFAULT NULL,
                    lastmod         DATE DEFAULT NULL,
                    changefreq      VARCHAR(20) DEFAULT NULL,
                    priority        DECIMAL(2,1) DEFAULT NULL,
                    fetched_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
            $this->db->executeStatement('CREATE INDEX idx_sitemap_url ON sitemap_urls (url)');
        }
    }
}