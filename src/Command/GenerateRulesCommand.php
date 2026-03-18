<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:generate-rules', description: 'Multi-LLM deliberation to generate comprehensive SEO ruleset for traditional + AI search')]
class GenerateRulesCommand extends Command
{
    private const MAX_ROUNDS = 3;

    // ── SEO CATEGORIES — comprehensive coverage for traditional + AI search ──
    private const CATEGORIES = [
        'on_page_content' => [
            'name' => 'On-Page Content Quality',
            'scope' => 'Word count, heading structure, keyword placement, content depth, readability, content freshness, duplicate content, thin content detection',
        ],
        'technical_seo' => [
            'name' => 'Technical SEO',
            'scope' => 'Indexing, crawlability, canonical tags, robots directives, sitemap health, redirect chains, HTTP status codes, hreflang, URL structure, crawl budget',
        ],
        'schema_structured_data' => [
            'name' => 'Schema & Structured Data',
            'scope' => 'JSON-LD presence, schema type correctness per page function, rich results eligibility, FAQ schema, Product schema, Organization schema, BreadcrumbList, Review/AggregateRating, HowTo schema, VideoObject',
        ],
        'internal_linking' => [
            'name' => 'Internal Link Architecture',
            'scope' => 'Core link equity flow, orphan pages, dead-end pages, link depth, anchor text diversity, topical cluster linking, hub-and-spoke structure, link equity concentration',
        ],
        'keyword_intent' => [
            'name' => 'Keyword & Intent Alignment',
            'scope' => 'Keyword cannibalization, intent mismatch (informational vs transactional), keyword coverage gaps, money page keyword targeting, long-tail opportunity detection, SERP feature targeting',
        ],
        'eeat_trust' => [
            'name' => 'E-E-A-T & Trust Signals',
            'scope' => 'Author attribution, expertise signals, testimonials/reviews presence, citation/source quality, about page completeness, contact information, privacy/terms pages, brand entity signals',
        ],
        'core_web_vitals' => [
            'name' => 'Core Web Vitals & Performance',
            'scope' => 'LCP, FID/INP, CLS, page weight, render-blocking resources, image optimization, mobile usability, TTFB, lazy loading',
        ],
        'ai_search_readiness' => [
            'name' => 'AI Search & Citation Eligibility',
            'scope' => 'AI Overview citation eligibility, question-answer content coverage, entity clarity for LLM understanding, concise definition paragraphs, topical authority depth, structured FAQ content, content that directly answers search queries, Perplexity/ChatGPT citation patterns',
        ],
        'entity_authority' => [
            'name' => 'Entity & Topical Authority',
            'scope' => 'Central entity presence and consistency, entity relationships (brand↔product↔category), Knowledge Graph signals, topical coverage completeness, semantic triplets, LSI term coverage, co-occurrence patterns',
        ],
        'user_signals' => [
            'name' => 'User Signals & Engagement',
            'scope' => 'Bounce rate anomalies, exit rate patterns, pogo-sticking detection, dwell time proxies, scroll depth signals, CTR vs position benchmarks, engagement decay detection',
        ],
        'conversion_path' => [
            'name' => 'Conversion Path & CTA',
            'scope' => 'CTA presence on money pages, conversion funnel completeness, form accessibility, contact path within 2 clicks, quote/configurator discoverability, mobile conversion UX',
        ],
        'competitive_intelligence' => [
            'name' => 'Competitive Intelligence',
            'scope' => 'Competitor content gap analysis, SERP position monitoring, featured snippet opportunities, competitor backlink signals, market share trending, new competitor detection',
        ],
        'content_freshness' => [
            'name' => 'Content Freshness & Lifecycle',
            'scope' => 'Stale content detection, last-modified signals, seasonal content management, content decay alerts, evergreen content protection, publish date optimization',
        ],
        'local_seo' => [
            'name' => 'Local & Dealer SEO',
            'scope' => 'Dealer page optimization, local schema markup, NAP consistency, Google Business Profile alignment, service area coverage, local keyword targeting',
        ],
        'media_assets' => [
            'name' => 'Media & Asset Optimization',
            'scope' => 'Image alt text coverage, image file size, video SEO (VideoObject schema), PDF SEO (title, indexing), image sitemap, WebP/AVIF format adoption',
        ],
    ];

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('category', null, InputOption::VALUE_OPTIONAL, 'Generate rules for a specific category only (e.g. ai_search_readiness)')
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Show prompt without calling APIs')
            ->addOption('verbose-llm', null, InputOption::VALUE_NONE, 'Show full LLM responses');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $categoryFilter = $input->getOption('category');
        $dryRun         = (bool) $input->getOption('dry-run');
        $verboseLlm     = (bool) $input->getOption('verbose-llm');

        $output->writeln('');
        $output->writeln('+============================================+');
        $output->writeln('|     LOGIRI RULE GENERATION ENGINE          |');
        $output->writeln('|  5-LLM Deliberation · Traditional + AI    |');
        $output->writeln('+============================================+');
        $output->writeln('');

        // ── Gather site context ──
        $output->writeln('Gathering site data context...');
        $siteContext = $this->gatherSiteContext($output);

        // ── Existing rules loaded but NOT used as constraints — from-scratch design ──
        $existingRules = $this->loadExistingRules();
        $output->writeln('Mode: FROM SCRATCH — designing optimal ruleset with pure AI SEO knowledge');
        $output->writeln('(' . count($existingRules) . ' existing rules loaded for reference only, not as constraints)');

        // ── Filter categories ──
        $categories = self::CATEGORIES;
        if ($categoryFilter) {
            if (!isset($categories[$categoryFilter])) {
                $output->writeln("[ERROR] Unknown category: {$categoryFilter}");
                $output->writeln("Available: " . implode(', ', array_keys($categories)));
                return Command::FAILURE;
            }
            $categories = [$categoryFilter => $categories[$categoryFilter]];
        }

        $output->writeln('Generating rules for ' . count($categories) . ' categories...');
        $output->writeln('');

        $allGeneratedRules = [];

        foreach ($categories as $catKey => $category) {
            $output->writeln("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            $output->writeln("  CATEGORY: {$category['name']}");
            $output->writeln("  Scope: {$category['scope']}");
            $output->writeln("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            $prompt = $this->buildGenerationPrompt($category, $siteContext, $existingRules, $catKey);

            if ($dryRun) {
                $output->writeln("  [DRY RUN] Prompt length: " . strlen($prompt) . " chars");
                $output->writeln("  [DRY RUN] First 500 chars:");
                $output->writeln(substr($prompt, 0, 500));
                $output->writeln('');
                continue;
            }

            // ── Round 1: All LLMs generate independently ──
            $output->writeln("  Round 1: Independent generation...");
            $r1Responses = $this->callAllLLMs($prompt, 4000);
            $r1Rules     = [];

            foreach ($r1Responses as $llm => $response) {
                if (isset($response['error'])) {
                    $output->writeln("    [!] {$llm}: {$response['error']}");
                    continue;
                }
                $parsed = $this->parseGeneratedRules($response['text']);
                $r1Rules[$llm] = $parsed;
                $output->writeln("    {$llm}: proposed " . count($parsed) . " rules");
                if ($verboseLlm) {
                    $output->writeln("    [RAW] " . substr($response['text'], 0, 200) . "...");
                }
            }

            if (empty($r1Rules)) {
                $output->writeln("  [!] No LLM responses for this category. Skipping.");
                continue;
            }

            // ── Round 2: Cross-review and merge ──
            $output->writeln("  Round 2: Cross-review and consensus...");
            $mergePrompt   = $this->buildMergePrompt($category, $r1Rules, $siteContext);
            $r2Responses   = $this->callAllLLMs($mergePrompt, 6000);
            $r2Rules       = [];
            $bestR2Rules   = [];
            $bestR2Count   = 0;

            foreach ($r2Responses as $llm => $response) {
                if (isset($response['error'])) {
                    $output->writeln("    [!] {$llm}: {$response['error']}");
                    continue;
                }
                $parsed = $this->parseGeneratedRules($response['text']);
                $r2Rules[$llm] = $parsed;
                $output->writeln("    {$llm}: refined to " . count($parsed) . " rules");
                // Track best R2 output as fallback
                if (count($parsed) > $bestR2Count) {
                    $bestR2Rules = $parsed;
                    $bestR2Count = count($parsed);
                }
            }

            // ── Round 3: Final synthesis — pick best version ──
            $output->writeln("  Round 3: Final synthesis...");
            $finalPrompt    = $this->buildFinalPrompt($category, $r2Rules, $existingRules);
            $r3Responses    = $this->callAllLLMs($finalPrompt, 8000);
            $bestRules      = [];
            $bestCount      = 0;

            foreach ($r3Responses as $llm => $response) {
                if (isset($response['error'])) continue;
                $parsed = $this->parseGeneratedRules($response['text']);
                $output->writeln("    {$llm}: finalized " . count($parsed) . " rules");
                if (count($parsed) > $bestCount) {
                    $bestRules = $parsed;
                    $bestCount = count($parsed);
                }
            }

            // Fallback: if Round 3 produced fewer rules than Round 2 (truncation), use Round 2
            if ($bestCount < $bestR2Count && $bestR2Count >= 3) {
                $output->writeln("  [!] Round 3 truncated ({$bestCount} rules) — using Round 2 output ({$bestR2Count} rules)");
                $bestRules = $bestR2Rules;
            }

            // ── Display results ──
            if (!empty($bestRules)) {
                $output->writeln('');
                $output->writeln("  ╔══════════════════════════════════════════╗");
                $output->writeln("  ║  AGREED RULES: {$category['name']}");
                $output->writeln("  ╚══════════════════════════════════════════╝");

                foreach ($bestRules as $rule) {
                    $this->displayRule($output, $rule);
                }

                $allGeneratedRules[$catKey] = $bestRules;
            } else {
                $output->writeln("  [!] No rules converged for this category.");
            }

            $output->writeln('');
        }

        // ── Save all rules to file ──
        if (!$dryRun && !empty($allGeneratedRules)) {
            $this->saveRulesToFile($allGeneratedRules, $output);
        }

        // ── Summary ──
        $totalRules = array_sum(array_map('count', $allGeneratedRules));
        $output->writeln('');
        $output->writeln('==============================================');
        $output->writeln("COMPLETE: {$totalRules} rules generated across " . count($allGeneratedRules) . " categories");
        $output->writeln("Output saved to: generated-rules.txt");
        $output->writeln('==============================================');

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  GATHER SITE CONTEXT FROM DATABASE
    // ─────────────────────────────────────────────

    private function gatherSiteContext(OutputInterface $output): array
    {
        $context = [];

        try {
            // Page type distribution
            $context['page_counts'] = $this->db->fetchAllAssociative(
                "SELECT page_type, COUNT(*) as cnt FROM page_crawl_snapshots GROUP BY page_type ORDER BY cnt DESC"
            );

            // Core pages list
            $context['core_pages'] = $this->db->fetchAllAssociative(
                "SELECT url, title_tag, h1, word_count, has_central_entity, h1_matches_title, schema_types FROM page_crawl_snapshots WHERE page_type = 'core' ORDER BY url"
            );

            // Content stats
            $context['content_stats'] = $this->db->fetchAssociative(
                "SELECT COUNT(*) as total, AVG(word_count) as avg_words, MIN(word_count) as min_words, MAX(word_count) as max_words FROM page_crawl_snapshots WHERE word_count > 0"
            );

            // Schema coverage
            $context['schema_stats'] = $this->db->fetchAssociative(
                "SELECT COUNT(*) as has_schema FROM page_crawl_snapshots WHERE schema_types IS NOT NULL AND schema_types != '[]'"
            );

            // Entity coverage
            $context['entity_stats'] = $this->db->fetchAssociative(
                "SELECT COUNT(*) as has_entity FROM page_crawl_snapshots WHERE has_central_entity = true"
            );

            // H1/Title match stats
            $context['h1_stats'] = $this->db->fetchAssociative(
                "SELECT COUNT(*) as matches FROM page_crawl_snapshots WHERE h1_matches_title = true"
            );

            // Internal linking stats
            $context['link_stats'] = $this->db->fetchAssociative(
                "SELECT COUNT(*) as has_core_link FROM page_crawl_snapshots WHERE has_core_link = true AND page_type = 'outer'"
            );

            // GSC top queries if available
            try {
                $context['top_queries'] = $this->db->fetchAllAssociative(
                    "SELECT query, SUM(impressions) as imp, SUM(clicks) as clk, AVG(position) as avg_pos FROM gsc_snapshots WHERE date_range = '28d' GROUP BY query ORDER BY imp DESC LIMIT 20"
                );
            } catch (\Exception $e) {
                $context['top_queries'] = [];
            }

            // GSC top pages
            try {
                $context['top_pages'] = $this->db->fetchAllAssociative(
                    "SELECT page, SUM(impressions) as imp, SUM(clicks) as clk FROM gsc_snapshots WHERE date_range = '28d' GROUP BY page ORDER BY imp DESC LIMIT 20"
                );
            } catch (\Exception $e) {
                $context['top_pages'] = [];
            }

            $output->writeln('  Site context loaded: ' .
                count($context['core_pages']) . ' core pages, ' .
                ($context['content_stats']['total'] ?? 0) . ' total pages with content, ' .
                count($context['top_queries']) . ' GSC queries');

        } catch (\Exception $e) {
            $output->writeln("  [WARN] Could not load full site context: " . $e->getMessage());
        }

        return $context;
    }

    // ─────────────────────────────────────────────
    //  BUILD GENERATION PROMPT (Round 1)
    // ─────────────────────────────────────────────

    private function buildGenerationPrompt(array $category, array $siteContext, array $existingRules, string $catKey = ''): string
    {
        $siteData = $this->formatSiteContext($siteContext);
        $existingList = $this->formatExistingRules($existingRules);
        $brandContext = $this->buildBrandContext($siteContext);
        $framework = $this->loadFramework($catKey);

        return <<<PROMPT
You are a senior SEO architect designing a comprehensive rule engine FROM SCRATCH for doubledtrailers.com.

IMPORTANT: You are building an entirely new ruleset. Ignore any existing rules. Design the optimal rules based purely on your expert knowledge of SEO — both traditional search and AI search (Google AI Overviews, Perplexity, ChatGPT search).

⚠️ ABSOLUTE HARD LIMITS — VIOLATING THESE MEANS THE RULE IS REJECTED:
1. PRODUCT/CORE PAGES: Body text must NOT exceed 500 words. The MSE (Most Significant Element) — product images, attributes, CTAs, reviews, FAQs — carries the page, NOT word volume. Do NOT recommend 600, 800, or 1000+ words for product pages. Any rule with a product page word threshold above 500 will be discarded.
2. OUTER/INFORMATIONAL PAGES: Minimum 1000 words. Below 1000 = thin content. Do NOT use 300 or 500 as the outer page minimum.
3. INTERNAL LINKS: Maximum 3 per page. Do NOT recommend 5+ internal links.
4. EXTERNAL LINKS: Zero. Replace with citation mentions (author, title, date only).
These limits are NON-NEGOTIABLE. They override any general SEO best practice you may have learned.

⚠️ CRITICAL — BRAND ACCURACY RULES:
When writing examples, diagnoses, or action outputs, you MUST ONLY use the real product names, real brand terminology, and real specifications listed below. DO NOT invent product names, model names, or brand terminology. If you need an example and aren't sure of the exact name, use a generic placeholder like "[Product Model Name]" or "[Core Page URL]" — never fabricate a name.

⚠️ CRITICAL — SEO FRAMEWORK:
All rules MUST be consistent with the following SEO framework. Do NOT contradict these principles. Rules that conflict with this framework will be rejected.

{$framework}

BRAND FACTS — DOUBLE D TRAILERS:
{$brandContext}

SITE CONTEXT:
- Domain: doubledtrailers.com
- Business: Custom horse trailer manufacturer (Double D Trailers)
- Central entity: horse trailer
- Audience: horse owners, equestrians, competitive riders
- Business model: High-ticket custom manufacturing (custom quotes per trailer)
- Goal: Rank #1 in traditional search AND get cited in AI search

SITE DATA:
{$siteData}

DATA SOURCES AVAILABLE:
- page_crawl_snapshots: url, title_tag, h1, h2s, meta_description, word_count, has_central_entity, central_entity_count, internal_links, has_core_link, core_links_found, h1_matches_title, schema_types, canonical_url, is_noindex, page_type, is_utility
- gsc_snapshots: query, page, clicks, impressions, ctr, position, date_range
- WordPress REST API: full page/post content, Yoast SEO data (title, description, canonical, robots, schema)
- Future data sources (flag if rule needs these): Core Web Vitals API, SEMrush API, Google Rich Results API, backlink data, competitor SERP data

YOUR TASK:
Design 5-8 rules for the category: {$category['name']}
Scope: {$category['scope']}

Each rule must:
1. Be measurable with the available data sources (or clearly flag what new data source is needed)
2. Have a specific trigger condition with actual field names and thresholds
3. Include concrete investigation steps
4. Include a play-brief-style action output (not a report — a task ticket with specific steps, code snippets where relevant, and verification criteria)
5. Specify priority and which team member handles it (Brook=SEO/Content, Brad=Dev, Kalib=Design, Jeanne=Owner)
6. Address BOTH traditional search ranking AND AI search citation eligibility

FORMAT EACH RULE EXACTLY LIKE THIS:

RULE_ID: [CATEGORY_PREFIX]-R[N]
RULE_NAME: [Descriptive name]
TRIGGER_SOURCE: [Which data table(s)]
TRIGGER_CONDITION: [Exact SQL-like condition]
THRESHOLD: [Specific number or comparison]
DATA_FIELDS: [Exact field names used]
NEEDS_NEW_DATA: [None / name of API or data source needed]
INVESTIGATION_STEPS:
1. [Step]
2. [Step]
3. [Step]
DIAGNOSIS: [One paragraph — what this means for rankings and business]
ACTION_OUTPUT: PLAY_BRIEF format — current state, your move (with code/copy if applicable), done when, recheck days
PRIORITY: [Critical / High / Medium / Low]
ASSIGNED: [Brook / Brad / Kalib / Jeanne]
AI_SEARCH_RELEVANCE: [How this rule affects AI search citation eligibility — one sentence]

Design rules that would make a REAL difference for this specific site. Think like the world's best SEO strategist building a system to dominate both Google and AI search for horse trailers.
PROMPT;
    }

    // ─────────────────────────────────────────────
    //  BUILD MERGE PROMPT (Round 2)
    // ─────────────────────────────────────────────

    private function buildMergePrompt(array $category, array $r1Rules, array $siteContext): string
    {
        $proposals = '';
        foreach ($r1Rules as $llm => $rules) {
            $proposals .= "\n\n=== " . strtoupper($llm) . " PROPOSED ===\n";
            foreach ($rules as $rule) {
                $proposals .= "\nRULE_ID: " . ($rule['rule_id'] ?? 'unknown');
                $proposals .= "\nRULE_NAME: " . ($rule['rule_name'] ?? 'unknown');
                $proposals .= "\nTRIGGER_CONDITION: " . ($rule['trigger_condition'] ?? '');
                $proposals .= "\nPRIORITY: " . ($rule['priority'] ?? '');
                $proposals .= "\nAI_SEARCH_RELEVANCE: " . ($rule['ai_search_relevance'] ?? '');
                $proposals .= "\n---";
            }
        }

        return <<<PROMPT
You are reviewing SEO rule proposals from multiple AI models for doubledtrailers.com.

⚠️ ABSOLUTE HARD LIMITS — REJECT any rule that violates these:
- Product/core pages: max 500 words body text. REJECT any rule requiring 600, 800, or 1000+ words on product pages.
- Outer/informational pages: min 1000 words. REJECT any rule using 300 or 500 as the outer minimum.
- Internal links: max 3 per page. REJECT any rule recommending 5+ internal links.
- External links: zero. REJECT any rule recommending outbound links.

⚠️ BRAND ACCURACY: Double D Trailers builds custom horse trailers using Z-Frame (high-tensile, zinc-infused material — NOT aluminum, NOT traditional steel). Real brand terms: Z-Frame (not Z-Bar), SafeTack (patented reverse-load with swing-out rear tack), SafeBump (single-piece molded fiber composite roof), SafeKick (recycled plastic/rubber wall panels). Do NOT invent product names or pricing. If you need an example, use "[Product Model Name]" as a placeholder or reference a real URL from the site data.

CATEGORY: {$category['name']}
SCOPE: {$category['scope']}

PROPOSALS FROM ROUND 1:
{$proposals}

YOUR TASK:
1. Identify the BEST rules across all proposals — rules that are most specific, most measurable, and most impactful for doubledtrailers.com
2. Merge overlapping rules into single, stronger versions
3. Eliminate rules that are too generic or unmeasurable
4. Remove any rules that reference incorrect brand terminology (aluminum, Z-Bar, SafeKill, etc.)
5. REJECT any rule with product page word counts above 500 or outer page minimums below 1000
6. Ensure every rule has a clear trigger condition with real field names
7. Ensure every rule addresses AI search citation eligibility
8. Ensure every rule is CONSISTENT with the SEO framework provided in Round 1

OUTPUT: 5-8 refined rules in the EXACT same format as Round 1 (RULE_ID, RULE_NAME, TRIGGER_SOURCE, TRIGGER_CONDITION, etc.)

Be ruthless — only keep rules that would genuinely move rankings for a custom horse trailer manufacturer using Z-Frame construction.
PROMPT;
    }

    // ─────────────────────────────────────────────
    //  BUILD FINAL PROMPT (Round 3)
    // ─────────────────────────────────────────────

    private function buildFinalPrompt(array $category, array $r2Rules, array $existingRules): string
    {
        $refined = '';
        foreach ($r2Rules as $llm => $rules) {
            $refined .= "\n\n=== " . strtoupper($llm) . " REFINED ===\n";
            foreach ($rules as $rule) {
                $refined .= "\nRULE_ID: " . ($rule['rule_id'] ?? 'unknown');
                $refined .= "\nRULE_NAME: " . ($rule['rule_name'] ?? 'unknown');
                $refined .= "\nTRIGGER_CONDITION: " . ($rule['trigger_condition'] ?? '');
                $refined .= "\nTHRESHOLD: " . ($rule['threshold'] ?? '');
                $refined .= "\nPRIORITY: " . ($rule['priority'] ?? '');
                $refined .= "\nNEEDS_NEW_DATA: " . ($rule['needs_new_data'] ?? 'None');
                $refined .= "\n---";
            }
        }

        return <<<PROMPT
FINAL ROUND: Produce the definitive rule set for category "{$category['name']}" on doubledtrailers.com.

This is a FROM-SCRATCH ruleset design. No existing rules to consider — build the best possible rules.

⚠️ ABSOLUTE HARD LIMITS — ANY RULE VIOLATING THESE IS AUTOMATICALLY REJECTED:
- Product/core pages: max 500 words body text. MSE elements carry the page. Do NOT set thresholds at 600, 800, or 1000+ words for product pages.
- Outer/informational pages: min 1000 words. Do NOT use 300 or 500 as the outer minimum.
- Internal links: max 3 per page. Do NOT recommend 5+ internal links.
- External links: zero. Citation mentions only (author, title, date — no hyperlinks).

⚠️ BRAND ACCURACY CHECK — Before finalizing, verify:
- Double D Trailers builds custom horse trailers using Z-Frame (high-tensile, zinc-infused — NOT aluminum, NOT traditional steel)
- Real terms: Z-Frame (construction material), SafeTack (patented reverse-load, swing-out rear tack), SafeBump (molded fiber composite roof with Z-Frame tubing every 16"), SafeKick (recycled plastic/rubber wall panels)
- Do NOT reference: "Z-Bar", "aluminum", "steel", "SafeKill", or any invented product names or pricing
- When using examples, reference real URLs from the site or use "[Product Model Name]" placeholder

REFINED PROPOSALS FROM ROUND 2:
{$refined}

YOUR TASK:
Produce the FINAL 5-8 rules. For each rule:
1. The trigger condition must reference real database fields (page_crawl_snapshots, gsc_snapshots)
2. The action output must be in play-brief format (current state → your move → done when → recheck)
3. If the rule needs a data source that doesn't exist yet, clearly state it in NEEDS_NEW_DATA
4. Every rule must have an AI_SEARCH_RELEVANCE line explaining how it affects AI citation eligibility
5. Rules must cover BOTH traditional search ranking AND AI search citation
6. All examples must use REAL Double D Trailers product names and terminology only
7. REJECT any rule with product page word thresholds above 500 or outer page minimums below 1000

CRITICAL: You MUST output ALL 5-8 rules. Keep each rule's ACTION_OUTPUT to 3-5 bullet points maximum — do NOT write full essays per rule. The DIAGNOSIS should be 1-2 sentences. Be concise so you have room for all rules.

FRAMEWORK COMPLIANCE: Every rule must be consistent with the SEO framework (macro/micro/outer page classification, MSE placement, predicate alignment, 75/25 macro-micro content split, heading contextual flow, image placement rules, max 3 internal links, first-sentence-answers-heading pattern, 100-word intro limit, product page max 500 words body text, outer page min 1000 words).

This is the production ruleset. Be precise. Be specific to custom horse trailers. Output in the exact RULE_ID/RULE_NAME/TRIGGER_SOURCE/etc format.
PROMPT;
    }

    // ─────────────────────────────────────────────
    //  PARSE GENERATED RULES FROM LLM OUTPUT
    // ─────────────────────────────────────────────

    private function parseGeneratedRules(string $text): array
    {
        $rules = [];

        // Normalize: strip Markdown formatting that wraps RULE_ID
        // Handles: ## RULE_ID: AIS-001, **RULE_ID:** AIS-001, ### RULE_ID: AIS-001
        $text = preg_replace('/^#{1,4}\s*/m', '', $text);
        $text = str_replace(['**RULE_ID:**', '**RULE_ID**:'], 'RULE_ID:', $text);
        $text = preg_replace('/\*\*([A-Z_]+):\*\*/', '$1:', $text);
        $text = preg_replace('/\*\*([A-Z_]+)\*\*\s*:/', '$1:', $text);

        // Split on RULE_ID: headers (with optional leading whitespace/newlines)
        $blocks = preg_split('/\n\s*RULE_ID:\s*/i', $text);

        foreach ($blocks as $block) {
            $block = trim($block);
            if (empty($block) || strlen($block) < 20) continue;

            $rule = [];
            $lines = explode("\n", $block, 2);
            $rule['rule_id'] = trim(preg_replace('/^[\*#\s]+/', '', $lines[0]));
            $rest = $lines[1] ?? '';

            $rule['rule_name']           = $this->extractRuleField($rest, 'RULE_NAME');
            $rule['trigger_source']      = $this->extractRuleField($rest, 'TRIGGER_SOURCE');
            $rule['trigger_condition']   = $this->extractRuleField($rest, 'TRIGGER_CONDITION');
            $rule['threshold']           = $this->extractRuleField($rest, 'THRESHOLD');
            $rule['data_fields']         = $this->extractRuleField($rest, 'DATA_FIELDS');
            $rule['needs_new_data']      = $this->extractRuleField($rest, 'NEEDS_NEW_DATA');
            $rule['investigation_steps'] = $this->extractRuleField($rest, 'INVESTIGATION_STEPS');
            $rule['diagnosis']           = $this->extractRuleField($rest, 'DIAGNOSIS');
            $rule['action_output']       = $this->extractRuleField($rest, 'ACTION_OUTPUT');
            $rule['priority']            = $this->extractRuleField($rest, 'PRIORITY');
            $rule['assigned']            = $this->extractRuleField($rest, 'ASSIGNED');
            $rule['ai_search_relevance'] = $this->extractRuleField($rest, 'AI_SEARCH_RELEVANCE');

            if ($rule['rule_name'] || $rule['trigger_condition']) {
                $rules[] = $rule;
            }
        }

        return $rules;
    }

    private function extractRuleField(string $text, string $field): string
    {
        // Strip markdown bold from field names in the text for matching
        $cleanText = preg_replace('/\*\*([A-Z_]+):\*\*/', '$1:', $text);
        $cleanText = preg_replace('/\*\*([A-Z_]+)\*\*\s*:/', '$1:', $cleanText);

        // Multi-line: capture until next FIELD_NAME: or end
        if (preg_match('/' . preg_quote($field, '/') . '\s*:\s*(.*?)(?=\n\s*[A-Z][A-Z_]{2,}\s*:|$)/s', $cleanText, $m)) {
            return trim($m[1]);
        }
        if (preg_match('/' . preg_quote($field, '/') . '\s*:\s*(.+)/i', $cleanText, $m)) {
            return trim($m[1]);
        }
        return '';
    }

    // ─────────────────────────────────────────────
    //  LOAD SEO FRAMEWORK (Jeanne's methodology)
    //  Loads foundational sections (1-12) + category-specific section
    // ─────────────────────────────────────────────

    private function loadFramework(?string $categoryKey = null): string
    {
        $path = __DIR__ . '/../../jeannes_framework.txt';
        if (!file_exists($path)) {
            return '(Framework file not found at ' . $path . ')';
        }

        $content = file_get_contents($path);

        // Map category keys to framework section names
        $sectionMap = [
            'on_page_content'         => 'On-Page Content Quality',
            'technical_seo'           => 'Technical SEO',
            'schema_structured_data'  => 'Schema & Structured Data',
            'internal_linking'        => 'Internal Link Architecture',
            'keyword_intent'          => 'Keyword & Intent Alignment',
            'eeat_trust'              => 'E-E-A-T & Trust Signals',
            'core_web_vitals'         => 'Core Web Vitals & Performance',
            'ai_search_readiness'     => 'AI Search & Citation Eligibility',
            'entity_authority'        => 'Entity & Topical Authority',
            'user_signals'            => 'User Signals & Engagement',
            'conversion_path'         => 'Conversion Path & CTA',
            'competitive_intelligence'=> 'Competitive Intelligence',
            'content_freshness'       => 'Content Freshness & Lifecycle',
            'local_seo'              => 'Local & Dealer SEO',
            'media_assets'           => 'Media & Asset Optimization',
        ];

        // Always include Part 1 (Sections 1-12) — the foundational rules
        $part1End = strpos($content, 'PART 2');
        if ($part1End === false) {
            // Old format or no Part 2 — return the whole file but truncated
            return substr($content, 0, 15000);
        }

        $foundational = substr($content, 0, $part1End);

        // If no specific category, return just foundational (saves tokens)
        if (!$categoryKey || !isset($sectionMap[$categoryKey])) {
            return $foundational;
        }

        // Find the specific category section in Part 2
        $sectionName = $sectionMap[$categoryKey];
        $pattern = '/SECTION\s+\d+:\s*' . preg_quote($sectionName, '/') . '\s*\n={10,}\n(.*?)(?=\n={10,}\nSECTION\s+\d+:|\z)/s';

        $categorySection = '';
        if (preg_match($pattern, $content, $m)) {
            $categorySection = "\n\nCATEGORY-SPECIFIC FRAMEWORK — {$sectionName}:\n" . trim($m[1]);
        }

        // Return foundational + category-specific (keeps prompt under ~12K words)
        return $foundational . $categorySection;
    }

    // ─────────────────────────────────────────────
    //  BUILD BRAND CONTEXT (prevents hallucination)
    // ─────────────────────────────────────────────

    private function buildBrandContext(array $siteContext): string
    {
        $lines = [];

        // Hard facts about the brand — these NEVER change based on data
        $lines[] = "Company: Double D Trailers (DDT)";
        $lines[] = "Founded: 1997 in Pink Hill, NC";
        $lines[] = "Current headquarters: Wilmington, NC";
        $lines[] = "What they build: Custom-built horse trailers";
        $lines[] = "Construction: Z-Frame — a high-tensile, zinc-infused material (NOT aluminum, NOT steel in the traditional sense)";
        $lines[] = "";
        $lines[] = "REAL BRAND TERMINOLOGY (use ONLY these terms):";
        $lines[] = "- Z-Frame: DDT's proprietary construction material — high-tensile, zinc-infused. This is what the trailers are made of. NOT 'Z-Bar', NOT aluminum, NOT traditional steel.";
        $lines[] = "- SafeTack: DDT's patented reverse-load design with swing-out rear tack. NOT 'safe tack', NOT 'SafeTrack'.";
        $lines[] = "- SafeBump: DDT's single-piece molded fiber composite roof reinforced with Z-Frame tubing every 16 inches. Protects horses' heads. NOT 'safe bump', NOT 'SafeKill'.";
        $lines[] = "- SafeKick: DDT's flexible, durable wall panel made of a combination of recycled plastic and rubber compound. NOT 'safe kick'.";
        $lines[] = "";
        $lines[] = "TRAILER CATEGORIES:";
        $lines[] = "- Bumper Pull horse trailers (smaller, pulled from vehicle bumper hitch)";
        $lines[] = "- Gooseneck horse trailers (larger, pulled from truck bed hitch)";
        $lines[] = "- Living Quarters horse trailers (have living space for humans)";
        $lines[] = "";
        $lines[] = "DO NOT reference or invent:";
        $lines[] = "- 'Aluminum' construction (DDT does NOT build aluminum trailers)";
        $lines[] = "- 'Steel' construction (DDT uses Z-Frame, not traditional steel)";
        $lines[] = "- 'Z-Bar' (the correct term is Z-Frame)";
        $lines[] = "- 'SafeKill' or 'safe kill' (does not exist)";
        $lines[] = "- Any product name not listed in the REAL PRODUCT PAGES below";
        $lines[] = "- Any made-up pricing (DDT does custom quotes, do not invent price ranges)";
        $lines[] = "";

        // Pull REAL product pages from the database
        if (!empty($siteContext['core_pages'])) {
            $lines[] = "REAL PRODUCT PAGES (use ONLY these names and URLs):";
            foreach ($siteContext['core_pages'] as $p) {
                $title = $p['title_tag'] ?? '(no title)';
                $h1    = $p['h1'] ?? '(no h1)';
                $url   = $p['url'] ?? '';
                $lines[] = "- URL: {$url}";
                $lines[] = "  Title: {$title}";
                $lines[] = "  H1: {$h1}";
            }
        }

        $lines[] = "";
        $lines[] = "TEAM (for rule assignment):";
        $lines[] = "- Brook: SEO + Content (40 hrs/week) — content writing, keyword strategy, on-page fixes";
        $lines[] = "- Brad: Developer (40 hrs/week) — schema, redirects, technical fixes, crawl commands";
        $lines[] = "- Kalib: Design (40 hrs/week) — UX, CTA design, page layout, conversion path";
        $lines[] = "- Jeanne: Owner (10 hrs/week) — strategic decisions, approvals, QA";

        return implode("\n", $lines);
    }

    // ─────────────────────────────────────────────
    //  FORMAT SITE CONTEXT FOR PROMPT
    // ─────────────────────────────────────────────

    private function formatSiteContext(array $ctx): string
    {
        $lines = [];

        if (!empty($ctx['page_counts'])) {
            $lines[] = "Page types: " . implode(', ', array_map(fn($r) => ($r['page_type'] ?? 'null') . ': ' . $r['cnt'], $ctx['page_counts']));
        }

        if (!empty($ctx['content_stats'])) {
            $s = $ctx['content_stats'];
            $lines[] = "Content: {$s['total']} pages with content | avg {$s['avg_words']} words | min {$s['min_words']} | max {$s['max_words']}";
        }

        if (!empty($ctx['schema_stats'])) {
            $lines[] = "Schema coverage: {$ctx['schema_stats']['has_schema']} pages have structured data";
        }

        if (!empty($ctx['entity_stats'])) {
            $lines[] = "Central entity: {$ctx['entity_stats']['has_entity']} pages mention 'horse trailer'";
        }

        if (!empty($ctx['h1_stats'])) {
            $lines[] = "H1/Title alignment: {$ctx['h1_stats']['matches']} pages match";
        }

        if (!empty($ctx['link_stats'])) {
            $lines[] = "Outer→Core links: {$ctx['link_stats']['has_core_link']} outer pages link to core";
        }

        if (!empty($ctx['core_pages'])) {
            $lines[] = "\nCore pages:";
            foreach ($ctx['core_pages'] as $p) {
                $boolFields = ['has_central_entity', 'h1_matches_title'];
                $entity = ($p['has_central_entity'] && $p['has_central_entity'] !== 'f') ? 'YES' : 'NO';
                $h1Match = ($p['h1_matches_title'] && $p['h1_matches_title'] !== 'f') ? 'YES' : 'NO';
                $lines[] = "  {$p['url']} | words: {$p['word_count']} | entity: {$entity} | h1_match: {$h1Match} | schema: {$p['schema_types']}";
            }
        }

        if (!empty($ctx['top_queries'])) {
            $lines[] = "\nTop GSC queries (28d):";
            foreach (array_slice($ctx['top_queries'], 0, 10) as $q) {
                $lines[] = "  \"{$q['query']}\" | imp: {$q['imp']} | clk: {$q['clk']} | pos: " . round($q['avg_pos'], 1);
            }
        }

        return implode("\n", $lines);
    }

    // ─────────────────────────────────────────────
    //  FORMAT EXISTING RULES FOR PROMPT
    // ─────────────────────────────────────────────

    private function formatExistingRules(array $rules): string
    {
        if (empty($rules)) return "(none loaded)";
        $lines = [];
        foreach ($rules as $r) {
            $lines[] = "{$r['id']} | {$r['name']} | Trigger: {$r['trigger_condition']}";
        }
        return implode("\n", $lines);
    }

    // ─────────────────────────────────────────────
    //  LOAD EXISTING RULES FROM system-prompt.txt
    // ─────────────────────────────────────────────

    private function loadExistingRules(): array
    {
        $promptPath = dirname(__DIR__, 2) . '/system-prompt.txt';
        if (!file_exists($promptPath)) return [];

        $content = file_get_contents($promptPath);
        $rules = [];

        preg_match_all('/\n(FC-R\d+)\s*\|\s*([^\n]+)\n(.*?)(?=\nFC-R\d+|\nRESULTS VERIFICATION|\z)/s', $content, $matches, PREG_SET_ORDER);

        foreach ($matches as $match) {
            $ruleText = trim($match[3]);
            $triggerCondition = '';
            if (preg_match('/Trigger Condition:\s*([^\n]+)/', $ruleText, $m)) $triggerCondition = trim($m[1]);

            $rules[] = [
                'id'                => trim($match[1]),
                'name'              => trim($match[2]),
                'trigger_condition' => $triggerCondition,
            ];
        }

        return $rules;
    }

    // ─────────────────────────────────────────────
    //  DISPLAY A GENERATED RULE
    // ─────────────────────────────────────────────

    private function displayRule(OutputInterface $output, array $rule): void
    {
        $output->writeln('');
        $output->writeln("  ── {$rule['rule_id']} | {$rule['rule_name']} ──");
        if ($rule['trigger_condition'])   $output->writeln("  Trigger:    {$rule['trigger_condition']}");
        if ($rule['threshold'])           $output->writeln("  Threshold:  {$rule['threshold']}");
        if ($rule['trigger_source'])      $output->writeln("  Source:     {$rule['trigger_source']}");
        if ($rule['needs_new_data'] && strtolower($rule['needs_new_data']) !== 'none') {
            $output->writeln("  ⚠ NEEDS:    {$rule['needs_new_data']}");
        }
        if ($rule['priority'])            $output->writeln("  Priority:   {$rule['priority']}");
        if ($rule['assigned'])            $output->writeln("  Assigned:   {$rule['assigned']}");
        if ($rule['ai_search_relevance']) $output->writeln("  AI Search:  {$rule['ai_search_relevance']}");
        if ($rule['diagnosis'])           $output->writeln("  Diagnosis:  " . substr($rule['diagnosis'], 0, 200));
    }

    // ─────────────────────────────────────────────
    //  SAVE ALL RULES TO FILE
    // ─────────────────────────────────────────────

    private function saveRulesToFile(array $allRules, OutputInterface $output): void
    {
        $filePath = dirname(__DIR__, 2) . '/generated-rules.txt';
        $content  = "LOGIRI GENERATED RULES — " . date('Y-m-d H:i:s') . "\n";
        $content .= "Generated by 5-LLM deliberation engine\n";
        $content .= "Domain: doubledtrailers.com\n";
        $content .= str_repeat('=', 60) . "\n\n";

        foreach ($allRules as $catKey => $rules) {
            $catName = self::CATEGORIES[$catKey]['name'] ?? $catKey;
            $content .= "\n" . str_repeat('─', 60) . "\n";
            $content .= "CATEGORY: {$catName}\n";
            $content .= str_repeat('─', 60) . "\n\n";

            foreach ($rules as $rule) {
                $content .= "{$rule['rule_id']} | {$rule['rule_name']}\n";
                $content .= "Trigger Source: {$rule['trigger_source']}\n";
                $content .= "Trigger Condition: {$rule['trigger_condition']}\n";
                $content .= "Threshold: {$rule['threshold']}\n";
                $content .= "Data Fields: {$rule['data_fields']}\n";
                $content .= "Needs New Data: {$rule['needs_new_data']}\n";
                $content .= "Investigation Steps:\n{$rule['investigation_steps']}\n";
                $content .= "Diagnosis: {$rule['diagnosis']}\n";
                $content .= "Action Output: {$rule['action_output']}\n";
                $content .= "Priority: {$rule['priority']}\n";
                $content .= "Assigned: {$rule['assigned']}\n";
                $content .= "AI Search Relevance: {$rule['ai_search_relevance']}\n";
                $content .= "\n---\n\n";
            }
        }

        file_put_contents($filePath, $content);
        $output->writeln("  Rules saved to: {$filePath}");
    }

    // ─────────────────────────────────────────────
    //  CALL ALL 5 LLMs IN PARALLEL
    // ─────────────────────────────────────────────

    private function callAllLLMs(string $prompt, int $maxTokens = 4000): array
    {
        $claudeKey     = $_ENV['ANTHROPIC_API_KEY']  ?? '';
        $openaiKey     = $_ENV['OPENAI_API_KEY']     ?? '';
        $geminiKey     = $_ENV['GEMINI_API_KEY']     ?? '';
        $grokKey       = $_ENV['XAI_API_KEY']        ?? '';
        $perplexityKey = $_ENV['PERPLEXITY_API_KEY'] ?? '';

        $systemMsg = 'You are a senior SEO architect specializing in both traditional search engine optimization and AI search citation eligibility. You design measurable, data-driven rule engines for specific websites. Be precise and specific — not generic.';

        $handles = [];
        $mh      = curl_multi_init();

        if ($claudeKey) {
            $ch = curl_init('https://api.anthropic.com/v1/messages');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'claude-sonnet-4-6', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'x-api-key: ' . $claudeKey, 'anthropic-version: 2023-06-01'],
                CURLOPT_TIMEOUT        => 120,
            ]);
            $handles['claude'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($openaiKey) {
            $ch = curl_init('https://api.openai.com/v1/chat/completions');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'gpt-4o', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => $systemMsg], ['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'Authorization: Bearer ' . $openaiKey],
                CURLOPT_TIMEOUT        => 120,
            ]);
            $handles['gpt4o'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($geminiKey) {
            $ch = curl_init("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={$geminiKey}");
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['contents' => [['parts' => [['text' => $prompt]]]], 'generationConfig' => ['maxOutputTokens' => $maxTokens]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
                CURLOPT_TIMEOUT        => 120,
            ]);
            $handles['gemini'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($grokKey) {
            $ch = curl_init('https://api.x.ai/v1/chat/completions');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'grok-3-fast', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => $systemMsg], ['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'Authorization: Bearer ' . $grokKey],
                CURLOPT_TIMEOUT        => 120,
            ]);
            $handles['grok'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        if ($perplexityKey) {
            $ch = curl_init('https://api.perplexity.ai/chat/completions');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'sonar-pro', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => $systemMsg], ['role' => 'user', 'content' => $prompt]]]),
                CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'Authorization: Bearer ' . $perplexityKey],
                CURLOPT_TIMEOUT        => 120,
            ]);
            $handles['perplexity'] = $ch;
            curl_multi_add_handle($mh, $ch);
        }

        $running = null;
        do { curl_multi_exec($mh, $running); curl_multi_select($mh); } while ($running > 0);

        $results = [];
        foreach ($handles as $llm => $ch) {
            $raw     = curl_multi_getcontent($ch);
            curl_multi_remove_handle($mh, $ch);
            curl_close($ch);
            $decoded = json_decode($raw, true);
            $results[$llm] = match($llm) {
                'claude'     => isset($decoded['content'][0]['text'])                           ? ['text' => $decoded['content'][0]['text']]                           : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'gpt4o'      => isset($decoded['choices'][0]['message']['content'])             ? ['text' => $decoded['choices'][0]['message']['content']]             : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'gemini'     => isset($decoded['candidates'][0]['content']['parts'][0]['text']) ? ['text' => $decoded['candidates'][0]['content']['parts'][0]['text']] : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'grok'       => isset($decoded['choices'][0]['message']['content'])             ? ['text' => $decoded['choices'][0]['message']['content']]             : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                'perplexity' => isset($decoded['choices'][0]['message']['content'])             ? ['text' => $decoded['choices'][0]['message']['content']]             : ['error' => $decoded['error']['message'] ?? 'Unknown error'],
                default      => ['error' => 'Unknown LLM'],
            };
        }

        curl_multi_close($mh);
        return $results;
    }
}


    
