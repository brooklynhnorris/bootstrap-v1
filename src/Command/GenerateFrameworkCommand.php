<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:generate-framework', description: 'Generate deep SEO framework documents per category using 5-LLM deliberation')]
class GenerateFrameworkCommand extends Command
{
    private const CATEGORIES = [
        'schema' => [
            'name' => 'Schema & Structured Data',
            'scope' => 'Product schema, Organization schema, FAQPage, BreadcrumbList, VideoObject, HowTo, AggregateRating, LocalBusiness, sameAs, JSON-LD best practices, rich result eligibility, AI knowledge graph feeding',
            'research_hints' => 'Google Rich Results documentation, schema.org specifications, Knowledge Graph patents (US 8,682,913 B1), entity reconciliation algorithms, Structured Data Testing Tool behavior, AI Overview structured data preferences',
        ],
        'internal_links' => [
            'name' => 'Internal Link Architecture',
            'scope' => 'PageRank flow, hub-and-spoke topology, anchor text optimization, link equity distribution, orphan page prevention, crawl depth optimization, contextual vs navigational links, conversion path linking',
            'research_hints' => 'Original PageRank patent (US 6,285,999), Reasonable Surfer Model patent (US 7,716,225), link graph analysis, crawl budget allocation algorithms, topical clustering through internal link signals',
        ],
        'keyword_intent' => [
            'name' => 'Keyword & Intent Alignment',
            'scope' => 'Search intent classification, keyword cannibalization, SERP feature targeting, transactional vs informational routing, long-tail keyword coverage, semantic keyword clustering, query-page matching',
            'research_hints' => 'Hummingbird algorithm, BERT/MUM intent classification, Google Quality Rater Guidelines Section 12 (Needs Met), intent taxonomy (navigational/informational/transactional/commercial investigation), query deserves freshness (QDF)',
        ],
        'eeat' => [
            'name' => 'E-E-A-T & Trust Signals',
            'scope' => 'Experience, Expertise, Authoritativeness, Trustworthiness signals, author attribution, about page depth, contact/legal page trust, social proof integration, review signals, citation credibility',
            'research_hints' => 'Google Quality Rater Guidelines Sections 3-5 (E-E-A-T), YMYL classification, reputation research methodology, Google Knowledge-Based Trust patent (US 9,116,975), author entity recognition, site-level trust aggregation',
        ],
        'core_web_vitals' => [
            'name' => 'Core Web Vitals & Performance',
            'scope' => 'LCP, CLS, INP, TTFB, FCP, page weight budgets, image optimization, render-blocking resources, lazy loading, server response optimization, mobile performance',
            'research_hints' => 'Chrome UX Report (CrUX) methodology, Page Experience ranking signal documentation, Lighthouse scoring algorithms, HTTP Archive performance benchmarks, AI crawler timeout behavior (GPTBot, ClaudeBot rendering limits)',
        ],
        'ai_search' => [
            'name' => 'AI Search & Citation Eligibility',
            'scope' => 'Google AI Overviews citation selection, Perplexity source ranking, ChatGPT Browse attribution, passage extraction algorithms, definition paragraph patterns, question-answer content formatting, entity clarity for LLM comprehension',
            'research_hints' => 'Google AI Overview documentation, Retrieval Augmented Generation (RAG) architecture, passage ranking patent (US 10,909,157), featured snippet selection criteria, Perplexity citation methodology, content atomization for LLM extraction',
        ],
        'entity_authority' => [
            'name' => 'Entity & Topical Authority',
            'scope' => 'Central entity presence, entity co-occurrence patterns, Knowledge Graph signals, topical coverage completeness, semantic triplets, brand entity resolution, entity-attribute-value relationships',
            'research_hints' => 'Google Knowledge Graph patent (US 8,682,913 B1), entity salience scoring, TF-IDF and BM25 term importance, topical authority through content clustering, entity embedding models, co-citation and co-occurrence analysis',
        ],
        'user_signals' => [
            'name' => 'User Signals & Engagement',
            'scope' => 'CTR optimization, pogo-sticking detection, dwell time signals, bounce rate interpretation, engagement rate metrics, title/meta description click-through optimization, search satisfaction signals',
            'research_hints' => 'Google user interaction patents, NavBoost algorithm documentation, click model research (cascade model, dynamic Bayesian network), Chrome user engagement signals, Core Web Vitals as ranking signals, pogostick rate detection',
        ],
        'conversion_path' => [
            'name' => 'Conversion Path & CTA',
            'scope' => 'CTA placement and intent alignment, conversion funnel optimization, quote/contact page accessibility, mobile conversion readiness, search intent to conversion matching, above-fold CTA requirements',
            'research_hints' => 'Google UX playbooks, mobile-first indexing conversion requirements, above-the-fold content assessment, Fitts Law for CTA sizing, F-pattern and Z-pattern reading layouts, conversion rate optimization research for high-ticket items',
        ],
        'competitive_intel' => [
            'name' => 'Competitive Intelligence',
            'scope' => 'SERP position monitoring, competitor content gap analysis, featured snippet defense, new competitor detection, backlink gap analysis, market share click tracking, AI citation competition',
            'research_hints' => 'SERP volatility tracking methodology, competitive link analysis frameworks, content gap analysis using search demand curves, featured snippet displacement patterns, AI Overview source rotation analysis',
        ],
        'content_freshness' => [
            'name' => 'Content Freshness & Lifecycle',
            'scope' => 'Stale content detection, freshness ranking signals, seasonal content management, content decay patterns, evergreen content protection, publish/modified date optimization',
            'research_hints' => 'Google Query Deserves Freshness (QDF) patent (US 8,260,785), freshness ranking algorithms, datePublished/dateModified schema impact, content decay rate analysis, seasonal search demand patterns, recrawl frequency optimization',
        ],
        'local_seo' => [
            'name' => 'Local & Dealer SEO',
            'scope' => 'LocalBusiness schema, NAP consistency, service area signals, Google Business Profile integration, geographic entity resolution, dealer page optimization, local pack ranking factors',
            'research_hints' => 'Google local ranking factors (proximity, relevance, prominence), Pigeon algorithm, Vicinity update, Google Business Profile best practices, local entity resolution patents, NAP consistency scoring, local link building signals',
        ],
        'media_assets' => [
            'name' => 'Media & Asset Optimization',
            'scope' => 'Image alt text and entity signals, image file optimization, video schema, image sitemap coverage, PDF SEO, filename conventions, IPTC metadata, modern image formats',
            'research_hints' => 'Google Image Search ranking documentation, image alt text as entity signal research, WebP/AVIF adoption impact studies, VideoObject schema rich result requirements, image sitemap specification, IPTC metadata for search discovery, image filename keyword impact studies',
        ],
    ];

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('category', null, InputOption::VALUE_OPTIONAL, 'Generate framework for a specific category (e.g., schema, eeat)')
            ->addOption('list', null, InputOption::VALUE_NONE, 'List available categories');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if ($input->getOption('list')) {
            $output->writeln("Available categories:");
            foreach (self::CATEGORIES as $key => $cat) {
                $output->writeln("  {$key}: {$cat['name']}");
            }
            return Command::SUCCESS;
        }

        $catFilter = $input->getOption('category');
        $categories = self::CATEGORIES;

        if ($catFilter) {
            if (!isset($categories[$catFilter])) {
                $output->writeln("[ERROR] Unknown category: {$catFilter}");
                $output->writeln("Available: " . implode(', ', array_keys($categories)));
                return Command::FAILURE;
            }
            $categories = [$catFilter => $categories[$catFilter]];
        }

        // Load the existing framework as the quality benchmark
        $existingFramework = '';
        $fwPath = dirname(__DIR__, 2) . '/jeannes_framework.txt';
        if (file_exists($fwPath)) {
            $existingFramework = file_get_contents($fwPath);
        }

        $output->writeln('');
        $output->writeln('+=============================================+');
        $output->writeln('|   LOGIRI FRAMEWORK GENERATOR                |');
        $output->writeln('|   5-LLM Deep Research · Patent-Level Depth  |');
        $output->writeln('+=============================================+');
        $output->writeln('');

        $allFrameworks = [];

        foreach ($categories as $key => $category) {
            $output->writeln("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            $output->writeln("  CATEGORY: {$category['name']}");
            $output->writeln("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            // Round 1: Independent deep research
            $output->writeln("  Round 1: Independent research...");
            $r1Prompt = $this->buildResearchPrompt($category, $existingFramework);
            $r1Responses = $this->callAllLLMs($r1Prompt, 6000);

            $r1Outputs = [];
            foreach ($r1Responses as $llm => $response) {
                if (isset($response['error'])) {
                    $output->writeln("    [!] {$llm}: {$response['error']}");
                    continue;
                }
                $r1Outputs[$llm] = $response['text'];
                $wordCount = str_word_count($response['text']);
                $output->writeln("    {$llm}: {$wordCount} words");
            }

            // Round 2: Synthesis — merge all outputs into one definitive framework
            $output->writeln("  Round 2: Synthesis...");
            $r2Prompt = $this->buildSynthesisPrompt($category, $r1Outputs, $existingFramework);
            $r2Responses = $this->callAllLLMs($r2Prompt, 8000);

            // Pick the best (longest, most structured) output
            $bestOutput = '';
            $bestLength = 0;
            foreach ($r2Responses as $llm => $response) {
                if (isset($response['error'])) {
                    $output->writeln("    [!] {$llm}: {$response['error']}");
                    continue;
                }
                $len = strlen($response['text']);
                $output->writeln("    {$llm}: " . str_word_count($response['text']) . " words");
                if ($len > $bestLength) {
                    $bestLength = $len;
                    $bestOutput = $response['text'];
                }
            }

            // Fallback: if Round 2 is weak, use best Round 1
            if ($bestLength < 1000) {
                $output->writeln("  [!] Round 2 thin — using best Round 1 output");
                foreach ($r1Outputs as $llm => $text) {
                    if (strlen($text) > $bestLength) {
                        $bestLength = strlen($text);
                        $bestOutput = $text;
                    }
                }
            }

            $allFrameworks[$key] = [
                'name' => $category['name'],
                'content' => $bestOutput,
            ];

            $output->writeln("  >> Framework generated: " . str_word_count($bestOutput) . " words");
            $output->writeln('');
        }

        // Save all frameworks to a single file
        $outputPath = dirname(__DIR__, 2) . '/generated-frameworks.txt';
        $file = fopen($outputPath, 'w');
        fwrite($file, "LOGIRI DEEP SEO FRAMEWORKS — " . date('Y-m-d H:i:s') . "\n");
        fwrite($file, "Generated by 5-LLM deliberation engine\n");
        fwrite($file, "Domain: doubledtrailers.com\n");
        fwrite($file, str_repeat('=', 60) . "\n\n");

        foreach ($allFrameworks as $key => $fw) {
            fwrite($file, str_repeat('─', 60) . "\n");
            fwrite($file, "FRAMEWORK: {$fw['name']}\n");
            fwrite($file, str_repeat('─', 60) . "\n\n");
            fwrite($file, $fw['content'] . "\n\n");
        }

        fclose($file);

        $output->writeln('==============================================');
        $output->writeln("COMPLETE: " . count($allFrameworks) . " frameworks generated");
        $output->writeln("Output saved to: generated-frameworks.txt");
        $output->writeln('==============================================');

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  BUILD RESEARCH PROMPT (Round 1)
    // ─────────────────────────────────────────────

    private function buildResearchPrompt(array $category, string $existingFramework): string
    {
        return <<<PROMPT
You are a senior SEO researcher producing a DEEP technical framework document for the category: {$category['name']}

SCOPE: {$category['scope']}

RESEARCH DIRECTIONS: {$category['research_hints']}

QUALITY BENCHMARK — This is the level of depth and specificity you MUST match or exceed:

{$existingFramework}

YOUR TASK:
Produce a framework document for "{$category['name']}" at the SAME depth as the benchmark above. This means:

1. CITE REAL PATENTS, ALGORITHMS, OR DOCUMENTATION by name/number where they exist. Examples:
   - "Based on Google Patent US X,XXX,XXX — [Algorithm Name]..."
   - "Per Google Quality Rater Guidelines Section X..."
   - "The schema.org specification for [Type] requires..."
   Do NOT fabricate patent numbers. If you're unsure of a number, describe the algorithm by name and behavior.

2. EXPLAIN THE UNDERLYING MECHANISM — not just "do X for better rankings" but WHY it works at the algorithm level. Example from the benchmark: "Search engines calculate content based on a weighted sum of word count + image dimensions. The MSE is found by traversing the DOM tree along the path of maximum content."

3. PROVIDE SPECIFIC, MEASURABLE RULES — not generic advice. Example: "Maximum 3 internal links per page" not "add internal links."

4. APPLY TO DOUBLE D TRAILERS SPECIFICALLY:
   - Z-Frame (high-tensile, zinc-infused construction — NOT aluminum)
   - SafeTack (patented reverse-load, swing-out rear tack)
   - SafeBump (single-piece molded fiber composite roof, Z-Frame tubing every 16")
   - SafeKick (recycled plastic/rubber wall panels)
   - Product pages: max 500 words body text, MSE elements carry the page
   - Outer pages: min 1000 words
   - Max 3 internal links per page, zero external links

5. STRUCTURE as numbered sections with clear headers, like the benchmark document.

6. INCLUDE A "RULES FOR" section broken down by role:
   - Rules for Web Developers (code/implementation)
   - Rules for SEO Specialists (content/strategy)
   - Rules for Web Designers (visual/UX)

Write the definitive framework document. This will be used to generate and evaluate SEO rules for years. Make it thorough.
PROMPT;
    }

    // ─────────────────────────────────────────────
    //  BUILD SYNTHESIS PROMPT (Round 2)
    // ─────────────────────────────────────────────

    private function buildSynthesisPrompt(array $category, array $r1Outputs, string $existingFramework): string
    {
        $proposals = '';
        foreach ($r1Outputs as $llm => $text) {
            $proposals .= "\n\n=== " . strtoupper($llm) . " RESEARCH ===\n";
            // Truncate to prevent token overflow but keep substance
            $proposals .= substr($text, 0, 4000);
        }

        return <<<PROMPT
You are synthesizing 5 independent SEO research outputs into ONE definitive framework document for: {$category['name']}

QUALITY BENCHMARK (match this depth):
{$existingFramework}

RESEARCH FROM 5 AI MODELS:
{$proposals}

YOUR TASK:
Merge the 5 research outputs into a SINGLE, definitive framework document. Rules:

1. KEEP the strongest insights from each model — real patents, specific algorithms, measurable rules
2. REMOVE any conflicting or generic advice — if two models disagree, go with the one citing a real source
3. REMOVE any incorrect DDT brand references (aluminum, Z-Bar, SafeKill)
4. ENSURE all rules are specific to Double D Trailers where applicable
5. STRUCTURE with numbered sections and clear headers matching the benchmark format
6. INCLUDE "Rules for Web Developers / SEO Specialists / Web Designers" sections
7. KEEP it CONCISE but DEEP — every sentence should teach something specific, not repeat generic SEO wisdom
8. Product pages: max 500 words body text. Outer pages: min 1000 words. Max 3 internal links per page.

The output will be appended to the existing framework file and used to generate rules. Make it production-ready.
PROMPT;
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
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'gpt-4o', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => 'You are a senior SEO researcher with deep knowledge of search engine patents, algorithms, and documentation. Cite real sources.'], ['role' => 'user', 'content' => $prompt]]]),
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
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'grok-3-fast', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => 'You are a senior SEO researcher with deep knowledge of search engine patents, algorithms, and documentation. Cite real sources.'], ['role' => 'user', 'content' => $prompt]]]),
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
                CURLOPT_POSTFIELDS     => json_encode(['model' => 'sonar-pro', 'max_tokens' => $maxTokens, 'messages' => [['role' => 'system', 'content' => 'You are a senior SEO researcher with deep knowledge of search engine patents, algorithms, and documentation. Cite real sources. Use web search to verify patent numbers and algorithm names.'], ['role' => 'user', 'content' => $prompt]]]),
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
            $raw = curl_multi_getcontent($ch);
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