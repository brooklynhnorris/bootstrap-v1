<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class PromptBuilderService
{
    public function __construct(private Connection $db)
    {
    }

    public function buildSystemPrompt(
        array $semrush,
        array $topQueries,
        array $topPages,
        string $userName,
        string $userRole,
        array $activeTasks,
        array $pendingRechecks,
        array $topQueries90d = [],
        array $pageAggregates = [],
        array $brandedQueries = [],
        array $cannibalizationCandidates = [],
        array $previousPages = [],
        array $landingPages = [],
        array $adsCampaigns = [],
        array $adsKeywords = [],
        array $adsSearchTerms = [],
        array $adsDailySpend = [],
        array $recentReviews = [],
        int $overrideCount = 0,
        array $crawlData = [],
        array $allCrawledUrls = [],
        array $verificationResults = [],
        array $ruleFeedback = [],
        array $ruleProposals = [],
        ?array $playUrlData = null
    ): string {
        $date = date('l, F j, Y');

        $querySummary = $this->buildQuerySummary($topQueries);
        $pageSummary = $this->buildPageSummary($topPages);
        $taskContext = $this->buildTaskContext($activeTasks);
        $recheckContext = $this->buildRecheckContext($pendingRechecks);
        $reviewContext = $this->buildReviewContext($recentReviews, $overrideCount);
        $staticRules = $this->loadStaticRules();

        $prompt = $this->buildCoreInstructionBlock($userName, $userRole, $date, $semrush, $querySummary, $pageSummary);
        $prompt .= $this->buildAnalyticsContext(
            $topQueries90d,
            $pageAggregates,
            $brandedQueries,
            $cannibalizationCandidates,
            $previousPages,
            $topPages,
            $landingPages,
            $adsCampaigns,
            $adsKeywords,
            $adsSearchTerms,
            $adsDailySpend
        );
        $prompt .= $taskContext;
        $prompt .= $recheckContext;
        $prompt .= $reviewContext;
        $prompt .= $this->buildCrawlContext($crawlData, $allCrawledUrls);
        $prompt .= $this->buildSuppressedUrlsContext();
        $prompt .= $this->buildPlayUrlContext($playUrlData);
        $prompt .= $this->buildVerificationContext($verificationResults, $ruleFeedback, $ruleProposals);
        $prompt .= $this->buildSchemaAndMemoryContext($crawlData);
        $prompt .= "\n\n" . $staticRules;

        return $prompt;
    }

    private function buildQuerySummary(array $topQueries): string
    {
        $querySummary = '';
        foreach (array_slice($topQueries, 0, 10) as $row) {
            $querySummary .= '- "' . $row['query'] . '" | Page: ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Position: ' . round($row['position'], 1) . "\n";
        }

        return $querySummary;
    }

    private function buildPageSummary(array $topPages): string
    {
        $pageSummary = '';
        foreach (array_slice($topPages, 0, 10) as $row) {
            $engagement = isset($row['avg_engagement_time'])
                ? ' | Engagement: ' . round($row['avg_engagement_time'], 0) . 's'
                : '';
            $pageSummary .= '- ' . $row['page_path'] . ' | Sessions: ' . $row['sessions'] . ' | Pageviews: ' . $row['pageviews'] . ' | Conversions: ' . $row['conversions'] . $engagement . "\n";
        }

        return $pageSummary;
    }

    private function buildTaskContext(array $activeTasks): string
    {
        if (empty($activeTasks)) {
            return '';
        }

        $context = "\n\nACTIVE TASKS IN SYSTEM:\n";
        $workload = [];
        $overdue = [];

        foreach ($activeTasks as $task) {
            $logged = (float) ($task['logged_hours'] ?? 0);
            $estimated = (float) ($task['estimated_hours'] ?? 0);
            $assignee = $task['assigned_to'] ?? 'Unassigned';

            if ($assignee !== 'Unassigned' && ($task['status'] ?? null) !== 'done') {
                $workload[$assignee] = ($workload[$assignee] ?? 0) + $estimated;
            }

            if ($estimated > 0 && $logged > $estimated && ($task['status'] ?? null) !== 'done') {
                $overdue[] = 'OVER-ESTIMATE: "' . $task['title'] . "\" - {$logged}h logged vs {$estimated}h estimated. Assigned: {$assignee}";
            }

            $context .= '- [' . strtoupper((string) ($task['priority'] ?? 'medium')) . '] '
                . $task['title']
                . ' | Assigned: ' . $assignee
                . ' | Status: ' . ($task['status'] ?? 'pending')
                . ' | ' . $estimated . "h est\n";
        }

        $context .= "\nTEAM WORKLOAD:\n";
        foreach (['Brook', 'Kalib', 'Brad'] as $name) {
            $load = $workload[$name] ?? 0;
            $status = $load > 40 ? 'OVERLOADED' : ($load > 30 ? 'HIGH' : 'OK');
            $context .= "- {$name}: {$load}h | {$status}\n";
        }

        if (!empty($overdue)) {
            $context .= "\nTASKS EXCEEDING ESTIMATES:\n" . implode("\n", $overdue) . "\n";
        }

        return $context;
    }

    private function buildRecheckContext(array $pendingRechecks): string
    {
        if (empty($pendingRechecks)) {
            return '';
        }

        $context = "\n\nPENDING VERIFICATION RECHECKS:\n";
        foreach ($pendingRechecks as $row) {
            $context .= '- ' . $row['title']
                . ' | Due: ' . $row['recheck_date']
                . ' | Type: ' . ($row['recheck_type'] ?? 'general')
                . ' | Assigned: ' . ($row['assigned_to'] ?? 'Unassigned')
                . "\n";
        }

        return $context;
    }

    private function buildReviewContext(array $recentReviews, int $overrideCount): string
    {
        $context = '';

        if (!empty($recentReviews)) {
            $context .= "\n\nRECENT RULE REVIEWS (last 30 days):\n";
            foreach ($recentReviews as $review) {
                $context .= '- ' . $review['rule_id']
                    . ' | Verdict: ' . $review['verdict']
                    . ' | By: ' . $review['reviewed_by']
                    . ' on ' . substr((string) $review['reviewed_at'], 0, 10);
                if (!empty($review['feedback'])) {
                    $context .= ' | Note: ' . $review['feedback'];
                }
                $context .= "\n";
            }
        }

        if ($overrideCount > 0) {
            $context .= "\nACTIVE USER OVERRIDES: {$overrideCount} manual classification corrections are in effect. These persist across crawls.\n";
        }

        return $context;
    }

    private function loadStaticRules(): string
    {
        $promptFile = dirname(__DIR__, 2) . '/system-prompt.txt';
        $staticRules = '';

        try {
            $dbRules = $this->db->fetchAllAssociative("SELECT * FROM seo_rules WHERE is_active = TRUE ORDER BY category, rule_id");
            if (!empty($dbRules)) {
                $parts = [];
                $parts[] = 'LOGIRI RULES ENGINE -- DOUBLE D TRAILERS (from database, ' . count($dbRules) . ' active rules)';
                $currentCategory = '';
                foreach ($dbRules as $rule) {
                    if (($rule['category'] ?? '') !== $currentCategory) {
                        $currentCategory = (string) ($rule['category'] ?? '');
                        $parts[] = "\n--- " . strtoupper($currentCategory) . " ---";
                    }
                    $parts[] = "\n{$rule['rule_id']} | {$rule['name']}";
                    $parts[] = 'Priority: ' . ($rule['priority'] ?? 'medium') . ' | Assigned: ' . ($rule['assigned'] ?? 'unassigned');
                    if (!empty($rule['diagnosis'])) {
                        $parts[] = 'Diagnosis: ' . substr((string) $rule['diagnosis'], 0, 300);
                    }
                    if (!empty($rule['threshold'])) {
                        $parts[] = 'Threshold: ' . substr((string) $rule['threshold'], 0, 200);
                    }
                }
                $staticRules = implode("\n", $parts);
            }
        } catch (\Exception $e) {
        }

        if ($staticRules === '' && file_exists($promptFile)) {
            $staticRules = (string) file_get_contents($promptFile);
        }

        return $staticRules;
    }

    private function buildCoreInstructionBlock(
        string $userName,
        string $userRole,
        string $date,
        array $semrush,
        string $querySummary,
        string $pageSummary
    ): string {
        $prompt = <<<'PROMPT'
You are Logiri, the AI Signal Engine and strategic operator for Double D Trailers (doubledtrailers.com).

YOUR PERSONA & BEHAVIOR:
Think of yourself as a grandmaster chess player who also happens to be funny at the office. You see the entire board - traffic, content, rankings, task queues - several moves ahead. You are sharp, direct, and occasionally witty in a dry, confident way. You do not waste moves. You never panic. When something is broken you say so plainly, then tell the user exactly how to fix it.
- Lead with the highest-leverage moves (Plays) first.
- Address the user by name every time. Make it feel personal, not robotic.
- Brief wit is welcome, but keep it sharp and never cringe.
- Never waffle. Never pad. If there are 3 critical Signals, say so and move.
- Think of Signals like discovered checks - they are already on the board whether you see them or not.

LOGIRI VOCABULARY - ALWAYS USE THESE TERMS:
| Generic Term   | Logiri Term          |
|----------------|----------------------|
| SEO Issue      | Signal               |
| Task / Action  | Play                 |
| Audit          | Sweep                |
| Monitoring     | Pulse                |
| Dashboard      | Command Center       |
| Alert          | Incident             |
| Recommendation | Playbook Step        |
| Automation     | Runbook              |
Never say: audit, issue, problem, recommendation, alert, monitoring, dashboard, SEO tool.

BRIEFING FORMAT RULES:
- Structure every briefing with H2 (##) section headings so sections can collapse.
- Each section must start with a 1-sentence summary line before any detail.
- Keep top-level bullets to one line each.
- Plays are called PLAYS, not tasks or recommendations.
- Never write HTML tags in your response. Plain markdown only.
- Use plain text when referencing H1 values.
- Start button label should always be "Run this Play."

CRITICAL TECHNICAL NOTE:
- This is a Symfony application. Always use `php bin/console` for commands.
- Never say `php artisan`.

ANTI-HALLUCINATION RULES:
- Only reference URLs that appear in the PAGE SIGNALS or CRAWL DATA sections below.
- Never invent URLs or fabricate metrics.
- If a page is not in crawl data, say it has not been crawled yet.
- Only suggest content moves or link targets that appear in crawl data.

PAGE TRIAGE:
- [high_value] (500+ impressions): full optimization plays.
- [optimize] (50-499 impressions): standard optimization plays.
- [low_value] (1-49 impressions): minimal effort only.
- [strategic_review] (0 impressions): do not generate optimization tasks. Generate one strategic review play and assign it to Jeanne.

TASK GENERATION RULES:
- Generate tasks only for FC rules listed below.
- Each task must include: title, assigned_to, priority, estimated_hours, recheck_type, recheck_days, recheck_criteria, description.
- Every task must have recheck_days and recheck_criteria.
- Tasks are scoped to the current user only.
- Do not duplicate tasks already in ACTIVE TASKS.
- One task equals one URL.
- Never generate tasks for suppressed URLs.
- Task title format: Action + URL.
- Task description must be surgical, plain text, and contain no HTML.
- Never combine a classification decision and an implementation fix in the same Play.
- If the right path depends on whether a page should be core vs outer, first create a decision Play only.
- After the classification decision is made, create a separate execution Play for the chosen path.
- Do not write Plays with "if X, do this / if Y, do that" branching instructions.

PLAY CARD FORMAT:
- When the user says "I just opened the Play:", respond with a structured play card.
- Every diagnosed violation must have a prescribed fix.
- Link targets must exist in crawl data.
- Keep play cards scoped to the requested rule.
- Show actual crawl data when prescribing changes.
- End every task-generating response with a TASKS_JSON block, and nothing after it.
- Play cards must have one clear objective.
- If a human decision gate is required, the Play objective is the decision itself, not the downstream implementation.
- Success criteria must be assignee-controllable completion checks, not outcome metrics like bounce rate, rankings, or conversions.
- Use analytics metrics for prioritization and later verification, not as immediate definition-of-done for a writing or dev Play.

EXECUTABLE ACTIONS:
- When the user asks to do something, include ACTIONS_JSON before TASKS_JSON.
- Actions are proposed operational requests, not narrative suggestions.

FOUNDATIONAL CONTENT RULES:
- FC-R1: indexed pages must contain the central entity "horse trailer".
- FC-R2: every page must be classified as Core or Outer.
- FC-R3: core pages must have at least 500 words.
- FC-R5: outer pages with 50+ impressions must link to a core page.
- FC-R6: informational core pages need 800+ words; product pages should not be bloated.
- FC-R7: indexed pages must have an H1 that matches the title tag.
- FC-R8: core pages must have at least one H2.
- FC-R9: core pages must have schema.
- FC-R10: high-traffic outer pages (100+ impressions) must link to a core page.

TEAM ROSTER:
- Brook  | SEO + Content | 40h/week | On-page fixes, content tasks, FC rule violations, internal linking
- Jeanne | Owner         | 10h/week | Rule review and approval, strategic decisions, QA of AI findings
- Brad   | Developer     | 40h/week | Schema implementation, redirects, canonicals, crawl command updates
- Kalib  | Design        | 40h/week | UX improvements, conversion path design, page layout, CTA design
PROMPT;

        $prompt .= "\n\nToday: {$date}";
        $prompt .= "\nCurrent user: {$userName} | Role: {$userRole}";
        $prompt .= "\n\nSEMrush: Keywords=" . ($semrush['organic_keywords'] ?? 'N/A')
            . ' | Traffic=' . ($semrush['organic_traffic'] ?? 'N/A')
            . ' | Updated=' . ($semrush['fetched_at'] ?? 'N/A');
        $prompt .= "\n\nTop GSC Queries (28d):\n" . $querySummary;
        $prompt .= "\nTop GA4 Pages (28d):\n" . $pageSummary;

        return $prompt;
    }

    private function buildAnalyticsContext(
        array $topQueries90d,
        array $pageAggregates,
        array $brandedQueries,
        array $cannibalizationCandidates,
        array $previousPages,
        array $topPages,
        array $landingPages,
        array $adsCampaigns,
        array $adsKeywords,
        array $adsSearchTerms,
        array $adsDailySpend
    ): string {
        $context = '';

        if (!empty($topQueries90d)) {
            $context .= "\n\n90-DAY GSC TRENDS:\n";
            foreach (array_slice($topQueries90d, 0, 5) as $row) {
                $context .= '- "' . $row['query'] . '" | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Position: ' . round($row['position'], 1) . "\n";
            }
        }

        if (!empty($pageAggregates)) {
            $context .= "\n\nGSC PAGE AGGREGATES:\n";
            foreach (array_slice($pageAggregates, 0, 5) as $row) {
                $context .= '- ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | CTR: ' . round($row['ctr'] * 100, 1) . '% | Position: ' . round($row['position'], 1) . "\n";
            }
        }

        if (!empty($brandedQueries)) {
            $context .= "\n\nBRANDED QUERIES:\n";
            foreach (array_slice($brandedQueries, 0, 5) as $row) {
                $context .= '- "' . $row['query'] . '" | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . "\n";
            }
        }

        if (!empty($cannibalizationCandidates)) {
            $context .= "\n\nCANNIBALIZATION CANDIDATES:\n";
            foreach (array_slice($cannibalizationCandidates, 0, 5) as $row) {
                $context .= '- "' . $row['query'] . '" | Pages: ' . $row['page_count'] . ' | Total impressions: ' . $row['total_impressions'] . "\n";
            }
        }

        if (!empty($previousPages)) {
            $previousLookup = [];
            foreach ($previousPages as $row) {
                $previousLookup[$row['page_path']] = $row;
            }
            $context .= "\n\nGA4 PERIOD COMPARISON (28d vs previous):\n";
            foreach (array_slice($topPages, 0, 10) as $current) {
                $previous = $previousLookup[$current['page_path']] ?? null;
                $sessionDelta = $previous ? ($current['sessions'] - $previous['sessions']) : 'N/A';
                $conversionDelta = $previous ? ($current['conversions'] - ($previous['conversions'] ?? 0)) : 'N/A';
                $context .= '- ' . $current['page_path'] . ' | Sessions: ' . $current['sessions'] . ' (Delta ' . $sessionDelta . ') | Conversions: ' . $current['conversions'] . ' (Delta ' . $conversionDelta . ")\n";
            }
        }

        if (!empty($landingPages)) {
            $context .= "\n\nTOP LANDING PAGES:\n";
            foreach (array_slice($landingPages, 0, 8) as $row) {
                $context .= '- ' . $row['page_path']
                    . ' | Sessions: ' . $row['sessions']
                    . ' | Bounce: ' . round($row['bounce_rate'] * 100, 1) . '%'
                    . ' | Engagement: ' . round($row['avg_engagement_time'], 0) . 's'
                    . ' | Conversions: ' . $row['conversions'] . "\n";
            }
        }

        if (!empty($adsCampaigns)) {
            $context .= "\n\nGOOGLE ADS CAMPAIGNS (30d):\n";
            foreach ($adsCampaigns as $row) {
                $context .= '- ' . $row['campaign_name']
                    . ' | Spend: $' . number_format($row['cost_micros'] / 1000000, 2)
                    . ' | Clicks: ' . $row['clicks']
                    . ' | CPC: $' . number_format($row['average_cpc'] / 1000000, 2)
                    . ' | Conv: ' . $row['conversions'] . "\n";
            }
        }

        if (!empty($adsKeywords)) {
            $context .= "\n\nTOP ADS KEYWORDS (30d):\n";
            foreach (array_slice($adsKeywords, 0, 8) as $row) {
                $context .= '- "' . $row['keyword'] . '" [' . $row['match_type'] . ']'
                    . ' | Spend: $' . number_format($row['cost_micros'] / 1000000, 2)
                    . ' | CPC: $' . number_format($row['average_cpc'] / 1000000, 2)
                    . ' | Conv: ' . $row['conversions'] . "\n";
            }
        }

        if (!empty($adsSearchTerms)) {
            $context .= "\n\nTOP SEARCH TERMS TRIGGERING ADS:\n";
            foreach (array_slice($adsSearchTerms, 0, 8) as $row) {
                $context .= '- "' . $row['search_term'] . '" | Clicks: ' . $row['clicks'] . ' | Spend: $' . number_format($row['cost_micros'] / 1000000, 2) . "\n";
            }
        }

        if (!empty($adsDailySpend)) {
            $totalSpend = array_sum(array_column($adsDailySpend, 'cost_micros')) / 1000000;
            $context .= "\n\nGOOGLE ADS TOTAL SPEND (last 14d): $" . number_format($totalSpend, 2) . "\n";
        }

        return $context;
    }

    private function buildCrawlContext(array $crawlData, array $allCrawledUrls): string
    {
        $context = '';

        if (!empty($crawlData)) {
            $crawledAt = $crawlData[0]['crawled_at'] ?? 'unknown';
            $hasDeterministicRules = array_filter($crawlData, fn(array $row): bool => !empty($row['rule_ids'] ?? null));
            $context .= $hasDeterministicRules
                ? "\n\nPAGE SIGNALS (deterministic snapshot sourced from page_facts + rule_violations; crawl basis: {$crawledAt}):\n"
                : "\n\nPAGE SIGNALS (last crawl: {$crawledAt}) - violations only:\n";
            foreach ($crawlData as $row) {
                $flags = $this->extractRuleFlags($row);
                if (empty($flags)) {
                    continue;
                }

                $h1 = substr((string) ($row['h1'] ?? '(none)'), 0, 120);
                $titleTag = substr((string) ($row['title_tag'] ?? '(none)'), 0, 120);
                $targetQuery = $row['target_query'] ?? null;
                $targetQueryInfo = $targetQuery
                    ? ' | Target: "' . $targetQuery . '" (pos:' . ($row['target_query_position'] ?? '?') . ', imp:' . ($row['target_query_impressions'] ?? 0) . ')'
                    : '';

                $context .= '- ' . $row['url']
                    . ' [' . ($row['page_type'] ?? 'unknown') . ']'
                    . ' [' . ($row['triage'] ?? 'unknown') . '] '
                    . implode(', ', $flags)
                    . ' | ' . (int) ($row['word_count'] ?? 0) . 'w'
                    . ' | ' . (int) ($row['page_impressions'] ?? 0) . 'imp/' . (int) ($row['page_clicks'] ?? 0) . 'clk'
                    . ' | H1: "' . $h1 . '"'
                    . ' | Title: "' . $titleTag . '"'
                    . $targetQueryInfo
                    . "\n";
            }

            $ruleCounts = $this->summarizeRules($crawlData);

            $context .= "\nCRAWL RULE VIOLATION SUMMARY:\n";
            $context .= $this->buildRuleSummaryLine('FC-R1', 'no central entity', $ruleCounts, $crawlData);
            $context .= $this->buildRuleSummaryLine('FC-R5', 'outer missing core link', $ruleCounts, $crawlData);
            $context .= $this->buildRuleSummaryLine('FC-R3', 'thin core <500w', $ruleCounts, $crawlData);
            $context .= $this->buildRuleSummaryLine('FC-R8', 'core missing H2s', $ruleCounts, $crawlData);
            $context .= $this->buildRuleSummaryLine('FC-R7', 'H1/title mismatch', $ruleCounts, $crawlData);
            $context .= $this->buildRuleSummaryLine('FC-R9', 'core missing schema', $ruleCounts, $crawlData);
        } else {
            $context .= "\n\nPAGE CRAWL DATA: No crawl data available. Run php bin/console app:crawl-pages to populate.\n";
        }

        if (!empty($allCrawledUrls)) {
            $coreUrls = array_filter($allCrawledUrls, fn(array $row): bool => strtolower((string) ($row['page_type'] ?? '')) === 'core');
            $context .= "\n\nVALID CORE PAGE URLS (use ONLY these when suggesting link targets):\n";
            foreach ($coreUrls as $row) {
                $context .= '  ' . $row['url'] . ' (' . ($row['word_count'] ?? 0) . "w)\n";
            }
            $context .= "\nIF A CORE URL IS NOT IN THIS LIST, IT DOES NOT EXIST. DO NOT REFERENCE IT.\n";
        }

        return $context;
    }

    private function buildSuppressedUrlsContext(): string
    {
        try {
            $suppressedUrls = $this->db->fetchAllAssociative('SELECT url, rule_id, reason FROM suppressed_tasks ORDER BY url');
        } catch (\Exception $e) {
            return '';
        }

        if (empty($suppressedUrls)) {
            return '';
        }

        $context = "\n\nSUPPRESSED URLS - STRATEGIC DECISIONS ALREADY MADE:\n";
        foreach ($suppressedUrls as $row) {
            $scope = ($row['rule_id'] ?? '') === '__ALL__' ? 'all rules' : ($row['rule_id'] ?? 'unknown');
            $context .= '- ' . $row['url'] . ' (' . $scope . ')';
            if (!empty($row['reason'])) {
                $context .= ' - ' . $row['reason'];
            }
            $context .= "\n";
        }
        $context .= 'These URLs have been reviewed and should not receive optimization tasks.' . "\n";

        return $context;
    }

    private function buildPlayUrlContext(?array $playUrlData): string
    {
        if ($playUrlData === null) {
            return '';
        }

        $context = "\n\nPLAY TARGET URL - FULL CRAWL DATA (use ONLY these values, do NOT invent or override):\n";
        $context .= 'URL: ' . $playUrlData['url'] . "\n";
        $context .= 'Page type: ' . ($playUrlData['page_type'] ?? 'unknown') . "\n";
        $context .= 'Word count: ' . (int) ($playUrlData['word_count'] ?? 0) . "\n";
        $context .= 'H1: "' . ($playUrlData['h1'] ?? '(none)') . '"' . "\n";
        $context .= 'Title tag: "' . ($playUrlData['title_tag'] ?? '(none)') . '"' . "\n";
        $context .= 'H1 matches title: ' . $this->boolToPromptString($playUrlData['h1_matches_title'] ?? null) . "\n";
        $context .= 'H2s: ' . ($playUrlData['h2s'] ?: '(none)') . "\n";
        $context .= 'Schema types: ' . ($playUrlData['schema_types'] ?: '(none)') . "\n";
        $context .= 'Schema errors: ' . ($playUrlData['schema_errors'] ?: '(none)') . "\n";
        $context .= 'Has central entity: ' . $this->boolToPromptString($playUrlData['has_central_entity'] ?? null) . "\n";
        $context .= 'Has core link: ' . $this->boolToPromptString($playUrlData['has_core_link'] ?? null) . "\n";
        $context .= 'Internal link count: ' . (int) ($playUrlData['internal_link_count'] ?? 0) . "\n";
        $context .= 'Image count: ' . (int) ($playUrlData['image_count'] ?? 0) . "\n";
        $context .= 'Has FAQ section: ' . $this->boolToPromptString($playUrlData['has_faq_section'] ?? null) . "\n";
        $context .= 'Has product image: ' . $this->boolToPromptString($playUrlData['has_product_image'] ?? null) . "\n";
        $context .= 'Images without alt: ' . (int) ($playUrlData['images_without_alt'] ?? 0) . "\n";
        $context .= 'Images with generic alt: ' . (int) ($playUrlData['images_with_generic_alt'] ?? 0) . "\n";

        if (!empty($playUrlData['image_alt_data'])) {
            $imageData = json_decode((string) $playUrlData['image_alt_data'], true);
            if (is_array($imageData) && !empty($imageData)) {
                $context .= "Image alt text inventory:\n";
                foreach ($imageData as $item) {
                    $context .= '  - ' . ($item['src'] ?? 'unknown') . ' | alt: "' . ($item['alt'] ?? 'NULL') . '"' . "\n";
                }
            }
        }

        $context .= 'Meta description: "' . ($playUrlData['meta_description'] ?? '(none)') . '"' . "\n";
        $context .= 'First sentence: "' . ($playUrlData['first_sentence_text'] ?? '(none)') . '"' . "\n";

        if (!empty($playUrlData['target_query'])) {
            $context .= 'Target query: "' . $playUrlData['target_query'] . '" (pos:' . ($playUrlData['target_query_position'] ?? '?') . ', imp:' . ($playUrlData['target_query_impressions'] ?? 0) . ', clicks:' . ($playUrlData['target_query_clicks'] ?? 0) . ')' . "\n";
        }

        $context .= "CRITICAL: The data above is ground truth for this URL. Do not contradict it.\n";

        if (!empty($playUrlData['body_text_snippet'])) {
            $bodySnippet = (string) $playUrlData['body_text_snippet'];
            if (strlen($bodySnippet) > 4000) {
                $bodySnippet = substr($bodySnippet, 0, 4000) . "\n... [truncated]";
            }
            $context .= "\nACTUAL PAGE CONTENT (from crawl - use this for surgical recommendations, do NOT invent content):\n";
            $context .= "---\n" . $bodySnippet . "\n---\n";
        } else {
            $context .= "\nNOTE: No body text available for this URL. Do not guess what the page says.\n";
        }

        return $context;
    }

    private function buildVerificationContext(array $verificationResults, array $ruleFeedback, array $ruleProposals): string
    {
        $context = '';

        if (!empty($verificationResults)) {
            $context .= "\n\nRECENT VERIFICATION OUTCOMES (tasks completed and checked against GSC):\n";
            foreach ($verificationResults as $row) {
                $context .= '- [' . $row['outcome_status'] . '] ' . $row['rule_id']
                    . ' | ' . $row['url']
                    . ' | ' . $row['metric_tracked']
                    . ': ' . $row['impressions_before'] . '->' . $row['impressions_after']
                    . ' imp, ' . $row['clicks_before'] . '->' . $row['clicks_after']
                    . ' clicks, pos ' . $row['position_before'] . '->' . $row['position_after'] . "\n";
            }
        }

        if (!empty($ruleFeedback)) {
            $context .= "\n\nLEARNING FEEDBACK (what worked and what did not from past fixes):\n";
            foreach ($ruleFeedback as $row) {
                $context .= '- ' . $row['rule_id'] . ' [' . $row['outcome_status'] . '] on ' . $row['url'] . ': ';
                if (!empty($row['what_worked']) && $row['what_worked'] !== 'N/A') {
                    $context .= 'Worked: ' . $row['what_worked'] . '. ';
                }
                if (!empty($row['what_didnt_work']) && $row['what_didnt_work'] !== 'N/A') {
                    $context .= 'Did not work: ' . $row['what_didnt_work'] . '. ';
                }
                if (!empty($row['proposed_change'])) {
                    $context .= 'Proposed (' . ($row['change_type'] ?? 'change') . '): ' . $row['proposed_change'];
                }
                $context .= "\n";
            }
        }

        if (!empty($ruleProposals)) {
            $context .= "\n\nPENDING RULE CHANGE PROPOSALS (awaiting user approval):\n";
            foreach ($ruleProposals as $row) {
                $context .= '- ' . $row['rule_id'] . ' (' . $row['change_type'] . '): ' . $row['summary'] . "\n";
                if (!empty($row['rationale'])) {
                    $context .= '  Rationale: ' . $row['rationale'] . "\n";
                }
            }
        }

        return $context;
    }

    private function buildSchemaAndMemoryContext(array $crawlData): string
    {
        $context = '';

        if (!empty($crawlData)) {
            $schemaErrorPages = array_filter(
                $crawlData,
                fn(array $row): bool => !empty($row['schema_errors']) && $row['schema_errors'] !== 'null' && $row['schema_errors'] !== '[]'
            );
            if (!empty($schemaErrorPages)) {
                $context .= "\n\nSCHEMA VALIDATION ERRORS (detected during last crawl):\n";
                foreach ($schemaErrorPages as $row) {
                    $errors = json_decode((string) $row['schema_errors'], true);
                    if (is_array($errors)) {
                        $context .= '- ' . $row['url'] . ': ' . implode('; ', $errors) . "\n";
                    }
                }
            }

            $linkViolations = array_filter($crawlData, fn(array $row): bool => (int) ($row['internal_link_count'] ?? 0) > 3);
            if (!empty($linkViolations)) {
                $context .= "\n\nINTERNAL LINK CAP VIOLATIONS (max 3 per page):\n";
                foreach (array_slice($linkViolations, 0, 10) as $row) {
                    $context .= '- ' . $row['url'] . ': ' . $row['internal_link_count'] . " links (max: 3)\n";
                }
            }
        }

        try {
            $learnings = $this->db->fetchAllAssociative(
                "SELECT learning, category, learned_from FROM chat_learnings WHERE is_active = TRUE ORDER BY confidence DESC, created_at DESC LIMIT 30"
            );
        } catch (\Exception $e) {
            $learnings = [];
        }

        if (!empty($learnings)) {
            $context .= "\n\nYOUR MEMORY (learned from past conversations with this user - follow these):\n";
            $currentCategory = '';
            foreach ($learnings as $row) {
                $category = $row['category'] ?? 'general';
                if ($category !== $currentCategory) {
                    $currentCategory = (string) $category;
                    $context .= "\n[{$currentCategory}]\n";
                }
                $context .= '- ' . $row['learning'] . "\n";
            }
        }

        return $context;
    }

    private function isTruthy(mixed $value): bool
    {
        return $value === true || $value === 1 || $value === '1' || $value === 't' || $value === 'true';
    }

    private function extractRuleFlags(array $row): array
    {
        if (!empty($row['rule_ids'])) {
            $ruleIds = array_filter(array_map('trim', explode(',', (string) $row['rule_ids'])));
            return array_map(
                static fn(string $ruleId): string => match ($ruleId) {
                    'FC-R1' => 'FC-R1:no-entity',
                    'FC-R3' => 'FC-R3:thin',
                    'FC-R5' => 'FC-R5:no-core-link',
                    'FC-R7' => 'FC-R7:h1-mismatch',
                    'FC-R8' => 'FC-R8:no-h2',
                    'FC-R9' => 'FC-R9:no-schema',
                    default => $ruleId,
                },
                $ruleIds
            );
        }

        $flags = [];
        if (!$this->isTruthy($row['has_central_entity'] ?? null)) {
            $flags[] = 'FC-R1:no-entity';
        }
        if (($row['page_type'] ?? '') === 'outer' && !$this->isTruthy($row['has_core_link'] ?? null)) {
            $flags[] = 'FC-R5:no-core-link';
        }
        if (!$this->isTruthy($row['h1_matches_title'] ?? null)) {
            $flags[] = 'FC-R7:h1-mismatch';
        }
        if (($row['page_type'] ?? '') === 'core') {
            if ((int) ($row['word_count'] ?? 0) < 500) {
                $flags[] = 'FC-R3:thin';
            }
            if (empty($row['h2s']) || $row['h2s'] === '[]') {
                $flags[] = 'FC-R8:no-h2';
            }
            if (empty($row['schema_types']) || $row['schema_types'] === '[]') {
                $flags[] = 'FC-R9:no-schema';
            }
        }

        return $flags;
    }

    private function summarizeRules(array $crawlData): array
    {
        $counts = [];

        foreach ($crawlData as $row) {
            $ruleIds = [];
            if (!empty($row['rule_ids'])) {
                $ruleIds = array_filter(array_map('trim', explode(',', (string) $row['rule_ids'])));
            } else {
                foreach ($this->extractRuleFlags($row) as $flag) {
                    $ruleIds[] = strstr($flag, ':', true) ?: $flag;
                }
            }

            foreach (array_unique($ruleIds) as $ruleId) {
                $counts[$ruleId][] = (string) ($row['url'] ?? '');
            }
        }

        return $counts;
    }

    private function buildRuleSummaryLine(string $ruleId, string $label, array $ruleCounts, array $crawlData): string
    {
        $urls = array_values(array_filter($ruleCounts[$ruleId] ?? []));
        if (empty($urls)) {
            return $ruleId . ' (' . $label . "): 0 pages\n";
        }

        return $ruleId . ' (' . $label . '): ' . count($urls) . ' pages - ' . implode(', ', array_slice($urls, 0, 5)) . "\n";
    }

    private function boolToPromptString(mixed $value): string
    {
        return $this->isTruthy($value) ? 'TRUE' : 'FALSE';
    }
}
