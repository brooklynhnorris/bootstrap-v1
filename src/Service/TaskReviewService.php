<?php

namespace App\Service;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\ParameterType;

class TaskReviewService
{
    private array $tableExistsCache = [];
    private array $pageContextCache = [];
    private array $rejectionStatsCache = [];
    private array $activeViolationCache = [];

    public function __construct(
        private Connection $db,
        private ViolationSnapshotService $violationSnapshotService,
    ) {
    }

    public function reviewPendingTasks(?string $assignee = null, int $limit = 50, array $statuses = ['pending']): array
    {
        $tasks = $this->loadTasks($assignee, $limit, $statuses);

        return array_map(fn (array $task) => $this->reviewTaskRow($task), $tasks);
    }

    public function reviewTaskById(int $taskId): ?array
    {
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$taskId]);

        if ($task === false) {
            return null;
        }

        return $this->reviewTaskRow($task);
    }

    public function buildDailySummary(?string $assignee = null, int $limit = 100): array
    {
        $reviews = $this->reviewPendingTasks($assignee, $limit, ['pending']);
        return $this->buildDailySummaryFromReviews($reviews, $assignee);
    }

    private function buildDailySummaryFromReviews(array $reviews, ?string $assignee): array
    {
        $verdictCounts = [
            'do' => 0,
            'reject' => 0,
            'wait' => 0,
            'revise_rule' => 0,
        ];
        $reasonCounts = [];
        $ruleCounts = [];

        foreach ($reviews as $review) {
            $verdict = $review['verdict'] ?? 'do';
            if (isset($verdictCounts[$verdict])) {
                $verdictCounts[$verdict]++;
            }

            foreach ($review['reason_codes'] ?? [] as $code) {
                $reasonCounts[$code] = ($reasonCounts[$code] ?? 0) + 1;
            }

            $ruleId = $review['task']['rule_id'] ?? null;
            if (is_string($ruleId) && $ruleId !== '' && in_array($verdict, ['reject', 'revise_rule'], true)) {
                $ruleCounts[$ruleId] = ($ruleCounts[$ruleId] ?? 0) + 1;
            }
        }

        arsort($reasonCounts);
        arsort($ruleCounts);

        $bestWork = array_slice(array_values(array_filter($reviews, fn (array $review) => ($review['verdict'] ?? '') === 'do')), 0, 5);
        $cleanup = array_slice(array_values(array_filter($reviews, fn (array $review) => in_array($review['verdict'] ?? '', ['reject', 'wait', 'revise_rule'], true))), 0, 10);

        return [
            'generated_at' => date('c'),
            'assignee' => $assignee,
            'board_health' => [
                'pending_reviewed' => count($reviews),
                'do' => $verdictCounts['do'],
                'reject' => $verdictCounts['reject'],
                'wait' => $verdictCounts['wait'],
                'revise_rule' => $verdictCounts['revise_rule'],
            ],
            'top_reason_codes' => $this->topCounts($reasonCounts, 8),
            'noisiest_rules' => $this->topCounts($ruleCounts, 5),
            'best_work_now' => array_map(fn (array $review) => $this->compactReview($review), $bestWork),
            'cleanup_queue' => array_map(fn (array $review) => $this->compactReview($review), $cleanup),
        ];
    }

    public function buildMorningBrief(?string $assignee = null, int $limit = 100): array
    {
        $reviews = $this->reviewPendingTasks($assignee, $limit, ['pending']);
        $dailySummary = $this->buildDailySummaryFromReviews($reviews, $assignee);

        $doNow = [];
        $waitForCrawl = [];
        $likelyFalsePositives = [];
        $resolvedOrOutdated = [];
        $rulesToRevise = [];
        $infrastructureBlockers = [];
        $rejectNow = [];
        $investigate = [];

        foreach ($reviews as $review) {
            $verdict = $review['verdict'] ?? 'do';
            $reasonCodes = $review['reason_codes'] ?? [];

            if ($verdict === 'do') {
                $doNow[] = $review;
            }

            if ($verdict === 'wait' && in_array('stale_crawl_data', $reasonCodes, true)) {
                $waitForCrawl[] = $review;
            }

            if ($verdict === 'wait' || ($verdict === 'reject' && in_array('missing_crawl_context', $reasonCodes, true))) {
                $investigate[] = $review;
            }

            if ($verdict === 'reject' && array_intersect($reasonCodes, [
                'asset_url_false_positive',
                'asset_or_bad_url',
                'page_type_mismatch',
                'no_video_on_page',
            ])) {
                $likelyFalsePositives[] = $review;
            }

            if ($verdict === 'reject' && in_array('no_active_violation', $reasonCodes, true)) {
                $resolvedOrOutdated[] = $review;
            }

            if ($verdict === 'reject') {
                $rejectNow[] = $review;
            }

            if ($verdict === 'revise_rule') {
                $rulesToRevise[] = $review;
            }

            if (in_array('stale_crawl_data', $reasonCodes, true) || in_array('missing_crawl_context', $reasonCodes, true)) {
                $infrastructureBlockers[] = $review;
            }
        }

        $staleCount = count(array_filter($reviews, fn (array $review) => in_array('stale_crawl_data', $review['reason_codes'] ?? [], true)));
        $missingContextCount = count(array_filter($reviews, fn (array $review) => in_array('missing_crawl_context', $review['reason_codes'] ?? [], true)));
        $suspectedDuplicates = $this->findSuspectedDuplicates($reviews);
        $pipelineStatusGuess = $this->buildPipelineStatusGuess(count($reviews), $staleCount, $missingContextCount);
        $boardCleanupPlan = $this->buildBoardCleanupPlan($reviews, $suspectedDuplicates);
        $rejectReasonBuckets = $this->groupReviewsByPrimaryReason($rejectNow);

        $operatingRecommendation = $this->buildOperatingRecommendation(
            $dailySummary['board_health'] ?? [],
            $staleCount,
            $missingContextCount,
            $doNow,
            $rulesToRevise,
            $likelyFalsePositives,
        );

        return [
            'generated_at' => date('c'),
            'assignee' => $assignee,
            'board_health' => $dailySummary['board_health'] ?? [],
            'do_now' => array_map(fn (array $review) => $this->compactReview($review), array_slice($doNow, 0, 8)),
            'do_now_total' => count($doNow),
            'reject_now' => array_map(fn (array $review) => $this->compactReview($review), $rejectNow),
            'reject_now_total' => count($rejectNow),
            'reject_reason_buckets' => $rejectReasonBuckets,
            'investigate' => array_map(fn (array $review) => $this->compactReview($review), $investigate),
            'investigate_total' => count($investigate),
            'wait_for_crawl' => array_map(fn (array $review) => $this->compactReview($review), array_slice($waitForCrawl, 0, 12)),
            'likely_false_positives' => array_map(fn (array $review) => $this->compactReview($review), array_slice($likelyFalsePositives, 0, 8)),
            'resolved_or_outdated_tasks' => array_map(fn (array $review) => $this->compactReview($review), array_slice($resolvedOrOutdated, 0, 12)),
            'rules_to_revise' => array_map(fn (array $review) => $this->compactReview($review), array_slice($rulesToRevise, 0, 8)),
            'board_actions' => [
                'execute_now' => array_values(array_filter(array_map(fn (array $review) => $review['task']['id'] ?? null, $doNow))),
                'reject_now' => array_values(array_filter(array_map(
                    fn (array $review) => in_array('no_active_violation', $review['reason_codes'] ?? [], true) ? ($review['task']['id'] ?? null) : null,
                    $reviews
                ))),
                'hold_for_crawl' => array_values(array_filter(array_map(
                    fn (array $review) => in_array('stale_crawl_data', $review['reason_codes'] ?? [], true) ? ($review['task']['id'] ?? null) : null,
                    $reviews
                ))),
                'investigate' => array_values(array_filter(array_map(
                    fn (array $review) => in_array('missing_crawl_context', $review['reason_codes'] ?? [], true) ? ($review['task']['id'] ?? null) : null,
                    $reviews
                ))),
            ],
            'suspected_duplicates' => $suspectedDuplicates,
            'board_cleanup_plan' => $boardCleanupPlan,
            'infrastructure_blockers' => [
                'stale_crawl_data' => $staleCount,
                'missing_crawl_context' => $missingContextCount,
            ],
            'pipeline_status_guess' => $pipelineStatusGuess,
            'top_reason_codes' => $dailySummary['top_reason_codes'] ?? [],
            'summary_consistency' => [
                'review_count' => count($reviews),
                'bucket_total' => count($doNow) + count($rejectNow) + count(array_filter($reviews, fn (array $review) => ($review['verdict'] ?? null) === 'wait')) + count($rulesToRevise),
                'reject_narrative_covers_all' => true,
                'investigate_narrative_covers_all' => true,
            ],
            'operating_recommendation' => $operatingRecommendation,
        ];
    }

    public function proposeRuleGaps(int $limit = 8): array
    {
        $limit = max(1, min($limit, 20));
        $ruleCorpusText = $this->loadRuleCorpusText();
        $ruleInventory = $this->loadRuleInventory();
        $signals = $this->loadRuleGapSignals();
        $guidelines = $this->buildRuleProposalGuidelines();
        $successMeasures = $this->buildRuleSuccessMeasures();
        $candidates = array_slice($this->buildRuleGapCandidates($ruleCorpusText, $signals), 0, $limit);

        return [
            'generated_at' => date('c'),
            'methodology' => [
                'scope' => 'Scans the current rule corpus, recent board-review patterns, and available first-party SEO signals to identify net-new rule opportunities or rule-family gaps.',
                'guardrails' => [
                    'Do not propose a new rule when the issue is already covered by an existing rule family or by generator hygiene.',
                    'Do not treat stale no_active_violation cards as proof of a bad rule.',
                    'If a proposal depends on unsupported evidence, label it needs_new_data instead of pretending it is executable now.',
                ],
            ],
            'current_rule_coverage' => [
                'rule_count' => count($ruleInventory),
                'categories' => $this->summarizeRuleInventory($ruleInventory),
                'coverage_markers' => $this->buildCoverageMarkers($ruleCorpusText),
            ],
            'signals' => $signals,
            'strict_guidelines' => $guidelines,
            'success_measures' => $successMeasures,
            'candidate_rules' => $candidates,
            'proposal_prompt' => $this->buildRuleGapPrompt($guidelines, $successMeasures, $candidates),
        ];
    }

    private function loadTasks(?string $assignee, int $limit, array $statuses): array
    {
        $limit = max(1, min($limit, 200));
        $statuses = array_values(array_filter(array_map('strval', $statuses), fn (string $status) => $status !== ''));
        if (empty($statuses)) {
            $statuses = ['pending'];
        }

        $sql = "SELECT * FROM tasks WHERE status IN (?)";
        $params = [$statuses];
        $types = [ArrayParameterType::STRING];

        if ($assignee !== null && trim($assignee) !== '') {
            $sql .= " AND assigned_to = ?";
            $params[] = $assignee;
            $types[] = ParameterType::STRING;
        }

        $sql .= " ORDER BY CASE priority
                    WHEN 'critical' THEN 0
                    WHEN 'urgent' THEN 0
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                    ELSE 4 END,
                  created_at DESC
                  LIMIT ?";
        $params[] = $limit;
        $types[] = ParameterType::INTEGER;

        return $this->db->fetchAllAssociative($sql, $params, $types);
    }

    private function loadRuleCorpusText(): string
    {
        $parts = [];

        if ($this->tableExists('seo_rules')) {
            try {
                $rows = $this->db->fetchAllAssociative(
                    "SELECT rule_id, name, full_text, diagnosis, action_output
                     FROM seo_rules
                     WHERE is_active = TRUE
                     ORDER BY rule_id ASC"
                );

                foreach ($rows as $row) {
                    $parts[] = implode("\n", array_filter([
                        (string) ($row['rule_id'] ?? ''),
                        (string) ($row['name'] ?? ''),
                        (string) ($row['full_text'] ?? ''),
                        (string) ($row['diagnosis'] ?? ''),
                        (string) ($row['action_output'] ?? ''),
                    ]));
                }
            } catch (\Throwable) {
                // Fall back to system-prompt text below.
            }
        }

        $systemPromptPath = dirname(__DIR__, 2) . DIRECTORY_SEPARATOR . 'system-prompt.txt';
        if (is_file($systemPromptPath)) {
            $content = @file_get_contents($systemPromptPath);
            if (is_string($content) && trim($content) !== '') {
                $parts[] = $content;
            }
        }

        return strtolower(implode("\n", $parts));
    }

    private function loadRuleInventory(): array
    {
        if ($this->tableExists('seo_rules')) {
            try {
                return $this->db->fetchAllAssociative(
                    "SELECT rule_id, name, category, priority
                     FROM seo_rules
                     WHERE is_active = TRUE
                     ORDER BY category ASC, rule_id ASC"
                );
            } catch (\Throwable) {
                // Fall through to parsing system prompt.
            }
        }

        $systemPromptPath = dirname(__DIR__, 2) . DIRECTORY_SEPARATOR . 'system-prompt.txt';
        if (!is_file($systemPromptPath)) {
            return [];
        }

        $content = @file_get_contents($systemPromptPath);
        if (!is_string($content) || trim($content) === '') {
            return [];
        }

        preg_match_all('/^([A-Z][A-Z0-9-]+)\s+\|\s+(.+)$/m', $content, $matches, PREG_SET_ORDER);
        $inventory = [];
        foreach ($matches as $match) {
            $inventory[] = [
                'rule_id' => trim((string) ($match[1] ?? '')),
                'name' => trim((string) ($match[2] ?? '')),
                'category' => $this->ruleFamilyLabel((string) ($match[1] ?? '')),
                'priority' => null,
            ];
        }

        return $inventory;
    }

    private function summarizeRuleInventory(array $ruleInventory): array
    {
        $counts = [];
        foreach ($ruleInventory as $rule) {
            $category = trim((string) ($rule['category'] ?? 'UNKNOWN'));
            if ($category === '') {
                $category = 'UNKNOWN';
            }
            $counts[$category] = ($counts[$category] ?? 0) + 1;
        }

        arsort($counts);

        $summary = [];
        foreach ($counts as $category => $count) {
            $summary[] = [
                'category' => $category,
                'count' => $count,
            ];
        }

        return $summary;
    }

    private function buildCoverageMarkers(string $ruleCorpusText): array
    {
        $markers = [
            'faqpage' => str_contains($ruleCorpusText, 'faqpage'),
            'videoobject' => str_contains($ruleCorpusText, 'videoobject'),
            'productgroup' => str_contains($ruleCorpusText, 'productgroup'),
            'aggregate_rating' => str_contains($ruleCorpusText, 'aggregaterating'),
            'author_date_article' => str_contains($ruleCorpusText, 'datepublished') || str_contains($ruleCorpusText, 'author'),
            'watch_page_key_moments' => str_contains($ruleCorpusText, 'seektoaction') || str_contains($ruleCorpusText, 'clip'),
            'answer_first_snippet' => str_contains($ruleCorpusText, 'answer-first') || str_contains($ruleCorpusText, 'featured snippet'),
            'schema_property_level_audit' => str_contains($ruleCorpusText, 'offershippingdetails') || str_contains($ruleCorpusText, 'merchantreturnpolicy'),
            'ai_overview_proxy_tracking' => str_contains($ruleCorpusText, 'ai overview') || str_contains($ruleCorpusText, 'citation share'),
        ];

        $present = [];
        $missing = [];
        foreach ($markers as $marker => $value) {
            if ($value) {
                $present[] = $marker;
            } else {
                $missing[] = $marker;
            }
        }

        return [
            'present' => $present,
            'missing' => $missing,
        ];
    }

    private function loadRuleGapSignals(): array
    {
        $signals = [
            'high_impression_outer_pages' => 0,
            'high_impression_outer_low_ctr_pages' => 0,
            'core_pages' => 0,
            'core_pages_with_schema' => 0,
            'pending_rule_proposals' => 0,
            'recent_rule_feedback_items' => 0,
        ];

        if ($this->tableExists('page_facts')) {
            try {
                $signals['high_impression_outer_pages'] = (int) $this->db->fetchOne(
                    "SELECT COUNT(*) FROM page_facts
                     WHERE page_type = 'outer'
                       AND is_indexable = TRUE
                       AND COALESCE(target_query_impressions, 0) >= 500"
                );

                $signals['high_impression_outer_low_ctr_pages'] = (int) $this->db->fetchOne(
                    "SELECT COUNT(*) FROM page_facts
                     WHERE page_type = 'outer'
                       AND is_indexable = TRUE
                       AND COALESCE(target_query_impressions, 0) >= 500
                       AND COALESCE(target_query_position, 999) BETWEEN 4 AND 15
                       AND (
                         CASE
                           WHEN COALESCE(target_query_impressions, 0) = 0 THEN 0
                           ELSE COALESCE(target_query_clicks, 0)::numeric / NULLIF(target_query_impressions, 0)
                         END
                       ) < 0.015"
                );

                $signals['core_pages'] = (int) $this->db->fetchOne(
                    "SELECT COUNT(*) FROM page_facts
                     WHERE page_type = 'core'
                       AND is_indexable = TRUE"
                );

                $signals['core_pages_with_schema'] = (int) $this->db->fetchOne(
                    "SELECT COUNT(*) FROM page_facts
                     WHERE page_type = 'core'
                       AND is_indexable = TRUE
                       AND schema_types IS NOT NULL
                       AND schema_types::text <> '[]'"
                );
            } catch (\Throwable) {
                // Keep defaults.
            }
        }

        if ($this->tableExists('rule_change_proposals')) {
            try {
                $signals['pending_rule_proposals'] = (int) $this->db->fetchOne(
                    "SELECT COUNT(*) FROM rule_change_proposals WHERE status = 'pending'"
                );
            } catch (\Throwable) {
                // Keep defaults.
            }
        }

        if ($this->tableExists('rule_feedback')) {
            try {
                $signals['recent_rule_feedback_items'] = (int) $this->db->fetchOne(
                    "SELECT COUNT(*) FROM rule_feedback
                     WHERE created_at >= NOW() - INTERVAL '30 days'"
                );
            } catch (\Throwable) {
                // Keep defaults.
            }
        }

        return $signals;
    }

    private function buildRuleProposalGuidelines(): array
    {
        return [
            [
                'rule' => 'Only propose net-new rules for gaps that are not already covered by an existing active rule family or by generator hygiene.',
                'source' => 'Logiri reviewer contract',
            ],
            [
                'rule' => 'Success gates must be measurable with first-party data already in the system, or explicitly marked needs_new_data.',
                'source' => 'Logiri reviewer contract',
            ],
            [
                'rule' => 'Do not ask the operator to manually verify crawl, word count, link counts, or schema types that the system already stores.',
                'source' => 'Logiri reviewer contract',
            ],
            [
                'rule' => 'FAQPage must not be treated as a primary Google rich-result KPI for DDT; Google limits FAQ rich results to well-known health or government sites.',
                'source' => 'Google Search Central FAQPage docs',
                'url' => 'https://developers.google.com/search/docs/appearance/structured-data/faqpage',
            ],
            [
                'rule' => 'VideoObject should only be proposed on a true watch page where users can actually watch the video.',
                'source' => 'Google Search Central Video structured data docs',
                'url' => 'https://developers.google.com/search/docs/appearance/structured-data/video',
            ],
            [
                'rule' => 'Structured data must match visible on-page content; a rule cannot propose markup that is absent from the rendered page.',
                'source' => 'Google general structured data guidelines',
                'url' => 'https://developers.google.com/search/docs/appearance/structured-data/sd-policies',
            ],
            [
                'rule' => 'Product-group or merchant-listing rules should be scoped only when variant or offer data can be validated, not inferred.',
                'source' => 'Google Product/ProductGroup docs',
                'url' => 'https://developers.google.com/search/docs/appearance/structured-data/product-variants',
            ],
            [
                'rule' => 'AI appearance metrics are mostly proxy metrics unless a platform explicitly reports the feature; do not claim direct AI citation wins from CTR or impressions alone.',
                'source' => 'Logiri reviewer contract',
            ],
        ];
    }

    private function buildRuleSuccessMeasures(): array
    {
        return [
            [
                'measure' => 'Indexability and canonical integrity',
                'minimum_gate' => '200 status, self-referencing canonical when appropriate, no conflicting noindex.',
                'why_it_matters' => 'No SEO or AI-appearance rule matters if the page cannot be crawled or consolidated correctly.',
            ],
            [
                'measure' => 'Query visibility',
                'minimum_gate' => 'Track impressions, clicks, average position, and CTR on the exact canonical URL.',
                'why_it_matters' => 'These are the closest first-party measures of whether the page is entering the right search conversations.',
            ],
            [
                'measure' => 'Rich-result eligibility',
                'minimum_gate' => 'Structured data validates cleanly, matches visible content, and uses a supported schema on an eligible page type.',
                'why_it_matters' => 'Markup only helps when it is both valid and relevant to the rendered page.',
            ],
            [
                'measure' => 'Opening-snippet answer quality',
                'minimum_gate' => 'The first sentence or opening block states the page entity and answer plainly enough to be extracted.',
                'why_it_matters' => 'Answer-first openings improve both snippet clarity and AI extraction readiness.',
            ],
            [
                'measure' => 'Entity coherence',
                'minimum_gate' => 'H1, title, opening body text, schema, and internal linking all reinforce the same subject.',
                'why_it_matters' => 'AI systems and crawlers both struggle when entity signals conflict.',
            ],
            [
                'measure' => 'Feature-specific eligibility',
                'minimum_gate' => 'Video features require watch pages; product features require visible product data; FAQ should not be scored as a primary Google win for DDT.',
                'why_it_matters' => 'Feature eligibility is not universal, and false assumptions create noisy tasks.',
            ],
        ];
    }

    private function buildRuleGapCandidates(string $ruleCorpusText, array $signals): array
    {
        $candidates = [];

        if (!$this->corpusHasAny($ruleCorpusText, ['answer-first', 'featured snippet', 'snippet-ready', 'answer block'])) {
            $candidates[] = [
                'candidate_id' => 'AIS-010',
                'name' => 'Answer-First Opening Block Governance for High-Impression Outer Pages',
                'priority' => 'high',
                'supported_now' => true,
                'needs_new_data' => [],
                'why_this_gap_exists' => 'The current rule set addresses snippets, CTR, and opening entity language in pieces, but it does not explicitly govern whether high-impression outer pages open with an extractable answer block.',
                'evidence' => [
                    'high_impression_outer_pages' => $signals['high_impression_outer_pages'] ?? 0,
                    'high_impression_outer_low_ctr_pages' => $signals['high_impression_outer_low_ctr_pages'] ?? 0,
                ],
                'trigger_concept' => 'Outer pages with strong impressions and middling positions/CTR where the opening sentence does not clearly define or answer the target query.',
                'strict_guidelines' => [
                    'Only fire on outer pages with live impression evidence.',
                    'Use first_sentence_text or opening snippet fields already in crawl data.',
                    'Do not force FAQ schema or extra links as a substitute for weak opening copy.',
                ],
                'success_gate' => 'Opening sentence explicitly names the query entity and answer pattern; 28-day CTR lift on the same canonical URL is the business KPI.',
            ];
        }

        if (!$this->corpusHasAny($ruleCorpusText, ['offershippingdetails', 'merchantreturnpolicy', 'productgroup'])) {
            $candidates[] = [
                'candidate_id' => 'DDT-SD-010',
                'name' => 'Schema Property Completeness Audit for Product and Variant Pages',
                'priority' => 'high',
                'supported_now' => false,
                'needs_new_data' => ['Schema property-level extraction beyond schema type names'],
                'why_this_gap_exists' => 'The current rule set mostly checks schema type presence, not whether important supported properties are actually complete or visible.',
                'evidence' => [
                    'core_pages' => $signals['core_pages'] ?? 0,
                    'core_pages_with_schema' => $signals['core_pages_with_schema'] ?? 0,
                ],
                'trigger_concept' => 'Core or variant-capable pages with Product/ProductGroup markup missing property-level completeness for supported commerce signals.',
                'strict_guidelines' => [
                    'Only propose property-level schema when the content is visible on the page.',
                    'Do not infer return, shipping, or offer details that are not present in source content.',
                    'Score success on validation plus supported-property completeness, not on rich-result appearance alone.',
                ],
                'success_gate' => 'Property-level schema completeness passes internal validation with zero unsupported or invisible fields.',
            ];
        }

        if (!$this->corpusHasAny($ruleCorpusText, ['seektoaction', 'key moments', 'clip'])) {
            $candidates[] = [
                'candidate_id' => 'MAO-R8',
                'name' => 'Video Watch-Page and Key-Moments Eligibility Governance',
                'priority' => 'medium',
                'supported_now' => false,
                'needs_new_data' => ['Embedded video ID extraction', 'Deep-link/timestamp support on watch pages'],
                'why_this_gap_exists' => 'Current video governance mostly stops at VideoObject presence. It does not distinguish true watch pages from generic embeds or evaluate key-moments eligibility.',
                'evidence' => [
                    'pending_rule_proposals' => $signals['pending_rule_proposals'] ?? 0,
                ],
                'trigger_concept' => 'Pages with valid embedded video content where users can watch the video and the page could support deeper video enhancements.',
                'strict_guidelines' => [
                    'Never fire on pages without a watchable video.',
                    'Do not generate video-schema tasks for assets or decorative embeds.',
                    'Treat VideoObject, Clip, and SeekToAction as separate capabilities with different requirements.',
                ],
                'success_gate' => 'Watch-page eligibility confirmed; video markup validates cleanly; key moments only proposed when deep links are technically supported.',
            ];
        }

        if (!$this->corpusHasAny($ruleCorpusText, ['datepublished', 'datemodified', 'author'])) {
            $candidates[] = [
                'candidate_id' => 'DDT-EEAT-09',
                'name' => 'Author, Date, and Editorial Freshness Governance for Outer Articles',
                'priority' => 'medium',
                'supported_now' => false,
                'needs_new_data' => ['Author/byline extraction from rendered pages'],
                'why_this_gap_exists' => 'The current EEAT rules cover testimonials, trust pages, and terminology, but not article-level authorship and freshness signals.',
                'evidence' => [
                    'high_impression_outer_pages' => $signals['high_impression_outer_pages'] ?? 0,
                    'recent_rule_feedback_items' => $signals['recent_rule_feedback_items'] ?? 0,
                ],
                'trigger_concept' => 'Informational outer pages with durable search demand but weak editorial provenance signals.',
                'strict_guidelines' => [
                    'Only propose author/date schema or visible bylines when the site actually exposes that information.',
                    'Use freshness as a relevance aid, not a license to churn content without purpose.',
                    'Separate editorial provenance from generic schema inflation.',
                ],
                'success_gate' => 'Visible byline and publish/update dates align with any Article schema; freshness changes correlate with stable or improving query visibility.',
            ];
        }

        if (!$this->corpusHasAny($ruleCorpusText, ['ai overview', 'citation share', 'serp feature owner'])) {
            $candidates[] = [
                'candidate_id' => 'AIS-011',
                'name' => 'AI Overview and SERP Feature Proxy Watchlist',
                'priority' => 'medium',
                'supported_now' => false,
                'needs_new_data' => ['SERP feature capture by query, including AI Overview presence or owner'],
                'why_this_gap_exists' => 'The current system uses indirect SEO metrics well, but it does not explicitly track feature-level AI or SERP ownership changes on target queries.',
                'evidence' => [
                    'high_impression_outer_low_ctr_pages' => $signals['high_impression_outer_low_ctr_pages'] ?? 0,
                ],
                'trigger_concept' => 'Priority query families where DDT has impressions but feature ownership may be suppressing clicks above organic results.',
                'strict_guidelines' => [
                    'Use feature tracking as a measurement layer, not as proof of causality.',
                    'Do not claim AI Overview wins without feature-level evidence.',
                    'Tie any remediation play back to canonical pages and measurable query clusters.',
                ],
                'success_gate' => 'Feature ownership or feature presence is captured consistently enough to compare before/after query behavior.',
            ];
        }

        return $candidates;
    }

    private function corpusHasAny(string $ruleCorpusText, array $needles): bool
    {
        foreach ($needles as $needle) {
            if (str_contains($ruleCorpusText, strtolower($needle))) {
                return true;
            }
        }

        return false;
    }

    private function buildRuleGapPrompt(array $guidelines, array $successMeasures, array $candidates): string
    {
        $guidelineLines = [];
        foreach ($guidelines as $guideline) {
            $line = '- ' . ($guideline['rule'] ?? '');
            if (!empty($guideline['source'])) {
                $line .= ' [' . $guideline['source'] . ']';
            }
            $guidelineLines[] = $line;
        }

        $measureLines = [];
        foreach ($successMeasures as $measure) {
            $measureLines[] = sprintf(
                '- %s: %s Success gate: %s',
                (string) ($measure['measure'] ?? 'Measure'),
                (string) ($measure['why_it_matters'] ?? ''),
                (string) ($measure['minimum_gate'] ?? '')
            );
        }

        $candidateLines = [];
        foreach ($candidates as $candidate) {
            $candidateLines[] = sprintf(
                '- %s | %s | Priority: %s | Supported now: %s | Needs new data: %s | Gap: %s',
                (string) ($candidate['candidate_id'] ?? 'CANDIDATE'),
                (string) ($candidate['name'] ?? 'Unnamed'),
                (string) ($candidate['priority'] ?? 'medium'),
                !empty($candidate['supported_now']) ? 'yes' : 'no',
                empty($candidate['needs_new_data']) ? 'none' : implode(', ', (array) $candidate['needs_new_data']),
                (string) ($candidate['why_this_gap_exists'] ?? '')
            );
        }

        return implode("\n", [
            'You are Logiri\'s proposed-rule engine.',
            '',
            'Your task is to propose only net-new rules or genuine rule-family gap fills that are not already covered by the active rule corpus or by generator hygiene.',
            '',
            'STRICT GUIDELINES',
            implode("\n", $guidelineLines),
            '',
            'SUCCESS MEASURES',
            implode("\n", $measureLines),
            '',
            'CURRENT GAP CANDIDATES',
            implode("\n", $candidateLines),
            '',
            'OUTPUT RULES',
            '- Propose at most 5 rules.',
            '- For each rule, state whether it is executable now or needs_new_data.',
            '- Use only measurable triggers and explicit suppression conditions.',
            '- Never use FAQ rich-result appearance as a primary KPI for Double D Trailers.',
            '- Never propose schema that is unsupported, invisible on-page, or ineligible for the page type.',
            '- If the proposal depends on AI-appearance tracking not present in first-party data, declare it as a measurement-layer rule and list the data source gap.',
            '',
            'Return JSON with: summary, candidate_rules, dropped_ideas, required_new_data, and why each proposal materially improves SEO or AI-appearance readiness.',
        ]);
    }

    private function reviewTaskRow(array $task): array
    {
        $title = (string) ($task['title'] ?? '');
        $ruleId = $this->normalizeRuleId($task['rule_id'] ?? null) ?? $this->extractRuleId($title);
        $rawUrl = $this->extractRawUrl($title);
        $url = $rawUrl !== null ? $this->violationSnapshotService->normalizeUrl($rawUrl) : null;
        $page = $url !== null ? $this->loadPageContext($url) : null;
        $rejectionStats = $this->loadRejectionStats($url, $ruleId, $page['page_type'] ?? null);
        $activeViolation = ($url !== null && $ruleId !== null) ? $this->findActiveViolationCached($url, $ruleId) : null;

        $verdict = 'do';
        $confidence = 0.72;
        $reasonCodes = [];
        $summaryParts = [];
        $recommendedAction = 'work_now';
        $ruleFollowup = ['needed' => false];

        if ($url !== null && $this->looksLikeAssetUrl($url)) {
            $verdict = 'reject';
            $confidence = 0.99;
            $reasonCodes[] = 'asset_url_false_positive';
            $summaryParts[] = 'Task targets an asset-style URL rather than a normal content page.';
            $recommendedAction = 'reject_and_learn';
            $ruleFollowup = $this->buildRuleFollowup($ruleId, 'guardrail_update', 'Suppress asset and upload URLs before tasks are generated.');
        }

        if ($rawUrl !== null && $url !== null && $this->isMalformedTaskUrl($rawUrl, $url)) {
            $verdict = 'reject';
            $confidence = 0.98;
            $reasonCodes[] = 'malformed_task_url';
            $summaryParts[] = 'Task URL looks malformed or placeholder-like rather than a real board page slug.';
            $recommendedAction = 'reject_and_learn';
            $ruleFollowup = $this->buildRuleFollowup($ruleId, 'guardrail_update', 'Suppress malformed or placeholder URLs such as /Title/, leaked host paths, and root-leakage variants before task creation.');
        }

        if ($url !== null && $this->looksLikeNonPageUtilityUrl($url)) {
            $verdict = 'reject';
            $confidence = 0.98;
            $reasonCodes[] = 'non_page_utility_url';
            $summaryParts[] = 'Task targets a non-page utility or file endpoint rather than a crawlable content page.';
            $recommendedAction = 'reject_and_learn';
            $ruleFollowup = $this->buildRuleFollowup($ruleId, 'guardrail_update', 'Suppress non-page utility endpoints such as /scripts/*.html and similar file-style paths before task creation.');
        }

        if ($page === null && $url !== null && $verdict === 'do') {
            $verdict = 'wait';
            $confidence = 0.88;
            $reasonCodes[] = 'missing_crawl_context';
            $summaryParts[] = 'No current page snapshot is available for this URL, so the task cannot be verified against live crawl facts yet.';
            $recommendedAction = 'wait_for_recrawl';
        }

        if ($page !== null) {
            $crawlAgeHours = $this->computeAgeHours($page['last_crawled_at'] ?? $page['crawled_at'] ?? null);
            if ($crawlAgeHours !== null && $crawlAgeHours > 48 && $verdict === 'do') {
                $verdict = 'wait';
                $confidence = 0.8;
                $reasonCodes[] = 'stale_crawl_data';
                $summaryParts[] = sprintf('Latest crawl context is %.1f hours old.', $crawlAgeHours);
                $recommendedAction = 'wait_for_recrawl';
            }
        }

        if ($ruleId !== null && $url !== null && $page !== null && $activeViolation === null && $verdict === 'do') {
            $verdict = 'reject';
            $confidence = 0.92;
            $reasonCodes[] = 'no_active_violation';
            $summaryParts[] = 'Latest deterministic violation snapshot does not show this rule currently failing on the target URL.';
            $recommendedAction = 'reject_and_learn';
        }

        if (($rejectionStats['same_rule_url_rejections'] ?? 0) >= 2) {
            $verdict = 'revise_rule';
            $confidence = max($confidence, 0.93);
            $reasonCodes[] = 'repeated_rule_url_rejections';
            $summaryParts[] = 'This rule/url pair has already been rejected multiple times, which points to a generation or rule-scope problem rather than another execution pass.';
            $recommendedAction = 'revise_rule';
            $ruleFollowup = $this->buildRuleFollowup($ruleId, 'modify_diagnosis', 'Repeated rejections on the same URL suggest the rule needs tighter gating or different branching.');
        }

        $topGuardrail = $rejectionStats['top_guardrail_code'] ?? null;
        if ($topGuardrail !== null) {
            if (in_array($topGuardrail, ['missing_payload', 'manual_serp_check', 'vague_placement'], true)) {
                $verdict = 'revise_rule';
                $confidence = max($confidence, 0.9);
                $reasonCodes[] = $topGuardrail;
                $summaryParts[] = 'Recent rejection history shows this type of task is under-specified for execution.';
                $recommendedAction = 'revise_rule';
                $ruleFollowup = $this->buildRuleFollowup($ruleId, 'modify_action', 'Task output should be self-sufficient and not depend on manual discovery or missing brief payloads.');
            } elseif (in_array($topGuardrail, ['asset_or_bad_url', 'page_type_mismatch', 'no_video_on_page'], true)) {
                $verdict = 'reject';
                $confidence = max($confidence, 0.94);
                $reasonCodes[] = $topGuardrail;
                $summaryParts[] = 'Recent rejection history shows this pattern is usually a false positive for this rule or page type.';
                $recommendedAction = 'reject_and_learn';
                $ruleFollowup = $this->buildRuleFollowup($ruleId, 'guardrail_update', 'Add stronger page-type and content-presence gates before task creation.');
            }
        }

        if ($this->containsManualDecisionGate($title, (string) ($task['description'] ?? ''))) {
            $verdict = 'revise_rule';
            $confidence = max($confidence, 0.84);
            $reasonCodes[] = 'manual_decision_gate';
            $summaryParts[] = 'The task asks the operator to resolve an upstream decision that should be derived before task creation.';
            $recommendedAction = 'revise_rule';
        }

        if ($this->mentionsMissingPayload((string) ($task['description'] ?? ''))) {
            $verdict = 'revise_rule';
            $confidence = max($confidence, 0.9);
            $reasonCodes[] = 'missing_payload';
            $summaryParts[] = 'The task references a missing copy or schema payload, so it is not execution-ready.';
            $recommendedAction = 'revise_rule';
            $ruleFollowup = $this->buildRuleFollowup($ruleId, 'modify_action', 'Do not emit tasks that require a play brief payload unless that payload is included.');
        }

        if ($summaryParts === []) {
            $summaryParts[] = 'Task aligns with the latest available rule and crawl evidence and looks ready for normal execution.';
        }

        $pageEvidence = $page !== null ? [
            'url' => $page['url'] ?? $url,
            'page_type' => $page['page_type'] ?? null,
            'word_count' => isset($page['word_count']) ? (int) $page['word_count'] : null,
            'target_query_impressions' => isset($page['target_query_impressions']) ? (int) $page['target_query_impressions'] : null,
            'last_crawled_at' => $page['last_crawled_at'] ?? $page['crawled_at'] ?? null,
            'context_source' => $page['context_source'] ?? null,
            'title_tag' => $page['title_tag'] ?? null,
            'h1' => $page['h1'] ?? null,
        ] : null;

        return [
            'task' => [
                'id' => (int) ($task['id'] ?? 0),
                'title' => $title,
                'status' => $task['status'] ?? null,
                'priority' => $task['priority'] ?? null,
                'assigned_to' => $task['assigned_to'] ?? null,
                'rule_id' => $ruleId,
                'url' => $url,
                'raw_url' => $rawUrl,
            ],
            'verdict' => $verdict,
            'confidence' => round($confidence, 2),
            'reason_codes' => array_values(array_unique($reasonCodes)),
            'human_summary' => implode(' ', $summaryParts),
            'recommended_action' => $recommendedAction,
            'rule_followup' => $ruleFollowup,
            'evidence' => [
                'page' => $pageEvidence,
                'active_violation' => $activeViolation,
                'rejection_stats' => $rejectionStats,
            ],
        ];
    }

    private function loadPageContext(string $url): ?array
    {
        $normalizedUrl = $this->violationSnapshotService->normalizeUrl($url);
        if (array_key_exists($normalizedUrl, $this->pageContextCache)) {
            return $this->pageContextCache[$normalizedUrl];
        }
        $candidates = [];

        if ($this->tableExists('page_facts')) {
            $page = $this->db->fetchAssociative(
                "SELECT url, page_type, word_count, h1, title_tag, target_query_impressions, last_crawled_at,
                        'page_facts' AS context_source
                 FROM page_facts
                 WHERE url = ?
                 LIMIT 1",
                [$normalizedUrl]
            );
            if ($page !== false) {
                $page['context_timestamp'] = $page['last_crawled_at'] ?? null;
                $candidates[] = $page;
            }
        }

        if ($this->tableExists('page_crawl_snapshots')) {
            $page = $this->db->fetchAssociative(
                "SELECT url, page_type, word_count, h1, title_tag, target_query_impressions, crawled_at,
                        'page_crawl_snapshots' AS context_source
                 FROM page_crawl_snapshots
                 WHERE url = ?
                 ORDER BY crawled_at DESC NULLS LAST, id DESC
                 LIMIT 1",
                [$normalizedUrl]
            );
            if ($page !== false) {
                $page['last_crawled_at'] = $page['crawled_at'] ?? null;
                $page['context_timestamp'] = $page['crawled_at'] ?? null;
                $candidates[] = $page;
            }
        }

        if ($candidates === []) {
            $this->pageContextCache[$normalizedUrl] = null;
            return null;
        }

        usort($candidates, function (array $a, array $b): int {
            $aTime = strtotime((string) ($a['context_timestamp'] ?? '')) ?: 0;
            $bTime = strtotime((string) ($b['context_timestamp'] ?? '')) ?: 0;
            return $bTime <=> $aTime;
        });

        $this->pageContextCache[$normalizedUrl] = $candidates[0];
        return $this->pageContextCache[$normalizedUrl];
    }

    private function loadRejectionStats(?string $url, ?string $ruleId, ?string $pageType): array
    {
        $cacheKey = json_encode([$url, $ruleId, $pageType], JSON_UNESCAPED_SLASHES);
        if (is_string($cacheKey) && array_key_exists($cacheKey, $this->rejectionStatsCache)) {
            return $this->rejectionStatsCache[$cacheKey];
        }

        if (!$this->tableExists('task_rejections')) {
            $result = [
                'same_rule_url_rejections' => 0,
                'same_rule_page_type_rejections' => 0,
                'top_guardrail_code' => null,
            ];
            if (is_string($cacheKey)) {
                $this->rejectionStatsCache[$cacheKey] = $result;
            }
            return $result;
        }

        $stats = [
            'same_rule_url_rejections' => 0,
            'same_rule_page_type_rejections' => 0,
            'top_guardrail_code' => null,
        ];

        if ($url !== null && $ruleId !== null) {
            $stats['same_rule_url_rejections'] = (int) $this->db->fetchOne(
                "SELECT COUNT(*) FROM task_rejections
                 WHERE rule_id = ? AND url = ?",
                [$ruleId, $this->violationSnapshotService->normalizeUrl($url)]
            );

            $topGuardrail = $this->db->fetchAssociative(
                "SELECT guardrail_code, COUNT(*) AS occurrences
                 FROM task_rejections
                 WHERE rule_id = ? AND url = ? AND guardrail_code IS NOT NULL
                 GROUP BY guardrail_code
                 ORDER BY occurrences DESC, guardrail_code ASC
                 LIMIT 1",
                [$ruleId, $this->violationSnapshotService->normalizeUrl($url)]
            );
            if ($topGuardrail !== false) {
                $stats['top_guardrail_code'] = $topGuardrail['guardrail_code'] ?? null;
            }
        }

        if ($ruleId !== null && is_string($pageType) && $pageType !== '') {
            $stats['same_rule_page_type_rejections'] = (int) $this->db->fetchOne(
                "SELECT COUNT(*) FROM task_rejections
                 WHERE rule_id = ? AND page_type = ?",
                [$ruleId, $pageType]
            );
        }

        if (is_string($cacheKey)) {
            $this->rejectionStatsCache[$cacheKey] = $stats;
        }

        return $stats;
    }

    private function compactReview(array $review): array
    {
        return [
            'task_id' => $review['task']['id'] ?? null,
            'title' => $review['task']['title'] ?? '',
            'rule_id' => $review['task']['rule_id'] ?? null,
            'url' => $review['task']['url'] ?? null,
            'raw_url' => $review['task']['raw_url'] ?? null,
            'verdict' => $review['verdict'] ?? null,
            'confidence' => $review['confidence'] ?? null,
            'reason_codes' => $review['reason_codes'] ?? [],
            'summary' => $review['human_summary'] ?? '',
            'context_source' => $review['evidence']['page']['context_source'] ?? null,
        ];
    }

    private function topCounts(array $counts, int $limit): array
    {
        $rows = [];
        foreach (array_slice($counts, 0, $limit, true) as $key => $count) {
            $rows[] = ['key' => $key, 'count' => $count];
        }

        return $rows;
    }

    private function extractUrl(string $title): ?string
    {
        if (preg_match('/[—-]\s*(\/)\s*$/u', $title)) {
            return '/';
        }

        if (preg_match('|(/[a-z0-9][a-z0-9_./-]*/?)|i', $title, $matches)) {
            return $this->violationSnapshotService->normalizeUrl($matches[1]);
        }

        return null;
    }

    private function extractRawUrl(string $title): ?string
    {
        if (preg_match('/[â€”-]\s*(\/)\s*$/u', $title)) {
            return '/';
        }

        if (preg_match('|(/[a-z0-9][a-z0-9_./-]*/?)|i', $title, $matches)) {
            return $matches[1];
        }

        return null;
    }

    private function extractRuleId(string $title): ?string
    {
        if (preg_match('/^\[([A-Z]+-[A-Za-z0-9]+)\]/', $title, $matches)) {
            return strtoupper(trim($matches[1]));
        }

        return null;
    }

    private function normalizeRuleId(mixed $ruleId): ?string
    {
        if (!is_string($ruleId)) {
            return null;
        }

        $ruleId = strtoupper(trim($ruleId));

        return $ruleId === '' ? null : $ruleId;
    }

    private function looksLikeAssetUrl(string $url): bool
    {
        $lower = strtolower($url);

        return str_contains($lower, '/wp-content/uploads/')
            || preg_match('/\.(jpg|jpeg|png|gif|webp|svg|pdf)$/', $lower) === 1;
    }

    private function looksLikeNonPageUtilityUrl(string $url): bool
    {
        $lower = strtolower($url);

        return str_starts_with($lower, '/scripts/')
            || str_starts_with($lower, '/wp-json/')
            || str_starts_with($lower, '/wp-admin/')
            || preg_match('/\.(html|json|xml|txt)$/', $lower) === 1;
    }

    private function isMalformedTaskUrl(string $rawUrl, string $normalizedUrl): bool
    {
        $rawUrl = trim($rawUrl);
        $rawLower = strtolower($rawUrl);
        $normalizedLower = strtolower($normalizedUrl);

        if ($rawLower === '') {
            return false;
        }

        if (preg_match('#^/(title|url|slug|page|placeholder)/?$#', $normalizedLower) === 1) {
            return true;
        }

        if (preg_match('#(?:^|/)(?:www\.)?doubledtrailers\.com(?:/|$)#', $rawLower) === 1) {
            return true;
        }

        if ($normalizedLower === '/' && !preg_match('#^(?:https?://)?(?:www\.)?doubledtrailers\.com/?$#', $rawLower)) {
            return true;
        }

        if (preg_match('#^/[A-Z][A-Za-z0-9-]*/?$#', $rawUrl) === 1) {
            return true;
        }

        return false;
    }

    private function containsManualDecisionGate(string $title, string $description): bool
    {
        $text = strtolower($title . "\n" . $description);

        return str_contains($text, 'confirm with brad')
            || str_contains($text, 'report back what you see')
            || str_contains($text, 'search incognito')
            || str_contains($text, 'before you write a word: confirm')
            || str_contains($text, 'if merge isn\'t feasible this cycle');
    }

    private function mentionsMissingPayload(string $description): bool
    {
        $text = strtolower($description);

        return str_contains($text, 'play brief is missing')
            || str_contains($text, 'there is nothing in the play brief')
            || str_contains($text, 'copy block from the play brief')
            || str_contains($text, 'schema block from the play brief')
            || str_contains($text, 'do not paraphrase it');
    }

    private function computeAgeHours(?string $timestamp): ?float
    {
        if (!is_string($timestamp) || trim($timestamp) === '') {
            return null;
        }

        $time = strtotime($timestamp);
        if ($time === false) {
            return null;
        }

        return round((time() - $time) / 3600, 1);
    }

    private function buildRuleFollowup(?string $ruleId, string $type, string $summary): array
    {
        if ($ruleId === null) {
            return ['needed' => false];
        }

        return [
            'needed' => true,
            'rule_id' => $ruleId,
            'type' => $type,
            'summary' => $summary,
        ];
    }

    private function buildOperatingRecommendation(
        array $boardHealth,
        int $staleCount,
        int $missingContextCount,
        array $doNow,
        array $rulesToRevise,
        array $likelyFalsePositives
    ): string {
        $pendingReviewed = (int) ($boardHealth['pending_reviewed'] ?? 0);
        $doCount = (int) ($boardHealth['do'] ?? 0);

        if ($pendingReviewed > 0 && ($staleCount + $missingContextCount) >= max(3, (int) floor($pendingReviewed * 0.6))) {
            if ($doCount > 0) {
                return sprintf(
                    'Most of the board is blocked by crawl freshness rather than bad tasks. Work the %d execution-ready items first, then trigger or investigate the crawl pipeline before spending time triaging the rest.',
                    count($doNow)
                );
            }

            return 'The board is mostly blocked by crawl freshness or missing page snapshots. Refresh the crawl pipeline first, then rerun the reviewer before spending time on manual task cleanup.';
        }

        if ($rulesToRevise !== []) {
            return 'The main bottleneck is rule quality, not operator throughput. Review the flagged rule revisions first so the board does not keep generating under-specified work.';
        }

        if (count($likelyFalsePositives) === 0 && count($doNow) > 0 && ($boardHealth['reject'] ?? 0) > 0) {
            return 'Fresh crawl evidence is available, and a large share of the board now looks outdated rather than blocked. Execute the small set of still-valid tasks, then clear the tasks whose violations no longer exist in the latest snapshot.';
        }

        if ($likelyFalsePositives !== []) {
            return 'False positives are starting to accumulate. Reject the clearly invalid tasks and use them to tighten page-type and asset-url guardrails before the next generation cycle.';
        }

        if ($doNow !== []) {
            return 'The board has actionable work available now. Execute the highest-confidence items first, then rerun the morning brief to see whether the remaining queue is still structurally blocked.';
        }

        return 'No clear execution tranche stands out yet. Re-run the brief after the next crawl or after any major board mutations so the reviewer can recompute priorities from fresher evidence.';
    }

    private function groupReviewsByPrimaryReason(array $reviews): array
    {
        $groups = [];

        foreach ($reviews as $review) {
            $reasonCodes = $review['reason_codes'] ?? [];
            $primaryReason = is_array($reasonCodes) && $reasonCodes !== [] ? (string) reset($reasonCodes) : 'unknown';

            if (!isset($groups[$primaryReason])) {
                $groups[$primaryReason] = [
                    'reason_code' => $primaryReason,
                    'count' => 0,
                    'tasks' => [],
                ];
            }

            $groups[$primaryReason]['count']++;
            $groups[$primaryReason]['tasks'][] = $this->compactReview($review);
        }

        usort($groups, fn (array $left, array $right): int => $right['count'] <=> $left['count']);

        return array_values($groups);
    }

    private function buildBoardCleanupPlan(array $reviews, array $suspectedDuplicates): array
    {
        $closeNowIds = [];
        $holdIds = [];
        $investigateIds = [];
        $closeNowRefs = [];
        $holdRefs = [];
        $investigateRefs = [];

        foreach ($reviews as $review) {
            $taskId = (int) ($review['task']['id'] ?? 0);
            $reasonCodes = $review['reason_codes'] ?? [];
            $verdict = $review['verdict'] ?? 'do';

            if ($taskId <= 0) {
                continue;
            }

            if ($verdict === 'reject' && in_array('no_active_violation', $reasonCodes, true)) {
                $closeNowIds[] = $taskId;
                $closeNowRefs[] = $this->boardTaskReference($review);
                continue;
            }

            if ($verdict === 'wait' && in_array('stale_crawl_data', $reasonCodes, true)) {
                $holdIds[] = $taskId;
                $holdRefs[] = $this->boardTaskReference($review);
                continue;
            }

            if (in_array('missing_crawl_context', $reasonCodes, true)) {
                $investigateIds[] = $taskId;
                $investigateRefs[] = $this->boardTaskReference($review);
            }
        }

        sort($closeNowIds);
        sort($holdIds);
        sort($investigateIds);

        $closeClusters = $this->groupCleanupReviews($reviews, 'reject', 'no_active_violation');
        $holdClusters = $this->groupCleanupReviews($reviews, 'wait', 'stale_crawl_data');
        $investigateClusters = $this->groupCleanupReviews($reviews, null, 'missing_crawl_context');

        return [
            'close_now_ids' => $closeNowIds,
            'hold_ids' => $holdIds,
            'investigate_ids' => $investigateIds,
            'close_now_refs' => $closeNowRefs,
            'hold_refs' => $holdRefs,
            'investigate_refs' => $investigateRefs,
            'close_now_clusters' => $closeClusters,
            'hold_clusters' => $holdClusters,
            'investigate_clusters' => $investigateClusters,
            'duplicate_groups' => $suspectedDuplicates,
            'shared_close_reason_code' => 'no_active_violation',
            'shared_close_reason' => 'Close tasks whose latest crawl and violation snapshot no longer show an active failure on the target URL.',
            'reason_templates' => [
                'no_active_violation' => 'Violation no longer exists in the latest crawl/snapshot pair; this task is outdated.',
                'stale_crawl_data' => 'Latest crawl evidence is too old to trust; keep the task open but wait for recrawl.',
                'missing_crawl_context' => 'No current crawl row exists for the URL; verify canonical URL and recrawl before acting.',
                'duplicate_normalized_url' => 'Multiple tasks collapse to the same normalized URL and should be deduplicated.',
                'host_leaked_into_path' => 'Task URL contains the site hostname inside the path and should be normalized before generation.',
                'double_slash_url' => 'Task URL contains repeated slashes and should be collapsed before generation.',
                'double_slash_in_title' => 'Task title still contains a malformed URL artifact even though the normalized path is valid.',
            ],
            'upstream_fix_recommendations' => $this->buildUpstreamFixRecommendations($suspectedDuplicates),
        ];
    }

    private function buildUpstreamFixRecommendations(array $suspectedDuplicates): array
    {
        $recommendations = [];
        $allReasons = [];

        foreach ($suspectedDuplicates as $group) {
            foreach ($group['reasons'] ?? [] as $reason) {
                $allReasons[$reason] = true;
            }
        }

        if (isset($allReasons['host_leaked_into_path'])) {
            $recommendations[] = 'Strip doubledtrailers.com or www.doubledtrailers.com prefixes from brief URLs before task titles and duplicate checks are built.';
        }

        if (isset($allReasons['double_slash_url']) || isset($allReasons['double_slash_in_title'])) {
            $recommendations[] = 'Collapse repeated slashes before task creation and always render task titles from the normalized URL, not the raw brief string.';
        }

        if (isset($allReasons['duplicate_normalized_url'])) {
            $recommendations[] = 'Deduplicate pending tasks by normalized URL plus action family so equivalent cards do not coexist under different raw URL variants.';
        }

        return $recommendations;
    }

    private function boardTaskReference(array $review): array
    {
        $title = trim((string) ($review['task']['title'] ?? ''));
        $url = (string) ($review['task']['url'] ?? '');
        $ruleId = (string) ($review['task']['rule_id'] ?? '');

        return [
            'task_id' => (int) ($review['task']['id'] ?? 0),
            'rule_id' => $ruleId !== '' ? $ruleId : null,
            'title' => $title,
            'url' => $url !== '' ? $url : null,
            'board_match' => $url !== '' ? $title . ' — ' . $url : $title,
        ];
    }

    private function groupCleanupReviews(array $reviews, ?string $requiredVerdict, string $requiredReasonCode): array
    {
        $groups = [];

        foreach ($reviews as $review) {
            $reasonCodes = $review['reason_codes'] ?? [];
            $verdict = $review['verdict'] ?? null;

            if ($requiredVerdict !== null && $verdict !== $requiredVerdict) {
                continue;
            }

            if (!in_array($requiredReasonCode, $reasonCodes, true)) {
                continue;
            }

            $taskId = (int) ($review['task']['id'] ?? 0);
            if ($taskId <= 0) {
                continue;
            }

            $ruleId = (string) ($review['task']['rule_id'] ?? 'NO_RULE');
            $family = $this->ruleFamilyLabel($ruleId);
            $pageType = (string) ($review['evidence']['page']['page_type'] ?? 'unknown');
            $clusterKey = $family . '|' . $pageType;

            if (!isset($groups[$clusterKey])) {
                $groups[$clusterKey] = [
                    'cluster_key' => $clusterKey,
                    'rule_family' => $family,
                    'page_type' => $pageType,
                    'reason_code' => $requiredReasonCode,
                    'task_ids' => [],
                    'rule_ids' => [],
                    'sample_urls' => [],
                    'sample_titles' => [],
                    'sample_task_refs' => [],
                    'count' => 0,
                ];
            }

            $groups[$clusterKey]['task_ids'][] = $taskId;
            $groups[$clusterKey]['rule_ids'][$ruleId] = true;

            $url = (string) ($review['task']['url'] ?? '');
            if ($url !== '' && count($groups[$clusterKey]['sample_urls']) < 3) {
                $groups[$clusterKey]['sample_urls'][$url] = true;
            }

            $title = trim((string) ($review['task']['title'] ?? ''));
            if ($title !== '' && count($groups[$clusterKey]['sample_titles']) < 2) {
                $groups[$clusterKey]['sample_titles'][$title] = true;
            }
            if (count($groups[$clusterKey]['sample_task_refs']) < 5) {
                $groups[$clusterKey]['sample_task_refs'][] = $this->boardTaskReference($review);
            }

            $groups[$clusterKey]['count']++;
        }

        foreach ($groups as &$group) {
            sort($group['task_ids']);
            $group['rule_ids'] = array_values(array_keys($group['rule_ids']));
            $group['sample_urls'] = array_values(array_keys($group['sample_urls']));
            $group['sample_titles'] = array_values(array_keys($group['sample_titles']));
        }
        unset($group);

        usort($groups, function (array $left, array $right): int {
            return [$right['count'], $left['cluster_key']] <=> [$left['count'], $right['cluster_key']];
        });

        return array_values($groups);
    }

    private function ruleFamilyLabel(string $ruleId): string
    {
        $ruleId = strtoupper(trim($ruleId));
        if ($ruleId === '') {
            return 'UNKNOWN';
        }

        if (preg_match('/^[A-Z]+/', $ruleId, $matches) === 1) {
            return $matches[0];
        }

        return $ruleId;
    }

    private function findSuspectedDuplicates(array $reviews): array
    {
        $groups = [];
        $byNormalizedUrl = [];

        foreach ($reviews as $review) {
            $taskId = (int) ($review['task']['id'] ?? 0);
            $url = (string) ($review['task']['url'] ?? '');
            $rawUrl = (string) ($review['task']['raw_url'] ?? '');
            $title = (string) ($review['task']['title'] ?? '');

            $reasons = [];
            if ($rawUrl !== '' && preg_match('#(?:^|/)(?:www\.)?doubledtrailers\.com(?:/|$)#i', $rawUrl) === 1) {
                $reasons[] = 'host_leaked_into_path';
            }
            if ($rawUrl !== '' && preg_match('#//+#', $rawUrl) === 1) {
                $reasons[] = 'double_slash_url';
            }
            if (str_contains($title, '//')) {
                $reasons[] = 'double_slash_in_title';
            }

            if ($url !== '') {
                $byNormalizedUrl[$url][] = [
                    'task_id' => $taskId,
                    'raw_url' => $rawUrl !== '' ? $rawUrl : null,
                    'reasons' => $reasons,
                ];
                continue;
            }

            if ($reasons !== []) {
                $groups[] = [
                    'normalized_url' => null,
                    'task_ids' => [$taskId],
                    'raw_urls' => $rawUrl !== '' ? [$rawUrl] : [],
                    'reasons' => array_values(array_unique($reasons)),
                ];
            }
        }

        foreach ($byNormalizedUrl as $normalizedUrl => $entries) {
            $taskIds = [];
            $rawUrls = [];
            $reasons = [];

            foreach ($entries as $entry) {
                $taskIds[] = $entry['task_id'];
                if (is_string($entry['raw_url']) && $entry['raw_url'] !== '') {
                    $rawUrls[] = $entry['raw_url'];
                }
                $reasons = array_merge($reasons, $entry['reasons']);
            }

            if (count($entries) > 1) {
                $reasons[] = 'duplicate_normalized_url';
            }

            $reasons = array_values(array_unique($reasons));
            if ($reasons === []) {
                continue;
            }

            sort($taskIds);
            $groups[] = [
                'normalized_url' => $normalizedUrl,
                'task_ids' => $taskIds,
                'raw_urls' => array_values(array_unique($rawUrls)),
                'reasons' => $reasons,
            ];
        }

        usort($groups, function (array $left, array $right): int {
            return [count($right['task_ids'] ?? []), $right['task_ids'][0] ?? 0]
                <=> [count($left['task_ids'] ?? []), $left['task_ids'][0] ?? 0];
        });

        return $groups;
    }

    private function buildPipelineStatusGuess(int $reviewCount, int $staleCount, int $missingContextCount): array
    {
        $blocked = $staleCount + $missingContextCount;
        $ratio = $reviewCount > 0 ? $blocked / $reviewCount : 0.0;

        if ($ratio >= 0.9 && $staleCount > 0) {
            return [
                'status' => 'likely_pipeline_issue',
                'severity' => 'high',
                'reason' => 'Most reviewed tasks are blocked by stale or missing crawl evidence.',
            ];
        }

        if ($ratio >= 0.5) {
            return [
                'status' => 'partial_pipeline_issue',
                'severity' => 'medium',
                'reason' => 'A large share of the board is blocked by crawl freshness or missing snapshots.',
            ];
        }

        return [
            'status' => 'healthy_enough',
            'severity' => 'low',
            'reason' => 'The reviewer has enough fresh crawl evidence to classify most tasks normally.',
        ];
    }

    private function tableExists(string $tableName): bool
    {
        if (array_key_exists($tableName, $this->tableExistsCache)) {
            return $this->tableExistsCache[$tableName];
        }

        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
            [$tableName]
        );

        $this->tableExistsCache[$tableName] = !empty($tables);
        return $this->tableExistsCache[$tableName];
    }

    private function findActiveViolationCached(string $url, string $ruleId): ?array
    {
        $cacheKey = strtoupper(trim($ruleId)) . '|' . $this->violationSnapshotService->normalizeUrl($url);
        if (array_key_exists($cacheKey, $this->activeViolationCache)) {
            return $this->activeViolationCache[$cacheKey];
        }

        $this->activeViolationCache[$cacheKey] = $this->violationSnapshotService->findActiveViolation($url, $ruleId);
        return $this->activeViolationCache[$cacheKey];
    }
}
