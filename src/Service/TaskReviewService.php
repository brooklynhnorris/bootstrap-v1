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

        foreach ($reviews as $review) {
            $verdict = $review['verdict'] ?? 'do';
            $reasonCodes = $review['reason_codes'] ?? [];

            if ($verdict === 'do') {
                $doNow[] = $review;
            }

            if ($verdict === 'wait' && in_array('stale_crawl_data', $reasonCodes, true)) {
                $waitForCrawl[] = $review;
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
            'operating_recommendation' => $operatingRecommendation,
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
