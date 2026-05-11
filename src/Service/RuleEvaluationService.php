<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class RuleEvaluationService
{
    private array $ruleMetaCache = [];
    /** @var array<string, bool> */
    private array $columnExistsCache = [];

    public function __construct(
        private Connection $db,
        private AvrScorer $avrScorer,
        private TaskSuggestionService $taskSuggestionService
    )
    {
    }

    public function evaluateFoundationalContentRules(): array
    {
        if (!$this->tableExists('page_facts') || !$this->tableExists('rule_violations')) {
            return ['snapshot_version' => 0, 'inserted' => 0];
        }

        $runId = null;
        $tracksRun = $this->tableExists('rule_runs');
        if ($tracksRun) {
            $runId = $this->openRuleRun('cron');
        }

        $snapshotVersion = (int) $this->db->fetchOne('SELECT COALESCE(MAX(snapshot_version), 0) + 1 FROM rule_violations');
        $pages = $this->db->fetchAllAssociative('SELECT * FROM page_facts ORDER BY url');
        $suppressionTable = $this->tableExists('suppressed_tasks');

        $inserted = 0;
        $rulesAttempted = 0;
        $rulesSucceeded = 0;
        $rulesFailed = 0;

        $promotionStats = [
            'promoted' => 0,
            'suppressed' => 0,
            'suppressed_by_reason' => [],
        ];

        try {
            foreach ($pages as $page) {
                $violations = $this->determineViolationsForPage($page);
                foreach ($violations as $violation) {
                    $status = 'fail';
                    if ($suppressionTable && $this->isSuppressed((string) $page['url'], $violation['rule_id'])) {
                        $status = 'suppressed';
                    }

                    $normalizedUrl = $this->normalizeUrl((string) $page['url']);
                    $candidateHash = $runId !== null
                        ? hash('sha256', $violation['rule_id'] . '|' . $normalizedUrl . '|' . $runId)
                        : null;

                    $row = [
                        'rule_id' => $violation['rule_id'],
                        'url' => $page['url'],
                        'status' => $status,
                        'severity' => $violation['severity'],
                        'assignee' => $violation['assignee'],
                        'triage' => $this->determineTriage((int) ($page['target_query_impressions'] ?? 0)),
                        'evidence_json' => json_encode($violation['evidence'], JSON_UNESCAPED_SLASHES),
                        'explanation_short' => $violation['message'],
                        'detected_at' => date('Y-m-d H:i:s'),
                        'snapshot_version' => $snapshotVersion,
                    ];

                    $ruleMeta = $this->getRuleMetadata($violation['rule_id']);
                    $violationForScore = $violation + [
                        'consecutive_violation_count' => $this->getConsecutiveViolationCount($violation['rule_id'], (string) $page['url']),
                        'position_delta' => $this->getPositionDelta((string) $page['url'], (float) ($page['target_query_position'] ?? 0.0)),
                        'open_task_age_days' => $this->getOpenTaskAgeDays($violation['rule_id'], (string) $page['url']),
                        'asset_filter_clean' => true,
                    ];
                    $score = $this->avrScorer->score($violationForScore, $page, $ruleMeta);

                    if ($runId !== null && $this->tableHasColumn('rule_violations', 'run_id')) {
                        $row['run_id'] = $runId;
                    }
                    if ($this->tableHasColumn('rule_violations', 'candidate_hash')) {
                        $row['candidate_hash'] = $candidateHash;
                    }
                    if ($this->tableHasColumn('rule_violations', 'decision')) {
                        $row['decision'] = 'pending';
                    }
                    if ($this->tableHasColumn('rule_violations', 'avr_score')) {
                        $row['avr_score'] = $score['avr_score'];
                    }
                    if ($this->tableHasColumn('rule_violations', 'avr_breakdown_json')) {
                        $row['avr_breakdown_json'] = json_encode([
                            'breakdown' => $score['breakdown'],
                            'inputs' => $score['inputs'],
                        ], JSON_UNESCAPED_SLASHES);
                    }

                    $rulesAttempted++;
                    $this->db->insert('rule_violations', $row);
                    $inserted++;
                    $rulesSucceeded++;
                }
            }

            if ($runId !== null) {
                $promotionStats = $this->taskSuggestionService->promoteFromViolations($runId);
            }
        } catch (\Throwable $e) {
            $rulesFailed++;
            if ($runId !== null && $tracksRun) {
                $this->closeRuleRun($runId, 'failed', [
                    'rules_attempted' => $rulesAttempted,
                    'rules_succeeded' => $rulesSucceeded,
                    'rules_failed' => $rulesFailed,
                    'violations_recorded' => $inserted,
                    'error' => $e->getMessage(),
                ], $e->getMessage());
            }
            throw $e;
        }

        if ($runId !== null && $tracksRun) {
            $this->closeRuleRun($runId, 'completed', [
                'rules_attempted' => $rulesAttempted,
                'rules_succeeded' => $rulesSucceeded,
                'rules_failed' => $rulesFailed,
                'violations_recorded' => $inserted,
                'tasks_promoted' => $promotionStats['promoted'] ?? 0,
                'tasks_suppressed' => $promotionStats['suppressed'] ?? 0,
                'suppressed_by_reason' => $promotionStats['suppressed_by_reason'] ?? [],
            ]);
        }

        if ($this->tableExists('data_sources')) {
            $now = date('Y-m-d H:i:s');
            $this->db->executeStatement(
                "INSERT INTO data_sources (source_key, last_fetched_at, last_success_at, last_status, row_count, notes)
                 VALUES ('rule_eval_foundational', :now, :now, 'ok', :row_count, NULL)
                 ON CONFLICT (source_key) DO UPDATE SET
                    last_fetched_at = EXCLUDED.last_fetched_at,
                    last_success_at = EXCLUDED.last_success_at,
                    last_status = EXCLUDED.last_status,
                    row_count = EXCLUDED.row_count,
                    notes = EXCLUDED.notes",
                ['now' => $now, 'row_count' => $inserted]
            );
        }

        return ['snapshot_version' => $snapshotVersion, 'inserted' => $inserted];
    }

    private function determineViolationsForPage(array $page): array
    {
        $violations = [];
        $isIndexable = $this->toBool($page['is_indexable'] ?? true);
        $pageType = strtolower((string) ($page['page_type'] ?? ''));
        $impressions = (int) ($page['target_query_impressions'] ?? 0);
        $hasCoreLink = $this->toBool($page['has_core_link'] ?? false);
        $schemaTypes = json_decode((string) ($page['schema_types'] ?? '[]'), true);
        $h1 = trim((string) ($page['h1'] ?? ''));

        if ($isIndexable && !$this->toBool($page['has_central_entity'] ?? false)) {
            $violations[] = $this->buildViolation(
                'FC-R1',
                'high',
                'Brook',
                'Indexed page is missing the central entity.',
                ['has_central_entity' => $page['has_central_entity'], 'page_type' => $pageType]
            );
        }

        if ($pageType === 'core' && (int) ($page['word_count'] ?? 0) < 500) {
            $violations[] = $this->buildViolation(
                'FC-R3',
                'high',
                'Brook',
                'Core page is below minimum word count.',
                ['word_count' => (int) ($page['word_count'] ?? 0), 'minimum' => 500]
            );
        }

        if ($pageType === 'outer' && !$hasCoreLink && $impressions >= 50) {
            $violations[] = $this->buildViolation(
                'FC-R5',
                'high',
                'Brook',
                'Traffic-bearing outer page is missing a core link.',
                ['has_core_link' => $page['has_core_link'], 'impressions' => $impressions, 'minimum' => 50]
            );
        }

        if ($isIndexable && ($h1 === '' || !$this->toBool($page['h1_matches_title'] ?? false))) {
            $violations[] = $this->buildViolation(
                'FC-R7',
                'high',
                'Brook',
                'Page H1 is missing or does not match the title tag.',
                ['h1' => $page['h1'], 'title_tag' => $page['title_tag'], 'h1_matches_title' => $page['h1_matches_title']]
            );
        }

        if ($pageType === 'core' && (int) ($page['h2_count'] ?? 0) < 1) {
            $violations[] = $this->buildViolation(
                'FC-R8',
                'medium',
                'Brook',
                'Core page is missing H2 headings.',
                ['h2_count' => (int) ($page['h2_count'] ?? 0), 'minimum' => 1]
            );
        }

        if ($pageType === 'core' && (!is_array($schemaTypes) || empty($schemaTypes))) {
            $violations[] = $this->buildViolation(
                'FC-R9',
                'medium',
                'Brad',
                'Core page is missing schema markup.',
                ['schema_types' => $schemaTypes]
            );
        }

        if ($pageType === 'outer' && !$hasCoreLink && $impressions >= 100) {
            $violations[] = $this->buildViolation(
                'FC-R10',
                'high',
                'Brook',
                'High-traffic outer page is missing a core link.',
                ['has_core_link' => $page['has_core_link'], 'impressions' => $impressions, 'minimum' => 100]
            );
        }

        $violations = array_merge(
            $violations,
            $this->evaluateInternalLinkRules($page)
        );

        return $violations;
    }

    private function evaluateInternalLinkRules(array $page): array
    {
        $violations = [];
        $url = (string) ($page['url'] ?? '');
        $pageType = (string) ($page['page_type'] ?? '');

        $excludedUrls = [
            '/',
            '/horse-trailers/',
            '/gooseneck-horse-trailers/',
            '/bumper-pull-horse-trailers/',
            '/living-quarters-horse-trailers/',
        ];

        $linkCount = (int) ($page['body_internal_link_count'] ?? 0);
        $confident = $this->toBool($page['body_link_extraction_confident'] ?? false);
        $isNoindex = $this->toBool($page['is_noindex'] ?? false);

        if (
            $confident
            && $linkCount > 3
            && in_array($pageType, ['core', 'outer'], true)
            && !$isNoindex
            && !in_array($url, $excludedUrls, true)
        ) {
            $violations[] = $this->buildViolation(
                'ILA-005',
                'critical',
                'Brad',
                'Page exceeds internal link hard cap of 3.',
                [
                    'body_internal_link_count' => $linkCount,
                    'maximum' => 3,
                    'page_type' => $pageType,
                ]
            );
        }

        return $violations;
    }

    private function buildViolation(string $ruleId, string $severity, string $assignee, string $message, array $evidence): array
    {
        return [
            'rule_id' => $ruleId,
            'severity' => $severity,
            'assignee' => $assignee,
            'message' => $message,
            'evidence' => $evidence,
        ];
    }

    private function determineTriage(int $impressions): string
    {
        return match (true) {
            $impressions >= 500 => 'high_value',
            $impressions >= 50 => 'optimize',
            $impressions > 0 => 'low_value',
            default => 'strategic_review',
        };
    }

    private function isSuppressed(string $url, string $ruleId): bool
    {
        $count = (int) $this->db->fetchOne(
            "SELECT COUNT(*) FROM suppressed_tasks
             WHERE url = ? AND (rule_id = ? OR rule_id = '__ALL__')",
            [$url, $ruleId]
        );

        return $count > 0;
    }

    private function toBool(mixed $value): bool
    {
        return $value === true || $value === 1 || $value === '1' || $value === 't' || $value === 'true';
    }

    private function tableExists(string $tableName): bool
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
            [$tableName]
        );

        return !empty($tables);
    }

    private function tableHasColumn(string $tableName, string $columnName): bool
    {
        $key = $tableName . '.' . $columnName;
        if (array_key_exists($key, $this->columnExistsCache)) {
            return $this->columnExistsCache[$key];
        }

        $columns = $this->db->fetchFirstColumn(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = ?",
            [$tableName, $columnName]
        );
        $exists = !empty($columns);
        $this->columnExistsCache[$key] = $exists;

        return $exists;
    }

    private function openRuleRun(string $triggeredBy): int
    {
        $this->db->insert('rule_runs', [
            'started_at' => date('Y-m-d H:i:s'),
            'status' => 'running',
            'triggered_by' => $triggeredBy,
        ]);

        return (int) $this->db->lastInsertId();
    }

    private function closeRuleRun(int $runId, string $status, array $stats, ?string $notes = null): void
    {
        $this->db->update('rule_runs', [
            'ended_at' => date('Y-m-d H:i:s'),
            'status' => $status,
            'rules_attempted' => $stats['rules_attempted'] ?? 0,
            'rules_succeeded' => $stats['rules_succeeded'] ?? 0,
            'rules_failed' => $stats['rules_failed'] ?? 0,
            'violations_recorded' => $stats['violations_recorded'] ?? 0,
            'tasks_promoted' => $stats['tasks_promoted'] ?? 0,
            'tasks_suppressed' => $stats['tasks_suppressed'] ?? 0,
            'summary_json' => json_encode($stats, JSON_UNESCAPED_SLASHES),
            'notes' => $notes,
        ], ['id' => $runId]);
    }

    private function normalizeUrl(string $url): string
    {
        $url = trim($url);
        $url = strtolower($url);
        return rtrim($url, '/');
    }

    private function getRuleMetadata(string $ruleId): array
    {
        if (isset($this->ruleMetaCache[$ruleId])) {
            return $this->ruleMetaCache[$ruleId];
        }

        if (!$this->tableExists('seo_rules')) {
            return $this->ruleMetaCache[$ruleId] = [
                'tier' => 'tier_c',
                'action_family' => 'general_fix',
                'business_multiplier' => 1.0,
            ];
        }

        $row = $this->db->fetchAssociative(
            "SELECT tier, action_family, business_multiplier FROM seo_rules WHERE rule_id = ? LIMIT 1",
            [$ruleId]
        ) ?: [];

        return $this->ruleMetaCache[$ruleId] = [
            'tier' => (string) ($row['tier'] ?? 'tier_c'),
            'action_family' => (string) ($row['action_family'] ?? 'general_fix'),
            'business_multiplier' => (float) ($row['business_multiplier'] ?? 1.0),
        ];
    }

    private function getConsecutiveViolationCount(string $ruleId, string $url): int
    {
        $normalizedUrl = $this->normalizeUrl($url);
        $count = (int) $this->db->fetchOne(
            "SELECT COUNT(*)
             FROM rule_violations
             WHERE rule_id = ?
               AND LOWER(TRIM(TRAILING '/' FROM url)) = ?",
            [$ruleId, $normalizedUrl]
        );

        return max(0, $count);
    }

    private function getPositionDelta(string $url, float $currentPosition): float
    {
        if (!$this->tableExists('page_crawl_snapshots') || !$this->tableHasColumn('page_crawl_snapshots', 'target_query_position')) {
            return 0.0;
        }

        $orderColumn = $this->tableHasColumn('page_crawl_snapshots', 'crawled_at') ? 'crawled_at' : 'id';

        $prior = $this->db->fetchOne(
            "SELECT target_query_position
             FROM page_crawl_snapshots
             WHERE LOWER(TRIM(TRAILING '/' FROM url)) = ?
               AND target_query_position IS NOT NULL
             ORDER BY {$orderColumn} DESC
             LIMIT 1",
            [$this->normalizeUrl($url)]
        );

        if ($prior === false || $prior === null) {
            return 0.0;
        }

        return $currentPosition - (float) $prior;
    }

    private function getOpenTaskAgeDays(string $ruleId, string $url): float
    {
        if (!$this->tableExists('tasks')) {
            return 0.0;
        }

        $row = $this->db->fetchAssociative(
            "SELECT created_at
             FROM tasks
             WHERE status NOT IN ('done', 'closed', 'rejected')
               AND title LIKE ?
               AND title LIKE ?
             ORDER BY created_at ASC
             LIMIT 1",
            ['%' . $ruleId . '%', '%' . $url . '%']
        );

        if (!is_array($row) || empty($row['created_at'])) {
            return 0.0;
        }

        $createdTs = strtotime((string) $row['created_at']);
        if ($createdTs === false) {
            return 0.0;
        }

        return max(0.0, (time() - $createdTs) / 86400);
    }
}
