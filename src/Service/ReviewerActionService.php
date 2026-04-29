<?php

namespace App\Service;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;

class ReviewerActionService
{
    public function __construct(
        private Connection $db,
        private CrawlOrchestratorService $crawlOrchestratorService,
    ) {
    }

    public function closeTasks(array $taskIds, string $reasonText, string $reasonCode = 'reviewer_close', string $scope = 'task_only', ?string $actor = null): array
    {
        $taskIds = array_values(array_unique(array_filter(array_map('intval', $taskIds), static fn (int $id) => $id > 0)));
        if ($taskIds === []) {
            throw new \InvalidArgumentException('At least one valid task ID is required.');
        }

        $actor = $this->normalizeActor($actor);
        $now = date('Y-m-d H:i:s');
        $closed = [];

        $tasks = $this->db->fetchAllAssociative(
            'SELECT * FROM tasks WHERE id IN (?) ORDER BY id',
            [$taskIds],
            [ArrayParameterType::INTEGER]
        );

        $this->db->beginTransaction();
        try {
            foreach ($tasks as $task) {
                $taskId = (int) ($task['id'] ?? 0);
                if ($taskId <= 0) {
                    continue;
                }

                $taskResultCode = $this->compactTaskResultCode($reasonCode);
                $this->db->update('tasks', [
                    'status' => 'closed',
                    'completed_at' => $now,
                    'recheck_date' => null,
                    'recheck_days' => null,
                    'recheck_verified' => true,
                    'recheck_result' => $taskResultCode,
                    'recheck_criteria' => $reasonText,
                ], ['id' => $taskId]);

                $taskUrl = $this->extractTaskUrl($task);
                $pageContext = $taskUrl !== '' ? $this->fetchLatestPageContext($taskUrl) : [];
                $guardrailCode = TaskRejectionGuardrailClassifier::classify($reasonCode, $scope, $reasonText, $task, $pageContext);

                if ($this->tableExists('task_rejections')) {
                    $this->db->insert('task_rejections', [
                        'task_id' => $taskId,
                        'rule_id' => $task['rule_id'] ?? null,
                        'url' => $taskUrl !== '' ? $taskUrl : null,
                        'page_type' => $pageContext['page_type'] ?? null,
                        'target_query' => $pageContext['target_query'] ?? null,
                        'reason_code' => $reasonCode,
                        'reason_text' => $reasonText,
                        'guardrail_code' => $guardrailCode,
                        'scope' => $scope,
                        'created_by' => $actor,
                        'created_at' => $now,
                    ]);
                }

                if (
                    $scope !== 'task_only'
                    && $taskUrl !== ''
                    && !empty($task['rule_id'])
                    && $this->tableExists('suppressed_tasks')
                    && TaskRejectionGuardrailClassifier::shouldCreateSuppressionRecord($guardrailCode, $reasonCode)
                ) {
                    $existing = (int) $this->db->fetchOne(
                        'SELECT COUNT(*) FROM suppressed_tasks WHERE url = ? AND rule_id = ?',
                        [$taskUrl, $task['rule_id']]
                    );

                    if ($existing === 0) {
                        $this->db->insert('suppressed_tasks', [
                            'url' => $taskUrl,
                            'rule_id' => $task['rule_id'],
                            'reason' => $reasonText,
                            'suppressed_by' => $actor,
                            'created_at' => $now,
                        ]);
                    }
                }

                $closed[] = [
                    'task_id' => $taskId,
                    'rule_id' => $task['rule_id'] ?? null,
                    'title' => $task['title'] ?? null,
                    'url' => $taskUrl !== '' ? $taskUrl : null,
                ];
            }

            $this->db->commit();
        } catch (\Throwable $e) {
            $this->db->rollBack();
            throw $e;
        }

        return [
            'closed_count' => count($closed),
            'reason_code' => $reasonCode,
            'reason_text' => $reasonText,
            'scope' => $scope,
            'tasks' => $closed,
        ];
    }

    public function submitRuleFeedback(
        string $ruleId,
        ?int $taskId = null,
        ?string $url = null,
        string $outcomeStatus = 'REVIEWED',
        ?string $whatWorked = null,
        ?string $whatDidntWork = null,
        ?string $proposedChange = null,
        string $changeType = 'none',
        ?string $actor = null
    ): array {
        $ruleId = strtoupper(trim($ruleId));
        if ($ruleId === '') {
            throw new \InvalidArgumentException('rule_id is required.');
        }

        if (!$this->tableExists('rule_feedback')) {
            throw new \RuntimeException('rule_feedback table is not available.');
        }

        $normalizedUrl = $this->normalizeUrl($url);
        $actor = $this->normalizeActor($actor);
        $now = date('Y-m-d H:i:s');

        $this->db->insert('rule_feedback', [
            'rule_id' => $ruleId,
            'url' => $normalizedUrl !== '' ? $normalizedUrl : '/',
            'task_id' => $taskId,
            'assigned_to' => $actor,
            'outcome_status' => $outcomeStatus,
            'fix_description' => null,
            'what_worked' => $whatWorked,
            'what_didnt_work' => $whatDidntWork,
            'proposed_change' => $proposedChange,
            'change_type' => $changeType,
            'created_at' => $now,
        ]);

        return [
            'ok' => true,
            'rule_id' => $ruleId,
            'task_id' => $taskId,
            'url' => $normalizedUrl !== '' ? $normalizedUrl : null,
            'outcome_status' => $outcomeStatus,
            'change_type' => $changeType,
        ];
    }

    public function reviseRule(string $ruleId, array $changes, ?string $summary = null, ?string $actor = null): array
    {
        $ruleId = strtoupper(trim($ruleId));
        if ($ruleId === '') {
            throw new \InvalidArgumentException('rule_id is required.');
        }

        $allowed = ['trigger_sql', 'trigger_condition', 'threshold', 'diagnosis', 'action_output', 'priority', 'assigned', 'is_active'];
        $updates = [];
        foreach ($changes as $field => $value) {
            if (is_string($field) && in_array($field, $allowed, true)) {
                $updates[$field] = $value;
            }
        }

        if ($updates === []) {
            throw new \InvalidArgumentException('No supported rule fields were supplied.');
        }

        $actor = $this->normalizeActor($actor);
        $updates['updated_at'] = date('Y-m-d H:i:s');
        $updates['updated_by'] = $actor;

        $updated = $this->db->update('seo_rules', $updates, ['rule_id' => $ruleId]);
        if ($updated === 0) {
            throw new \RuntimeException("Rule {$ruleId} not found.");
        }

        if ($summary !== null && trim($summary) !== '' && $this->tableExists('rule_feedback')) {
            $this->db->insert('rule_feedback', [
                'rule_id' => $ruleId,
                'url' => '/',
                'task_id' => null,
                'assigned_to' => $actor,
                'outcome_status' => 'RULE_REVISED',
                'fix_description' => null,
                'what_worked' => null,
                'what_didnt_work' => null,
                'proposed_change' => trim($summary),
                'change_type' => 'modify_action',
                'created_at' => date('Y-m-d H:i:s'),
            ]);
        }

        return [
            'ok' => true,
            'rule_id' => $ruleId,
            'updated_fields' => array_keys($changes),
            'summary' => $summary,
        ];
    }

    public function triggerCrawl(string $mode, array $urls = [], int $limit = 250, bool $syncPageFacts = true): array
    {
        return match ($mode) {
            'targeted' => $this->crawlOrchestratorService->triggerTargetedCrawl($urls, $syncPageFacts),
            'full' => $this->crawlOrchestratorService->triggerFullHtmlCrawl($limit, $syncPageFacts),
            'wordpress_refresh' => $this->crawlOrchestratorService->triggerWordPressRefresh($syncPageFacts),
            'nightly' => $this->crawlOrchestratorService->runNightlyRefresh($limit, true),
            default => throw new \InvalidArgumentException('Unsupported crawl mode. Use targeted, full, wordpress_refresh, or nightly.'),
        };
    }

    private function extractTaskUrl(array $task): string
    {
        if (!empty($task['url']) && is_string($task['url'])) {
            return $this->normalizeUrl($task['url']);
        }

        if (preg_match('|(/[a-z0-9][a-z0-9_/-]*/)|i', (string) ($task['title'] ?? ''), $matches)) {
            return $this->normalizeUrl((string) $matches[1]);
        }

        return '';
    }

    private function fetchLatestPageContext(string $url): array
    {
        try {
            return $this->db->fetchAssociative(
                "SELECT page_type, target_query
                 FROM page_crawl_snapshots
                 WHERE url = :url
                 ORDER BY crawled_at DESC NULLS LAST, id DESC
                 LIMIT 1",
                ['url' => $url]
            ) ?: [];
        } catch (\Throwable) {
            return [];
        }
    }

    private function normalizeUrl(?string $url): string
    {
        if ($url === null) {
            return '';
        }

        $url = trim($url);
        if ($url === '') {
            return '';
        }

        if (str_starts_with($url, 'http://') || str_starts_with($url, 'https://')) {
            $path = parse_url($url, PHP_URL_PATH);
            $url = is_string($path) ? $path : '';
        }

        if ($url === '') {
            return '';
        }

        if (!str_starts_with($url, '/')) {
            $url = '/' . $url;
        }

        $url = preg_replace('#/+#', '/', $url) ?? $url;

        return $url === '/' ? '/' : rtrim($url, '/') . '/';
    }

    private function normalizeActor(?string $actor): string
    {
        $actor = trim((string) $actor);
        return $actor !== '' ? $actor : 'logiri-reviewer';
    }

    private function compactTaskResultCode(string $reasonCode): string
    {
        $reasonCode = trim($reasonCode);

        return match ($reasonCode) {
            'no_active_violation' => 'no_active_violation',
            'malformed_task_url' => 'malformed_task_url',
            'asset_url_false_positive' => 'asset_false_positive',
            default => strlen($reasonCode) <= 20 ? $reasonCode : 'reviewer_reject',
        };
    }

    private function tableExists(string $tableName): bool
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name
             FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = ?",
            [$tableName]
        );

        return $tables !== [];
    }
}
