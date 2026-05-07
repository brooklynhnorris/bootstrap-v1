<?php

namespace App\Service;

use Doctrine\DBAL\Connection;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBagInterface;

class TaskSuggestionService
{
    public function __construct(
        private Connection $db,
        private ViolationSnapshotService $violationSnapshotService,
        private ParameterBagInterface $params
    )
    {
    }

    public function createTasksFromResponse(string $text, array $crawlData = []): array
    {
        $tasksCreated = [];

        if (!preg_match('/<!-- TASKS_JSON -->\s*(.*?)\s*<!-- \/TASKS_JSON -->/s', $text, $matches)) {
            return [
                'text' => $text,
                'tasks_created' => $tasksCreated,
            ];
        }

        $aiTasks = json_decode(trim($matches[1]), true);
        $activeCount = (int) $this->db->fetchOne("SELECT COUNT(*) FROM tasks WHERE status NOT IN ('done','closed')");

        if (is_array($aiTasks) && $activeCount < 30) {
            foreach ($aiTasks as $aiTask) {
                $newTask = $this->createSingleTask($aiTask, $crawlData);
                if ($newTask !== null) {
                    $tasksCreated[] = $newTask;
                }
            }
        }

        $text = preg_replace('/<!-- TASKS_JSON -->.*?<!-- \/TASKS_JSON -->/s', '', $text);
        $text = preg_replace('/^\s*\[\s*\{[^\[\]]*"title"[^\[\]]*\}[\s\S]*?\]\s*$/m', '', $text);

        return [
            'text' => rtrim((string) $text),
            'tasks_created' => $tasksCreated,
        ];
    }

    private function createSingleTask(array $aiTask, array $crawlData): ?array
    {
        $title = trim((string) ($aiTask['title'] ?? ''));
        if ($title === '') {
            return null;
        }

        $urlFragment = $this->extractUrlFragment($title);
        $candidateUrl = trim((string) ($aiTask['url'] ?? $urlFragment ?? ''));
        $ruleId = $this->extractRuleId((string) ($aiTask['title'] ?? '')) ?? $this->normalizeRuleId($aiTask['rule_id'] ?? null);

        if ($candidateUrl === '') {
            return null;
        }

        $normalizedUrl = $this->normalizeUrl($candidateUrl);
        $sourceViolationId = $this->resolveSourceViolationId($aiTask, $normalizedUrl, $ruleId);
        if ($sourceViolationId <= 0) {
            $this->recordSuppression($aiTask, $normalizedUrl, 'MISSING_REQUIRED_EVIDENCE', 'no_source_violation');
            return null;
        }

        $violation = $this->fetchViolationById($sourceViolationId);
        if ($violation === null) {
            $this->recordSuppression($aiTask, $normalizedUrl, 'MISSING_REQUIRED_EVIDENCE', 'source_violation_not_found');
            return null;
        }

        $ruleId = (string) ($violation['rule_id'] ?? $ruleId ?? '');
        if ($ruleId === '') {
            $this->recordSuppression($aiTask, $normalizedUrl, 'MISSING_REQUIRED_EVIDENCE', 'missing_rule_id');
            return null;
        }

        $runId = isset($violation['run_id']) ? (int) $violation['run_id'] : 0;
        $avrScore = isset($violation['avr_score']) ? (int) $violation['avr_score'] : 0;
        $actionFamily = $this->getActionFamilyForRule($ruleId);

        // 1) Asset filter
        $assetReason = self::detectAssetUrl($candidateUrl);
        if ($assetReason !== null) {
            $this->recordSuppression($aiTask + ['source_violation_id' => $sourceViolationId], $normalizedUrl, 'ASSET_URL', $assetReason);
            return null;
        }

        // 2) URL canonical exists in page_facts
        if (!$this->canonicalExists($normalizedUrl)) {
            $this->recordSuppression($aiTask + ['source_violation_id' => $sourceViolationId], $normalizedUrl, 'INVALID_URL', 'canonical_not_found');
            return null;
        }

        // 3) Evidence URL match
        $evidenceUrl = $this->normalizeUrl((string) ($violation['url'] ?? ''));
        if ($evidenceUrl === '' || $evidenceUrl !== $normalizedUrl) {
            $this->recordSuppression($aiTask + ['source_violation_id' => $sourceViolationId], $normalizedUrl, 'URL_EVIDENCE_MISMATCH', "evidence_url={$evidenceUrl}");
            return null;
        }

        // 4) Existing task / idempotency
        $idempotencyKey = (string) ($violation['candidate_hash'] ?? '');
        if ($idempotencyKey !== '') {
            $existingByKey = $this->db->fetchAssociative(
                "SELECT id, status, COALESCE(lifecycle_state,'') AS lifecycle_state FROM tasks WHERE idempotency_key = ? LIMIT 1",
                [$idempotencyKey]
            );
            if ($existingByKey) {
                $this->recordSuppression($aiTask + ['source_violation_id' => $sourceViolationId], $normalizedUrl, 'EXISTING_OPEN_TASK', 'task_id=' . $existingByKey['id'] . ';lifecycle_state=' . $existingByKey['lifecycle_state']);
                return null;
            }
        }

        // Legacy duplicate catch (title-level) stays as safety.
        $existing = $this->db->fetchAssociative(
            "SELECT id FROM tasks WHERE title = ? AND status NOT IN ('done','closed') LIMIT 1",
            [$title]
        );
        if ($existing) {
            $this->recordSuppression($aiTask + ['source_violation_id' => $sourceViolationId], $normalizedUrl, 'EXISTING_OPEN_TASK', 'title_duplicate_task_id=' . $existing['id']);
            return null;
        }

        // 5) Cross-rule collision: one task per (run_id, normalized_url, action_family)
        if ($runId > 0) {
            $collision = $this->db->fetchAssociative(
                "SELECT t.id
                 FROM tasks t
                 JOIN rule_violations rv ON rv.id = t.source_violation_id
                 JOIN seo_rules sr ON sr.rule_id = rv.rule_id
                 WHERE rv.run_id = ?
                   AND LOWER(TRIM(TRAILING '/' FROM rv.url)) = ?
                   AND COALESCE(sr.action_family, 'general_fix') = ?
                 LIMIT 1",
                [$runId, $normalizedUrl, $actionFamily]
            );
            if ($collision) {
                $this->recordSuppression($aiTask + ['source_violation_id' => $sourceViolationId], $normalizedUrl, 'CROSS_RULE_COLLISION', 'task_id=' . $collision['id']);
                return null;
            }
        }

        // 6) Low AVR score
        $avrFloor = (int) $this->params->get('logiri.avr_floor');
        if ($avrScore < $avrFloor) {
            $this->recordSuppression($aiTask + ['source_violation_id' => $sourceViolationId], $normalizedUrl, 'LOW_AVR_SCORE', "avr_score={$avrScore};floor={$avrFloor}");
            return null;
        }

        // 7) Role capacity gate
        $assignedRole = strtolower(trim((string) ($aiTask['role'] ?? 'default')));
        $capacityByRole = (array) $this->params->get('logiri.capacity_per_role_per_day');
        $capacity = isset($capacityByRole[$assignedRole]) ? (int) $capacityByRole[$assignedRole] : (int) ($capacityByRole['default'] ?? 5);
        $currentCount = (int) $this->db->fetchOne(
            "SELECT COUNT(*) FROM tasks WHERE COALESCE(assigned_role,'default') = ? AND DATE(created_at) = CURRENT_DATE",
            [$assignedRole]
        );
        if ($currentCount >= $capacity) {
            $this->recordSuppression($aiTask + ['source_violation_id' => $sourceViolationId], $normalizedUrl, 'ROLE_CAPACITY_EXCEEDED', "role={$assignedRole};count={$currentCount};capacity={$capacity}");
            return null;
        }

        $activeViolation = $urlFragment !== null ? $this->findActiveViolation($urlFragment, $ruleId, $crawlData) : null;
        $priority = $this->resolvePriority($aiTask, $activeViolation);
        $assignedTo = $aiTask['assigned_to'] ?? ($activeViolation['assignee'] ?? null);
        $description = isset($aiTask['description']) ? strip_tags((string) $aiTask['description']) : null;

        // 8) Insert task with avr_score
        $this->db->insert('tasks', [
            'title' => $title,
            'description' => $description,
            'assigned_to' => $assignedTo,
            'assigned_role' => $assignedRole,
            'status' => 'pending',
            'priority' => $priority,
            'estimated_hours' => (float) ($aiTask['estimated_hours'] ?? 1),
            'logged_hours' => 0,
            'recheck_type' => $aiTask['recheck_type'] ?? null,
            'recheck_days' => isset($aiTask['recheck_days']) ? (int) $aiTask['recheck_days'] : null,
            'recheck_criteria' => $aiTask['recheck_criteria'] ?? null,
            'idempotency_key' => $idempotencyKey !== '' ? $idempotencyKey : null,
            'source_violation_id' => $sourceViolationId,
            'run_id' => $runId > 0 ? $runId : null,
            'lifecycle_state' => 'active',
            'last_seen_at' => date('Y-m-d H:i:s'),
            'avr_score' => $avrScore,
            'created_at' => date('Y-m-d H:i:s'),
        ]);

        // 9) Mark violation promoted
        if ($this->tableExists('rule_violations') && $this->tableHasColumn('rule_violations', 'decision')) {
            $this->db->update('rule_violations', ['decision' => 'promoted'], ['id' => $sourceViolationId]);
        }

        $created = $this->db->fetchAssociative(
            'SELECT id, title, priority, assigned_to, estimated_hours, recheck_type FROM tasks WHERE title = ? AND status != ? LIMIT 1',
            [$title, 'done']
        );

        return $created ?: ['title' => $title];
    }

    public static function detectAssetUrl(string $url): ?string
    {
        $u = self::normalizeUrlForFilterStatic($url);

        $parsed = parse_url($u);
        $path = isset($parsed['path']) && is_string($parsed['path']) ? $parsed['path'] : $u;

        if (preg_match('#^/wp-content/uploads/#i', $path) === 1) {
            return 'wp-content-uploads-prefix';
        }
        if (preg_match('#^/scripts/.*\.html$#i', $path) === 1) {
            return 'scripts-html-suffix';
        }

        $blockedExt = [
            'jpg', 'jpeg', 'png', 'webp', 'gif', 'svg',
            'pdf', 'doc', 'docx', 'xls', 'xlsx',
            'zip',
            'mp4', 'webm',
        ];
        if (preg_match('/\.([a-z0-9]+)$/i', $path, $m) === 1) {
            $ext = strtolower((string) ($m[1] ?? ''));
            if (in_array($ext, $blockedExt, true)) {
                return 'extension-' . $ext;
            }
        }

        return null;
    }

    private function extractUrlFragment(string $title): ?string
    {
        if (preg_match('|(/[a-z0-9][a-z0-9_-]+(?:/[a-z0-9_-]+)*/)|i', $title, $matches)) {
            return $matches[1];
        }

        return null;
    }

    private function extractRuleId(string $title): ?string
    {
        if (preg_match('/^\[([A-Z]+-[A-Za-z0-9]+)\]/', $title, $matches)) {
            return $matches[1];
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

    private function isSuppressed(string $title, string $urlFragment): bool
    {
        try {
            $ruleId = $this->extractRuleId($title);
            if ($ruleId) {
                $suppressed = (int) $this->db->fetchOne(
                    'SELECT COUNT(*) FROM suppressed_tasks WHERE url = ? AND (rule_id = ? OR rule_id IS NULL)',
                    [$urlFragment, $ruleId]
                );
                if ($suppressed > 0) {
                    return true;
                }
            }

            $blanketSuppressed = (int) $this->db->fetchOne(
                "SELECT COUNT(*) FROM suppressed_tasks WHERE url = ? AND rule_id = '__ALL__'",
                [$urlFragment]
            );

            return $blanketSuppressed > 0;
        } catch (\Exception $e) {
            return false;
        }
    }

    private function recordSuppression(array $aiTask, string $url, string $reasonCode, ?string $reasonText = null): void
    {
        $normalizedUrl = $this->normalizeUrl($url);
        $ruleId = $this->extractRuleId((string) ($aiTask['title'] ?? '')) ?? $this->normalizeRuleId($aiTask['rule_id'] ?? null);

        try {
            if ($this->tableExists('suppressed_tasks')) {
                $this->db->insert('suppressed_tasks', [
                    'url' => $normalizedUrl,
                    'rule_id' => $ruleId,
                    'reason' => $reasonCode,
                    'created_at' => date('Y-m-d H:i:s'),
                ]);
            }
        } catch (\Exception $e) {
            // Non-fatal: suppression logging should never block the workflow.
        }

        if (!$this->tableExists('rule_violations')) {
            return;
        }

        $sourceViolationId = $this->resolveSourceViolationId($aiTask, $normalizedUrl, $ruleId);
        if ($sourceViolationId <= 0) {
            return;
        }

        if (
            !$this->tableHasColumn('rule_violations', 'decision')
            || !$this->tableHasColumn('rule_violations', 'suppression_reason_code')
            || !$this->tableHasColumn('rule_violations', 'suppression_reason_text')
        ) {
            return;
        }

        try {
            $updates = [
                'decision' => 'suppressed',
                'suppression_reason_code' => $reasonCode,
                'suppression_reason_text' => $reasonText,
            ];

            $this->db->update('rule_violations', $updates, ['id' => $sourceViolationId]);
        } catch (\Exception $e) {
            // Non-fatal
        }
    }

    private function resolveSourceViolationId(array $aiTask, string $normalizedUrl, ?string $ruleId): int
    {
        $sourceViolationId = isset($aiTask['source_violation_id']) ? (int) $aiTask['source_violation_id'] : 0;
        if ($sourceViolationId <= 0 && isset($aiTask['violation_id'])) {
            $sourceViolationId = (int) $aiTask['violation_id'];
        }
        if ($sourceViolationId > 0) {
            return $sourceViolationId;
        }
        if ($ruleId === null || !$this->tableExists('rule_violations')) {
            return 0;
        }

        $id = $this->db->fetchOne(
            "SELECT id
             FROM rule_violations
             WHERE rule_id = ?
               AND LOWER(TRIM(TRAILING '/' FROM url)) = ?
             ORDER BY detected_at DESC, id DESC
             LIMIT 1",
            [$ruleId, $normalizedUrl]
        );

        return (int) ($id ?: 0);
    }

    private function canonicalExists(string $normalizedUrl): bool
    {
        if (!$this->tableExists('page_facts')) {
            return false;
        }
        $count = (int) $this->db->fetchOne(
            "SELECT COUNT(*) FROM page_facts WHERE LOWER(TRIM(TRAILING '/' FROM url)) = ?",
            [$normalizedUrl]
        );
        return $count > 0;
    }

    private function fetchViolationById(int $id): ?array
    {
        if (!$this->tableExists('rule_violations')) {
            return null;
        }
        $row = $this->db->fetchAssociative(
            "SELECT id, rule_id, url, run_id, candidate_hash, avr_score FROM rule_violations WHERE id = ? LIMIT 1",
            [$id]
        );
        return $row ?: null;
    }

    private function getActionFamilyForRule(string $ruleId): string
    {
        if (!$this->tableExists('seo_rules')) {
            return 'general_fix';
        }
        $value = $this->db->fetchOne("SELECT action_family FROM seo_rules WHERE rule_id = ? LIMIT 1", [$ruleId]);
        $value = is_string($value) ? trim($value) : '';
        return $value !== '' ? $value : 'general_fix';
    }

    private function passesTrafficGate(array $aiTask, string $title, string $urlFragment, array $crawlData): bool
    {
        try {
            $pageImpressions = $this->lookupImpressions($urlFragment, $crawlData);

            if ($pageImpressions > 0) {
                return true;
            }

            $assignedTo = strtolower((string) ($aiTask['assigned_to'] ?? ''));
            $titleLower = strtolower($title);

            return str_contains($titleLower, 'evaluate')
                || str_contains($titleLower, 'noindex')
                || str_contains($titleLower, 'redirect')
                || str_contains($titleLower, 'strategic')
                || str_contains($titleLower, 'consolidat')
                || $assignedTo === 'jeanne';
        } catch (\Exception $e) {
            return true;
        }
    }

    private function lookupImpressions(string $urlFragment, array $crawlData): int
    {
        foreach ($crawlData as $row) {
            if ($this->urlsMatch((string) ($row['url'] ?? ''), $urlFragment)) {
                return (int) ($row['target_query_impressions'] ?? 0);
            }
        }

        if ($this->tableExists('page_facts')) {
            $dbImpressions = $this->db->fetchOne(
                "SELECT target_query_impressions FROM page_facts WHERE url = ? LIMIT 1",
                [$this->violationSnapshotService->normalizeUrl($urlFragment)]
            );
            if ($dbImpressions !== false && $dbImpressions !== null) {
                return (int) $dbImpressions;
            }
        }

        $dbImpressions = $this->db->fetchOne(
            "SELECT target_query_impressions FROM page_crawl_snapshots WHERE url LIKE ? ORDER BY crawled_at DESC LIMIT 1",
            ['%' . $urlFragment . '%']
        );

        return (int) ($dbImpressions ?: 0);
    }

    private function findActiveViolation(string $urlFragment, ?string $ruleId, array $crawlData): ?array
    {
        if ($ruleId === null) {
            return null;
        }

        foreach ($crawlData as $row) {
            if (!$this->urlsMatch((string) ($row['url'] ?? ''), $urlFragment)) {
                continue;
            }

            $ruleIds = array_filter(array_map('trim', explode(',', (string) ($row['rule_ids'] ?? ''))));
            if (in_array($ruleId, $ruleIds, true)) {
                return [
                    'url' => $row['url'],
                    'rule_id' => $ruleId,
                    'severity' => $row['severity'] ?? null,
                    'assignee' => $row['assignee'] ?? null,
                ];
            }
        }

        return $this->violationSnapshotService->findActiveViolation($urlFragment, $ruleId);
    }

    private function resolvePriority(array $aiTask, ?array $activeViolation): string
    {
        $priority = strtolower((string) ($aiTask['priority'] ?? ''));
        if (in_array($priority, ['critical', 'high', 'medium', 'low'], true)) {
            return $priority;
        }

        $severity = strtolower((string) ($activeViolation['severity'] ?? ''));

        return match ($severity) {
            'critical' => 'critical',
            'high' => 'high',
            'medium' => 'medium',
            default => 'medium',
        };
    }

    private function urlsMatch(string $left, string $right): bool
    {
        return $this->violationSnapshotService->normalizeUrl($left) === $this->violationSnapshotService->normalizeUrl($right);
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
        $columns = $this->db->fetchFirstColumn(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = ?",
            [$tableName, $columnName]
        );

        return !empty($columns);
    }

    private function normalizeUrl(string $url): string
    {
        $url = trim($url);
        $url = strtolower($url);
        return rtrim($url, '/');
    }

    private function normalizeUrlForFilter(string $url): string
    {
        return self::normalizeUrlForFilterStatic($url);
    }

    private static function normalizeUrlForFilterStatic(string $url): string
    {
        $u = trim($url);
        $u = strtolower($u);
        $u = rtrim($u, '/');

        $q = strpos($u, '?');
        if ($q !== false) {
            $u = substr($u, 0, $q);
        }

        $f = strpos($u, '#');
        if ($f !== false) {
            $u = substr($u, 0, $f);
        }

        return $u;
    }
}
