<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class ActionRequestService
{
    public function __construct(private Connection $db)
    {
    }

    public function queueMany(array $actions, string $requestedBy = 'llm'): array
    {
        if (!$this->tableExists('action_requests')) {
            return [];
        }

        $queued = [];
        foreach ($actions as $action) {
            if (!is_array($action)) {
                continue;
            }

            $actionType = (string) ($action['action'] ?? '');
            if ($actionType === '') {
                continue;
            }

            $target = $this->resolveTarget($action);
            $payload = $action;
            unset($payload['action']);

            $this->db->insert('action_requests', [
                'action_type' => $actionType,
                'target_type' => $target['target_type'],
                'target_id' => $target['target_id'],
                'payload_json' => json_encode($payload, JSON_UNESCAPED_SLASHES),
                'requested_by' => $requestedBy,
                'approval_status' => 'pending',
                'created_at' => date('Y-m-d H:i:s'),
            ]);

            $queued[] = sprintf('%s queued for review', $actionType);
        }

        return $queued;
    }

    public function listRequests(?string $status = null, int $limit = 50): array
    {
        if (!$this->tableExists('action_requests')) {
            return [];
        }

        $limit = max(1, min($limit, 200));
        if ($status) {
            return $this->db->fetchAllAssociative(
                'SELECT * FROM action_requests WHERE approval_status = ? ORDER BY created_at DESC LIMIT ?',
                [$status, $limit]
            );
        }

        return $this->db->fetchAllAssociative(
            'SELECT * FROM action_requests ORDER BY created_at DESC LIMIT ?',
            [$limit]
        );
    }

    public function approve(int $id, string $approvedBy): array
    {
        $request = $this->getRequest($id);
        if (!$request) {
            throw new \RuntimeException('Action request not found.');
        }

        if (($request['approval_status'] ?? '') !== 'pending') {
            throw new \RuntimeException('Only pending action requests can be approved.');
        }

        $summary = $this->executeAction($request, $approvedBy);
        $now = date('Y-m-d H:i:s');
        $payload = $this->decodePayload($request['payload_json'] ?? null);
        $payload['approved_by'] = $approvedBy;

        $this->db->update('action_requests', [
            'approval_status' => 'executed',
            'approved_at' => $now,
            'executed_at' => $now,
            'payload_json' => json_encode($payload, JSON_UNESCAPED_SLASHES),
        ], ['id' => $id]);

        return [
            'request' => $this->getRequest($id),
            'summary' => $summary,
        ];
    }

    public function reject(int $id, string $rejectedBy, ?string $reason = null): array
    {
        $request = $this->getRequest($id);
        if (!$request) {
            throw new \RuntimeException('Action request not found.');
        }

        if (($request['approval_status'] ?? '') !== 'pending') {
            throw new \RuntimeException('Only pending action requests can be rejected.');
        }

        $payload = $this->decodePayload($request['payload_json'] ?? null);
        $payload['rejected_by'] = $rejectedBy;
        if ($reason) {
            $payload['rejection_reason'] = $reason;
        }

        $this->db->update('action_requests', [
            'approval_status' => 'rejected',
            'approved_at' => date('Y-m-d H:i:s'),
            'payload_json' => json_encode($payload, JSON_UNESCAPED_SLASHES),
        ], ['id' => $id]);

        return $this->getRequest($id) ?? [];
    }

    public function getRequest(int $id): ?array
    {
        if (!$this->tableExists('action_requests')) {
            return null;
        }

        $request = $this->db->fetchAssociative('SELECT * FROM action_requests WHERE id = ?', [$id]);
        return $request ?: null;
    }

    private function resolveTarget(array $action): array
    {
        if (!empty($action['task_id'])) {
            return ['target_type' => 'task', 'target_id' => (string) $action['task_id']];
        }

        if (!empty($action['rule_id'])) {
            return ['target_type' => 'rule', 'target_id' => (string) $action['rule_id']];
        }

        if (!empty($action['url'])) {
            return ['target_type' => 'url', 'target_id' => (string) $action['url']];
        }

        return ['target_type' => 'system', 'target_id' => null];
    }

    private function executeAction(array $request, string $approvedBy): string
    {
        $actionType = (string) ($request['action_type'] ?? '');
        $payload = $this->decodePayload($request['payload_json'] ?? null);

        return match ($actionType) {
            'clear_tasks' => $this->executeClearTasks($payload, $approvedBy),
            'clear_tasks_url' => $this->executeClearTasksUrl($payload, $approvedBy),
            'disable_rule' => $this->executeToggleRule($payload, false, $approvedBy),
            'enable_rule' => $this->executeToggleRule($payload, true, $approvedBy),
            'update_rule_field' => $this->executeUpdateRuleField($payload, $approvedBy),
            'add_learning' => $this->executeAddLearning($payload),
            'dismiss_task' => $this->executeDismissTask($payload, $approvedBy),
            'suppress_url' => $this->executeSuppressUrl($payload, $approvedBy),
            default => throw new \RuntimeException("Unsupported action type: {$actionType}"),
        };
    }

    private function executeClearTasks(array $payload, string $approvedBy): string
    {
        $ruleId = strtoupper((string) ($payload['rule_id'] ?? ''));
        if ($ruleId === '') {
            throw new \RuntimeException('Missing rule_id.');
        }

        $count = $this->db->executeStatement(
            "DELETE FROM tasks WHERE rule_id = ? AND status NOT IN ('done','closed')",
            [$ruleId]
        );

        return "Cleared {$count} pending tasks for {$ruleId} (approved by {$approvedBy}).";
    }

    private function executeClearTasksUrl(array $payload, string $approvedBy): string
    {
        $url = (string) ($payload['url'] ?? '');
        if ($url === '') {
            throw new \RuntimeException('Missing url.');
        }

        $count = $this->db->executeStatement(
            "DELETE FROM tasks WHERE title LIKE ? AND status NOT IN ('done','closed')",
            ['%' . $url . '%']
        );

        return "Cleared {$count} pending tasks for {$url} (approved by {$approvedBy}).";
    }

    private function executeToggleRule(array $payload, bool $active, string $approvedBy): string
    {
        $ruleId = strtoupper((string) ($payload['rule_id'] ?? ''));
        if ($ruleId === '') {
            throw new \RuntimeException('Missing rule_id.');
        }

        $updated = $this->db->update('seo_rules', [
            'is_active' => $active,
            'updated_at' => date('Y-m-d H:i:s'),
            'updated_by' => $approvedBy,
        ], ['rule_id' => $ruleId]);

        if ($updated === 0) {
            throw new \RuntimeException("Rule {$ruleId} not found.");
        }

        return $active
            ? "Rule {$ruleId} activated (approved by {$approvedBy})."
            : "Rule {$ruleId} deactivated (approved by {$approvedBy}).";
    }

    private function executeUpdateRuleField(array $payload, string $approvedBy): string
    {
        $ruleId = strtoupper((string) ($payload['rule_id'] ?? ''));
        $field = (string) ($payload['field'] ?? '');
        $value = $payload['value'] ?? null;
        $allowedFields = ['trigger_sql', 'trigger_condition', 'threshold', 'diagnosis', 'action_output', 'priority', 'assigned'];

        if ($ruleId === '' || $field === '' || !in_array($field, $allowedFields, true)) {
            throw new \RuntimeException('Invalid rule update payload.');
        }

        $updated = $this->db->update('seo_rules', [
            $field => $value,
            'updated_at' => date('Y-m-d H:i:s'),
            'updated_by' => $approvedBy,
        ], ['rule_id' => $ruleId]);

        if ($updated === 0) {
            throw new \RuntimeException("Rule {$ruleId} not found.");
        }

        return "Rule {$ruleId} field '{$field}' updated (approved by {$approvedBy}).";
    }

    private function executeAddLearning(array $payload): string
    {
        $learning = trim((string) ($payload['learning'] ?? ''));
        $category = (string) ($payload['category'] ?? 'general');
        if ($learning === '') {
            throw new \RuntimeException('Missing learning.');
        }

        $existing = (int) $this->db->fetchOne(
            "SELECT COUNT(*) FROM chat_learnings WHERE learning ILIKE ? AND is_active = TRUE",
            ['%' . substr($learning, 0, 50) . '%']
        );

        if ($existing === 0) {
            $this->db->insert('chat_learnings', [
                'learning' => substr($learning, 0, 500),
                'category' => $category,
                'confidence' => 8,
                'learned_from' => 'approved_action',
                'is_active' => true,
                'created_at' => date('Y-m-d H:i:s'),
            ]);
        }

        return 'Learning stored.';
    }

    private function executeDismissTask(array $payload, string $approvedBy): string
    {
        $taskId = (int) ($payload['task_id'] ?? 0);
        if ($taskId <= 0) {
            throw new \RuntimeException('Missing task_id.');
        }

        $dismissType = (string) ($payload['type'] ?? 'invalid');
        $reason = (string) ($payload['reason'] ?? 'Dismissed by approved action');

        $updated = $this->db->update('tasks', [
            'status' => 'closed',
            'completed_at' => date('Y-m-d H:i:s'),
            'recheck_date' => null,
            'recheck_verified' => true,
            'recheck_result' => $dismissType,
            'recheck_criteria' => $reason,
        ], ['id' => $taskId]);

        if ($updated === 0) {
            throw new \RuntimeException("Task {$taskId} not found.");
        }

        return "Task #{$taskId} closed as {$dismissType} (approved by {$approvedBy}).";
    }

    private function executeSuppressUrl(array $payload, string $approvedBy): string
    {
        $url = (string) ($payload['url'] ?? '');
        $ruleId = (string) ($payload['rule_id'] ?? '__ALL__');
        $reason = (string) ($payload['reason'] ?? 'Suppressed via approved action');

        if ($url === '') {
            throw new \RuntimeException('Missing url.');
        }

        $existing = (int) $this->db->fetchOne(
            'SELECT COUNT(*) FROM suppressed_tasks WHERE url = ? AND rule_id = ?',
            [$url, $ruleId]
        );

        if ($existing === 0) {
            $this->db->insert('suppressed_tasks', [
                'url' => $url,
                'rule_id' => $ruleId,
                'reason' => $reason,
                'suppressed_by' => $approvedBy,
                'created_at' => date('Y-m-d H:i:s'),
            ]);
        }

        $cleared = $this->db->executeStatement(
            "DELETE FROM tasks WHERE title LIKE ? AND status NOT IN ('done','closed')",
            ['%' . $url . '%']
        );

        return "Suppressed {$url} (rule: {$ruleId}) and cleared {$cleared} pending tasks.";
    }

    private function decodePayload(mixed $payload): array
    {
        if (is_array($payload)) {
            return $payload;
        }

        if (!is_string($payload) || trim($payload) === '') {
            return [];
        }

        $decoded = json_decode($payload, true);
        return is_array($decoded) ? $decoded : [];
    }

    private function tableExists(string $tableName): bool
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
            [$tableName]
        );

        return !empty($tables);
    }
}
