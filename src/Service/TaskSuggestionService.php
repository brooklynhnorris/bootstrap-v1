<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class TaskSuggestionService
{
    public function __construct(
        private Connection $db,
        private ViolationSnapshotService $violationSnapshotService
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

        $existing = $this->db->fetchAssociative(
            "SELECT id FROM tasks WHERE title = ? AND status NOT IN ('done','closed') LIMIT 1",
            [$title]
        );
        if ($existing) {
            return null;
        }

        $urlFragment = $this->extractUrlFragment($title);
        $ruleId = $this->extractRuleId((string) ($aiTask['title'] ?? '')) ?? $this->normalizeRuleId($aiTask['rule_id'] ?? null);
        $activeViolation = null;
        if ($urlFragment !== null) {
            $activeViolation = $this->findActiveViolation($urlFragment, $ruleId, $crawlData);
            if ($ruleId !== null && $activeViolation === null) {
                return null;
            }

            $rulePrefix = substr($title, 0, 10);
            $nearDuplicate = $this->db->fetchAssociative(
                "SELECT id FROM tasks WHERE title LIKE ? AND title LIKE ? AND status NOT IN ('done','closed') LIMIT 1",
                ['%' . $urlFragment . '%', $rulePrefix . '%']
            );
            if ($nearDuplicate) {
                return null;
            }

            if ($this->isSuppressed($title, $urlFragment)) {
                return null;
            }

            if (!$this->passesTrafficGate($aiTask, $title, $urlFragment, $crawlData)) {
                return null;
            }
        }

        $priority = $this->resolvePriority($aiTask, $activeViolation);
        $assignedTo = $aiTask['assigned_to'] ?? ($activeViolation['assignee'] ?? null);
        $description = isset($aiTask['description']) ? strip_tags((string) $aiTask['description']) : null;

        $this->db->insert('tasks', [
            'title' => $title,
            'description' => $description,
            'assigned_to' => $assignedTo,
            'assigned_role' => $aiTask['role'] ?? null,
            'status' => 'pending',
            'priority' => $priority,
            'estimated_hours' => (float) ($aiTask['estimated_hours'] ?? 1),
            'logged_hours' => 0,
            'recheck_type' => $aiTask['recheck_type'] ?? null,
            'recheck_days' => isset($aiTask['recheck_days']) ? (int) $aiTask['recheck_days'] : null,
            'recheck_criteria' => $aiTask['recheck_criteria'] ?? null,
            'created_at' => date('Y-m-d H:i:s'),
        ]);

        $created = $this->db->fetchAssociative(
            'SELECT id, title, priority, assigned_to, estimated_hours, recheck_type FROM tasks WHERE title = ? AND status != ? LIMIT 1',
            [$title, 'done']
        );

        return $created ?: ['title' => $title];
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
}
