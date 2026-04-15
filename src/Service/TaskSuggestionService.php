<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class TaskSuggestionService
{
    public function __construct(private Connection $db)
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
        if ($urlFragment !== null) {
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

        $priority = in_array(($aiTask['priority'] ?? ''), ['critical', 'high', 'medium', 'low'], true)
            ? $aiTask['priority']
            : 'medium';

        $this->db->insert('tasks', [
            'title' => $title,
            'description' => isset($aiTask['description']) ? strip_tags((string) $aiTask['description']) : null,
            'assigned_to' => $aiTask['assigned_to'] ?? null,
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
            $pageImpressions = 0;

            foreach ($crawlData as $row) {
                if (str_contains((string) ($row['url'] ?? ''), $urlFragment)) {
                    $pageImpressions = (int) ($row['target_query_impressions'] ?? 0);
                    break;
                }
            }

            if ($pageImpressions === 0) {
                $dbImpressions = $this->db->fetchOne(
                    "SELECT target_query_impressions FROM page_crawl_snapshots WHERE url LIKE ? ORDER BY crawled_at DESC LIMIT 1",
                    ['%' . $urlFragment . '%']
                );
                $pageImpressions = (int) ($dbImpressions ?: 0);
            }

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
}
