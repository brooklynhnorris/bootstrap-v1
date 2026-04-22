<?php

namespace App\Service;

final class McpReviewerServer
{
    public function __construct(private TaskReviewService $taskReviewService)
    {
    }

    public function initialize(array $params = []): array
    {
        return [
            'protocolVersion' => '2025-11-25',
            'capabilities' => [
                'tools' => new \stdClass(),
                'resources' => new \stdClass(),
            ],
            'serverInfo' => [
                'name' => 'logiri-reviewer',
                'version' => '0.1.0',
            ],
        ];
    }

    public function listTools(): array
    {
        return [
            [
                'name' => 'review_daily_summary',
                'description' => 'Return a structured daily board-health summary with top noise signals, best work now, and cleanup queue.',
                'inputSchema' => [
                    'type' => 'object',
                    'properties' => [
                        'assignee' => [
                            'type' => 'string',
                            'description' => 'Optional persona name, such as Brook, Brad, Jeanne, or Kalib.',
                        ],
                        'limit' => [
                            'type' => 'integer',
                            'minimum' => 1,
                            'maximum' => 200,
                            'description' => 'Maximum number of pending tasks to review while generating the summary.',
                        ],
                    ],
                    'additionalProperties' => false,
                ],
            ],
            [
                'name' => 'review_pending_tasks',
                'description' => 'Review pending or in-progress tasks against live rule, crawl, and rejection evidence and return structured verdicts.',
                'inputSchema' => [
                    'type' => 'object',
                    'properties' => [
                        'assignee' => [
                            'type' => 'string',
                            'description' => 'Optional persona name used to filter the task list.',
                        ],
                        'limit' => [
                            'type' => 'integer',
                            'minimum' => 1,
                            'maximum' => 200,
                            'description' => 'Maximum number of tasks to review.',
                        ],
                        'statuses' => [
                            'type' => 'array',
                            'description' => 'Optional list of task statuses to review.',
                            'items' => [
                                'type' => 'string',
                                'enum' => ['pending', 'in_progress', 'done', 'closed'],
                            ],
                        ],
                    ],
                    'additionalProperties' => false,
                ],
            ],
            [
                'name' => 'review_task',
                'description' => 'Review a single task by ID and return the structured reviewer verdict plus evidence.',
                'inputSchema' => [
                    'type' => 'object',
                    'properties' => [
                        'task_id' => [
                            'type' => 'integer',
                            'minimum' => 1,
                            'description' => 'Task ID to review.',
                        ],
                    ],
                    'required' => ['task_id'],
                    'additionalProperties' => false,
                ],
            ],
        ];
    }

    public function listResources(): array
    {
        return [
            [
                'uri' => 'logiri://reviewer/about',
                'name' => 'Reviewer About',
                'description' => 'Describes the Logiri reviewer server and its current responsibilities.',
                'mimeType' => 'application/json',
            ],
        ];
    }

    public function readResource(string $uri): array
    {
        if ($uri !== 'logiri://reviewer/about') {
            throw new \InvalidArgumentException('Unknown resource: ' . $uri);
        }

        $payload = [
            'name' => 'logiri-reviewer',
            'version' => '0.1.0',
            'purpose' => 'Read-only reviewer for Logiri task QA and daily board summaries.',
            'tools' => array_map(static fn (array $tool) => $tool['name'], $this->listTools()),
        ];

        return [
            'contents' => [
                [
                    'uri' => $uri,
                    'mimeType' => 'application/json',
                    'text' => json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES),
                ],
            ],
        ];
    }

    public function callTool(string $name, array $arguments = []): array
    {
        $payload = match ($name) {
            'review_daily_summary' => $this->taskReviewService->buildDailySummary(
                $this->nullableString($arguments['assignee'] ?? null),
                $this->boundedInt($arguments['limit'] ?? 100, 1, 200)
            ),
            'review_pending_tasks' => $this->taskReviewService->reviewPendingTasks(
                $this->nullableString($arguments['assignee'] ?? null),
                $this->boundedInt($arguments['limit'] ?? 50, 1, 200),
                $this->normalizeStatuses($arguments['statuses'] ?? ['pending'])
            ),
            'review_task' => $this->reviewSingleTask($arguments),
            default => throw new \InvalidArgumentException('Unknown tool: ' . $name),
        };

        return [
            'content' => [
                [
                    'type' => 'text',
                    'text' => json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES),
                ],
            ],
            'structuredContent' => $payload,
        ];
    }

    private function reviewSingleTask(array $arguments): array
    {
        $taskId = $this->boundedInt($arguments['task_id'] ?? 0, 1, PHP_INT_MAX);
        $review = $this->taskReviewService->reviewTaskById($taskId);

        if ($review === null) {
            throw new \InvalidArgumentException('Task not found: ' . $taskId);
        }

        return $review;
    }

    private function normalizeStatuses(mixed $statuses): array
    {
        if (!is_array($statuses)) {
            return ['pending'];
        }

        $allowed = ['pending', 'in_progress', 'done', 'closed'];
        $normalized = [];
        foreach ($statuses as $status) {
            if (!is_string($status)) {
                continue;
            }
            $status = trim($status);
            if (in_array($status, $allowed, true)) {
                $normalized[] = $status;
            }
        }

        return $normalized === [] ? ['pending'] : array_values(array_unique($normalized));
    }

    private function boundedInt(mixed $value, int $min, int $max): int
    {
        $int = is_numeric($value) ? (int) $value : $min;
        return max($min, min($int, $max));
    }

    private function nullableString(mixed $value): ?string
    {
        if (!is_string($value)) {
            return null;
        }

        $value = trim($value);
        return $value === '' ? null : $value;
    }
}
