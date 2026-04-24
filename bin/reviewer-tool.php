#!/usr/bin/env php
<?php

declare(strict_types=1);

error_reporting(E_ALL);
ini_set('display_errors', '0');
ini_set('html_errors', '0');
ini_set('log_errors', '1');
ob_start();

use App\Kernel;
use App\Service\CrawlOrchestratorService;
use App\Service\ReviewerActionService;
use App\Service\TaskReviewService;
use App\Service\ViolationSnapshotService;
use Symfony\Component\Dotenv\Dotenv;

set_error_handler(static function (int $severity, string $message, string $file = '', int $line = 0): bool {
    if (!(error_reporting() & $severity)) {
        return false;
    }

    $label = match ($severity) {
        E_WARNING, E_USER_WARNING => 'Warning',
        E_NOTICE, E_USER_NOTICE => 'Notice',
        E_DEPRECATED, E_USER_DEPRECATED => 'Deprecated',
        default => 'PHP',
    };

    fwrite(STDERR, sprintf("%s: %s in %s on line %d%s", $label, $message, $file, $line, PHP_EOL));

    return true;
});

require dirname(__DIR__) . '/vendor/autoload.php';

$projectDir = dirname(__DIR__);
if (class_exists(Dotenv::class) && is_file($projectDir . '/.env')) {
    (new Dotenv())->loadEnv($projectDir . '/.env');
}

$env = $_SERVER['APP_ENV'] ?? $_ENV['APP_ENV'] ?? 'prod';
$debug = filter_var($_SERVER['APP_DEBUG'] ?? $_ENV['APP_DEBUG'] ?? false, FILTER_VALIDATE_BOOL);

$kernel = new Kernel($env, $debug);
$kernel->boot();
$jsonEmitted = false;

try {
    $tool = $argv[1] ?? null;
    $rawArguments = $argv[2] ?? '{}';
    $arguments = json_decode($rawArguments, true);

    if (!is_string($tool) || $tool === '') {
        throw new InvalidArgumentException('Missing tool name.');
    }

    if (!is_array($arguments)) {
        throw new InvalidArgumentException('Tool arguments must be valid JSON.');
    }

    $container = $kernel->getContainer();
    $connection = $container->get('doctrine.dbal.default_connection');
    $reviewService = new TaskReviewService($connection, new ViolationSnapshotService($connection));
    $actionService = new ReviewerActionService($connection, new CrawlOrchestratorService($connection));

    $payload = match ($tool) {
        'review_daily_summary' => $reviewService->buildDailySummary(
            nullableString($arguments['assignee'] ?? null),
            boundedInt($arguments['limit'] ?? 100, 1, 200),
        ),
        'morning_brief' => $reviewService->buildMorningBrief(
            nullableString($arguments['assignee'] ?? null),
            boundedInt($arguments['limit'] ?? 100, 1, 200),
        ),
        'review_pending_tasks' => [
            'generated_at' => date('c'),
            'assignee' => nullableString($arguments['assignee'] ?? null),
            'statuses' => normalizeStatuses($arguments['statuses'] ?? ['pending']),
            'tasks' => $reviewService->reviewPendingTasks(
                nullableString($arguments['assignee'] ?? null),
                boundedInt($arguments['limit'] ?? 50, 1, 200),
                normalizeStatuses($arguments['statuses'] ?? ['pending']),
            ),
        ],
        'review_all_pending' => [
            'generated_at' => date('c'),
            'assignee' => nullableString($arguments['assignee'] ?? null),
            'statuses' => ['pending'],
            'tasks' => $reviewService->reviewPendingTasks(
                nullableString($arguments['assignee'] ?? null),
                boundedInt($arguments['limit'] ?? 100, 1, 500),
                ['pending'],
            ),
        ],
        'propose_rule_gaps' => $reviewService->proposeRuleGaps(
            boundedInt($arguments['limit'] ?? 8, 1, 20),
        ),
        'close_tasks' => $actionService->closeTasks(
            normalizeIntList($arguments['task_ids'] ?? []),
            trim((string) ($arguments['reason_text'] ?? 'Closed by reviewer action')),
            trim((string) ($arguments['reason_code'] ?? 'reviewer_close')),
            trim((string) ($arguments['scope'] ?? 'task_only')),
            nullableString($arguments['actor'] ?? null),
        ),
        'submit_rule_feedback' => $actionService->submitRuleFeedback(
            (string) ($arguments['rule_id'] ?? ''),
            isset($arguments['task_id']) ? boundedInt($arguments['task_id'], 1, PHP_INT_MAX) : null,
            nullableString($arguments['url'] ?? null),
            (string) ($arguments['outcome_status'] ?? 'REVIEWED'),
            nullableString($arguments['what_worked'] ?? null),
            nullableString($arguments['what_didnt_work'] ?? null),
            nullableString($arguments['proposed_change'] ?? null),
            (string) ($arguments['change_type'] ?? 'none'),
            nullableString($arguments['actor'] ?? null),
        ),
        'revise_rule' => $actionService->reviseRule(
            (string) ($arguments['rule_id'] ?? ''),
            is_array($arguments['changes'] ?? null) ? $arguments['changes'] : [],
            nullableString($arguments['summary'] ?? null),
            nullableString($arguments['actor'] ?? null),
        ),
        'trigger_crawl' => $actionService->triggerCrawl(
            (string) ($arguments['mode'] ?? 'targeted'),
            is_array($arguments['urls'] ?? null) ? $arguments['urls'] : [],
            boundedInt($arguments['limit'] ?? 250, 1, 1000),
            !array_key_exists('sync_page_facts', $arguments) || filter_var($arguments['sync_page_facts'], FILTER_VALIDATE_BOOL)
        ),
        'review_task' => reviewTask($reviewService, $arguments),
        default => throw new InvalidArgumentException('Unknown tool: ' . $tool),
    };

    discardUnexpectedStdout();
    echo json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    if (ob_get_level()) {
        ob_end_flush();
    }
    $jsonEmitted = true;
} catch (Throwable $e) {
    discardUnexpectedStdout();
    fwrite(STDERR, $e->getMessage() . PHP_EOL);
    exit(1);
} finally {
    restore_error_handler();
    if (!$jsonEmitted) {
        discardUnexpectedStdout();
    }
    $kernel->shutdown();
}

function reviewTask(TaskReviewService $reviewService, array $arguments): array
{
    $taskId = boundedInt($arguments['task_id'] ?? 0, 1, PHP_INT_MAX);
    $review = $reviewService->reviewTaskById($taskId);

    if ($review === null) {
        throw new InvalidArgumentException('Task not found: ' . $taskId);
    }

    return $review;
}

function normalizeStatuses(mixed $statuses): array
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

function normalizeIntList(mixed $values): array
{
    if (!is_array($values)) {
        return [];
    }

    $normalized = [];
    foreach ($values as $value) {
        if (is_numeric($value)) {
            $int = (int) $value;
            if ($int > 0) {
                $normalized[] = $int;
            }
        }
    }

    return array_values(array_unique($normalized));
}

function boundedInt(mixed $value, int $min, int $max): int
{
    $int = is_numeric($value) ? (int) $value : $min;
    return max($min, min($int, $max));
}

function nullableString(mixed $value): ?string
{
    if (!is_string($value)) {
        return null;
    }

    $value = trim($value);
    return $value === '' ? null : $value;
}

function discardUnexpectedStdout(): void
{
    if (!ob_get_level()) {
        return;
    }

    $buffer = ob_get_contents();
    if ($buffer !== false && trim($buffer) !== '') {
        fwrite(STDERR, trim($buffer) . PHP_EOL);
    }

    ob_clean();
}
