#!/usr/bin/env php
<?php

declare(strict_types=1);

use App\Kernel;
use App\Service\CrawlOrchestratorService;
use App\Service\McpReviewerServer;
use App\Service\ReviewerActionService;
use App\Service\TaskReviewService;
use App\Service\ViolationSnapshotService;
use Symfony\Component\Dotenv\Dotenv;

require dirname(__DIR__) . '/vendor/autoload.php';

$projectDir = dirname(__DIR__);
if (class_exists(Dotenv::class)) {
    $dotenv = new Dotenv();
    if (is_file($projectDir . '/.env')) {
        $dotenv->loadEnv($projectDir . '/.env');
    }
}

$env = $_SERVER['APP_ENV'] ?? $_ENV['APP_ENV'] ?? 'prod';
$debug = filter_var($_SERVER['APP_DEBUG'] ?? $_ENV['APP_DEBUG'] ?? false, FILTER_VALIDATE_BOOL);

$kernel = new Kernel($env, $debug);
$kernel->boot();

$container = $kernel->getContainer();
$connection = $container->get('doctrine.dbal.default_connection');
$violationSnapshotService = new ViolationSnapshotService($connection);
$taskReviewService = new TaskReviewService($connection, $violationSnapshotService);
$reviewerActionService = new ReviewerActionService($connection, new CrawlOrchestratorService($connection));
$server = new McpReviewerServer($taskReviewService, $reviewerActionService);

$stdin = fopen('php://stdin', 'rb');
$stdout = fopen('php://stdout', 'wb');
$debugLog = $projectDir . '/var/log/reviewer-mcp-debug.log';

if (!is_resource($stdin) || !is_resource($stdout)) {
    fwrite(STDERR, "Could not open stdio streams.\n");
    exit(1);
}

debugLog($debugLog, 'server booted');

while (($message = readMcpMessage($stdin)) !== null) {
    debugLog($debugLog, 'message received', $message);
    try {
        $response = handleMcpMessage($server, $message);
        if ($response !== null) {
            debugLog($debugLog, 'writing response', $response);
            writeMcpMessage($stdout, $response);
        }
    } catch (Throwable $e) {
        debugLog($debugLog, 'exception while handling message', [
            'message' => $e->getMessage(),
            'trace' => $e->getTraceAsString(),
        ]);
        if (isset($message['id'])) {
            writeMcpMessage($stdout, [
                'jsonrpc' => '2.0',
                'id' => $message['id'],
                'error' => [
                    'code' => -32000,
                    'message' => $e->getMessage(),
                ],
            ]);
        }
    }
}

$kernel->shutdown();

function handleMcpMessage(McpReviewerServer $server, array $message): ?array
{
    $method = $message['method'] ?? null;
    $params = is_array($message['params'] ?? null) ? $message['params'] : [];
    $id = $message['id'] ?? null;

    if (!is_string($method)) {
        if ($id === null) {
            return null;
        }

        return [
            'jsonrpc' => '2.0',
            'id' => $id,
            'error' => [
                'code' => -32600,
                'message' => 'Invalid request',
            ],
        ];
    }

    if ($method === 'notifications/initialized') {
        return null;
    }

    $result = match ($method) {
        'initialize' => $server->initialize($params),
        'ping' => [],
        'tools/list' => ['tools' => $server->listTools()],
        'tools/call' => $server->callTool(
            (string) ($params['name'] ?? ''),
            is_array($params['arguments'] ?? null) ? $params['arguments'] : []
        ),
        'resources/list' => ['resources' => $server->listResources()],
        'resources/read' => $server->readResource((string) ($params['uri'] ?? '')),
        default => throw new InvalidArgumentException('Method not found: ' . $method),
    };

    if ($id === null) {
        return null;
    }

    return [
        'jsonrpc' => '2.0',
        'id' => $id,
        'result' => $result,
    ];
}

function readMcpMessage($stream): ?array
{
    $headerBuffer = stream_get_line($stream, 65536, "\r\n\r\n");
    if ($headerBuffer === false || $headerBuffer === '') {
        $headerBuffer = stream_get_line($stream, 65536, "\n\n");
    }

    if ($headerBuffer === false || $headerBuffer === '') {
        return feof($stream) ? null : null;
    }

    [$rawHeaders] = array_pad(preg_split("/\r?\n\r?\n/", $headerBuffer, 2) ?: [], 2, '');
    $headers = [];
    foreach (preg_split("/\r\n/", $rawHeaders) ?: [] as $line) {
        if (trim($line) === '') {
            continue;
        }
        [$name, $value] = array_pad(explode(':', $line, 2), 2, null);
        if ($name !== null && $value !== null) {
            $headers[strtolower(trim($name))] = trim($value);
        }
    }

    $contentLength = isset($headers['content-length']) ? (int) $headers['content-length'] : 0;
    if ($contentLength <= 0) {
        return null;
    }

    $body = '';
    while (strlen($body) < $contentLength) {
        $chunk = fread($stream, $contentLength - strlen($body));
        if ($chunk === false || $chunk === '') {
            break;
        }
        $body .= $chunk;
    }

    if ($body === '') {
        return null;
    }

    $decoded = json_decode($body, true);
    return is_array($decoded) ? $decoded : null;
}

function writeMcpMessage($stream, array $message): void
{
    $json = json_encode($message, JSON_UNESCAPED_SLASHES);
    if ($json === false) {
        return;
    }

    fwrite($stream, "Content-Length: " . strlen($json) . "\r\n");
    fwrite($stream, "Content-Type: application/json\r\n\r\n");
    fwrite($stream, $json);
    fflush($stream);
}

function debugLog(string $path, string $message, ?array $context = null): void
{
    $line = '[' . date('c') . '] ' . $message;
    if ($context !== null) {
        $encoded = json_encode($context, JSON_UNESCAPED_SLASHES);
        if ($encoded !== false) {
            $line .= ' ' . $encoded;
        }
    }

    @file_put_contents($path, $line . PHP_EOL, FILE_APPEND);
}
