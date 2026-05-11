<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

$kernel = new \App\Kernel('prod', false);
$kernel->boot();

$container = $kernel->getContainer();
/** @var \App\Service\TaskSuggestionService $service */
$service = $container->get(\App\Service\TaskSuggestionService::class);

$refClass = new ReflectionClass($service);
$method = $refClass->getMethod('tableHasColumn');
$method->setAccessible(true);
$cacheProperty = $refClass->getProperty('columnExistsCache');
$cacheProperty->setAccessible(true);

$table = 'tasks';
$column = 'idempotency_key';
$cacheKey = $table . '.' . $column;

// Warm cache with first call.
$first = $method->invoke($service, $table, $column);
if ($first !== true) {
    fwrite(STDERR, "[FAIL] Expected {$cacheKey} to exist.\n");
    exit(1);
}

$cache = $cacheProperty->getValue($service);
if (!is_array($cache) || !array_key_exists($cacheKey, $cache)) {
    fwrite(STDERR, "[FAIL] Cache key {$cacheKey} missing after first call.\n");
    exit(1);
}

$start = hrtime(true);
for ($i = 0; $i < 100; $i++) {
    $value = $method->invoke($service, $table, $column);
    if ($value !== true) {
        fwrite(STDERR, "[FAIL] Cached lookup returned false at iteration {$i}.\n");
        exit(1);
    }
}
$elapsedNs = hrtime(true) - $start;
$elapsedMs = $elapsedNs / 1_000_000;

if ($elapsedMs >= 100.0) {
    fwrite(STDERR, sprintf("[FAIL] 100 cached lookups took %.3fms (expected < 100ms).\n", $elapsedMs));
    exit(1);
}

fwrite(STDOUT, sprintf("[PASS] 100 cached lookups completed in %.3fms.\n", $elapsedMs));
fwrite(STDOUT, "tableHasColumn cache test PASSED\n");

