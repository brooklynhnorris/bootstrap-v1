<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

use App\Service\TaskSuggestionService;

$kernel = new \App\Kernel('prod', false);
$kernel->boot();
$container = $kernel->getContainer();

$db = $container->get('doctrine.dbal.default_connection');
$violationSnapshotService = $container->get(\App\Service\ViolationSnapshotService::class);
$params = method_exists($container, 'getParameterBag')
    ? $container->getParameterBag()
    : $container->get('parameter_bag');

$svc = new TaskSuggestionService($db, $violationSnapshotService, $params);
$synthMethod = new \ReflectionMethod(TaskSuggestionService::class, 'synthesizeTaskFromViolation');
$synthMethod->setAccessible(true);

$cases = [
    [
        'name' => 'Brook assigned rule maps to seo role',
        'violation' => ['id' => 1, 'url' => '/x/', 'rule_id' => 'SCH-001'],
        'rule' => [
            'rule_id' => 'SCH-001',
            'name' => 'Add Organization Schema',
            'assigned' => 'Brook',
            'priority' => 'high',
            'action_output' => 'Do schema work',
        ],
        'expect' => ['assigned_to' => 'Brook', 'assigned_role' => 'seo'],
    ],
    [
        'name' => 'Technical SEO Team maps to Brad/dev',
        'violation' => ['id' => 2, 'url' => '/y/', 'rule_id' => 'TEC-001'],
        'rule' => [
            'rule_id' => 'TEC-001',
            'name' => 'Fix Canonical',
            'assigned' => 'Technical SEO Team',
            'priority' => 'medium',
            'action_output' => 'Fix canonical tags',
        ],
        'expect' => ['assigned_to' => 'Brad', 'assigned_role' => 'dev'],
    ],
    [
        'name' => 'Long freeform assignment falls to first-token default',
        'violation' => ['id' => 3, 'url' => '/z/', 'rule_id' => 'OPQ-001'],
        'rule' => [
            'rule_id' => 'OPQ-001',
            'name' => 'Improve On-Page Content',
            'assigned' => 'Brook (content audit and ...; Brad (schema validation...)',
            'priority' => 'low',
            'action_output' => 'Revise page copy',
        ],
        'expect' => ['assigned_to' => 'Brook', 'assigned_role' => 'default'],
    ],
];

$failed = false;
foreach ($cases as $c) {
    $task = $synthMethod->invoke($svc, $c['violation'], $c['rule']);
    $ok = $task['assigned_to'] === $c['expect']['assigned_to']
        && $task['assigned_role'] === $c['expect']['assigned_role']
        && str_starts_with((string) $task['title'], '[' . $c['rule']['rule_id'] . '] ')
        && str_ends_with((string) $task['title'], $c['violation']['url']);
    if (!$ok) {
        $failed = true;
        fwrite(STDERR, '[FAIL] ' . $c['name'] . ' => ' . json_encode($task, JSON_UNESCAPED_SLASHES) . PHP_EOL);
        continue;
    }
    fwrite(STDOUT, '[PASS] ' . $c['name'] . PHP_EOL);
}

if ($failed) {
    exit(1);
}

fwrite(STDOUT, "promote-from-violations contract test PASSED\n");

