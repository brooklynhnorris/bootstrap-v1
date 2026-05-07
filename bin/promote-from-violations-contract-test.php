<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

use App\Service\TaskSuggestionService;
use App\Service\ViolationSnapshotService;
use Doctrine\DBAL\DriverManager;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBag;

$db = DriverManager::getConnection(['url' => 'sqlite:///:memory:']);
$violationSnapshotService = new ViolationSnapshotService($db);
$params = new ParameterBag([
    'logiri.avr_floor' => 15,
    'logiri.capacity_per_role_per_day' => [
        'seo' => 8,
        'content' => 5,
        'dev' => 4,
        'sales' => 3,
        'default' => 5,
    ],
]);
$svc = new TaskSuggestionService($db, $violationSnapshotService, $params);

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
    $task = $svc->synthesizeTaskFromViolation($c['violation'], $c['rule']);
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

