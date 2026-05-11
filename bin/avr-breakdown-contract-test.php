<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

$kernel = new \App\Kernel('prod', false);
$kernel->boot();
$db = $kernel->getContainer()->get('doctrine.dbal.default_connection');

$row = $db->fetchAssociative(
    "SELECT t.id AS task_id, t.avr_score, t.source_violation_id, rv.avr_breakdown_json
     FROM tasks t
     JOIN rule_violations rv ON rv.id = t.source_violation_id
     WHERE t.status NOT IN ('done', 'closed')
       AND t.avr_score IS NOT NULL
       AND rv.avr_breakdown_json IS NOT NULL
     ORDER BY t.created_at DESC
     LIMIT 1"
);

if (!$row) {
    fwrite(STDERR, "[FAIL] No active task with avr_breakdown_json found.\n");
    exit(1);
}

$decoded = json_decode((string) $row['avr_breakdown_json'], true);
if (!is_array($decoded)) {
    fwrite(STDERR, "[FAIL] avr_breakdown_json is not valid JSON.\n");
    exit(1);
}

$breakdown = $decoded['breakdown'] ?? null;
if (!is_array($breakdown)) {
    fwrite(STDERR, "[FAIL] Missing breakdown object.\n");
    exit(1);
}

$required = ['impact', 'confidence', 'urgency', 'revenue_proximity', 'effort'];
foreach ($required as $key) {
    if (!array_key_exists($key, $breakdown)) {
        fwrite(STDERR, "[FAIL] Missing breakdown key: {$key}\n");
        exit(1);
    }
}

fwrite(STDOUT, "[PASS] Task #{$row['task_id']} has AVR score {$row['avr_score']} with full breakdown payload.\n");
fwrite(STDOUT, "AVR breakdown contract test PASSED\n");

