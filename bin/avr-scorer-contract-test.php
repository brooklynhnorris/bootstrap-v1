<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

use App\Service\AvrScorer;

$scorer = new AvrScorer();

$tests = [
    [
        'name' => 'Tier-A schema rule on product page should score high',
        'violation' => [
            'rule_id' => 'SCH-001',
            'consecutive_violation_count' => 12,
            'position_delta' => 9.5,
            'open_task_age_days' => 10,
            'asset_filter_clean' => true,
        ],
        'page' => [
            'page_type' => 'core',
            'target_query_impressions' => 5000,
            'target_query_position' => 17.2,
            'conversions_28d' => 20,
            'is_indexable' => true,
            'last_crawled_at' => date('Y-m-d H:i:s', strtotime('-1 day')),
            'schema_types' => '["Product"]',
            'url' => '/gooseneck-horse-trailers/',
        ],
        'rule' => [
            'tier' => 'tier_a',
            'action_family' => 'schema_impl',
            'business_multiplier' => 1.5,
        ],
        'min' => 80,
        'max' => 100,
    ],
    [
        'name' => 'Tier-C stale blog content with low traffic should score low',
        'violation' => [
            'rule_id' => 'ETA-005',
            'consecutive_violation_count' => 1,
            'position_delta' => -1.0,
            'open_task_age_days' => 0,
            'asset_filter_clean' => true,
        ],
        'page' => [
            'page_type' => 'outer',
            'target_query_impressions' => 50,
            'target_query_position' => 6.0,
            'conversions_28d' => 0,
            'is_indexable' => true,
            'last_crawled_at' => date('Y-m-d H:i:s', strtotime('-21 days')),
            'schema_types' => '[]',
            'url' => '/some-blog-post/',
        ],
        'rule' => [
            'tier' => 'tier_c',
            'action_family' => 'content_expand',
            'business_multiplier' => 1.0,
        ],
        'min' => 5,
        'max' => 25,
    ],
];

$failed = false;
foreach ($tests as $t) {
    $result = $scorer->score($t['violation'], $t['page'], $t['rule']);
    $score = (int) $result['avr_score'];
    if ($score < $t['min'] || $score > $t['max']) {
        $failed = true;
        fwrite(STDERR, "[FAIL] {$t['name']} => score={$score} expected {$t['min']}..{$t['max']}\n");
        continue;
    }
    fwrite(STDOUT, "[PASS] {$t['name']} => score={$score}\n");
}

$highScore = (int) $scorer->score($tests[0]['violation'], $tests[0]['page'], $tests[0]['rule'])['avr_score'];
$lowScore = (int) $scorer->score($tests[1]['violation'], $tests[1]['page'], $tests[1]['rule'])['avr_score'];
$spread = $highScore - $lowScore;
if ($spread < 50) {
    $failed = true;
    fwrite(STDERR, "[FAIL] score spread too small => high={$highScore}, low={$lowScore}, spread={$spread}, expected >= 50\n");
} else {
    fwrite(STDOUT, "[PASS] score spread check => high={$highScore}, low={$lowScore}, spread={$spread}\n");
}

if ($failed) {
    exit(1);
}

fwrite(STDOUT, "AVR scorer contract test PASSED\n");
