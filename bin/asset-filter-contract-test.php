<?php

declare(strict_types=1);

use App\Service\TaskSuggestionService;

require dirname(__DIR__) . '/vendor/autoload.php';

$cases = [
    ['https://www.doubledtrailers.com/about/', null],
    ['https://www.doubledtrailers.com/wp-content/uploads/photo.jpg', 'wp-content-uploads-prefix'],
    ['/wp-content/uploads/2024/horse.png', 'wp-content-uploads-prefix'],
    ['https://www.doubledtrailers.com/scripts/widget.html', 'scripts-html-suffix'],
    ['https://www.doubledtrailers.com/scripts/folder/widget.html', 'scripts-html-suffix'],
    ['https://www.doubledtrailers.com/scripts/widget.htm', null],
    ['https://www.doubledtrailers.com/some.PDF', 'extension-pdf'],
    ['https://www.doubledtrailers.com/some.pdf?ref=email', 'extension-pdf'],
    ['https://www.doubledtrailers.com/blog/2024-recap', null],
    ['https://www.doubledtrailers.com/', null],
    ['https://www.doubledtrailers.com/products/horse.htm.html', null],
    ['https://www.doubledtrailers.com/specs/diagram.svg?v=2', 'extension-svg'],
    ['https://www.doubledtrailers.com/contact?return=/wp-content/uploads/x.jpg', null],
];

$failures = [];
foreach ($cases as [$input, $expected]) {
    $actual = TaskSuggestionService::detectAssetUrl($input);
    if ($actual !== $expected) {
        $failures[] = [
            'input' => $input,
            'expected' => $expected,
            'actual' => $actual,
        ];
    }
}

if (!empty($failures)) {
    fwrite(STDERR, "Asset filter contract test FAILED\n");
    foreach ($failures as $failure) {
        fwrite(
            STDERR,
            sprintf(
                "- %s\n  expected: %s\n  actual:   %s\n",
                $failure['input'],
                var_export($failure['expected'], true),
                var_export($failure['actual'], true)
            )
        );
    }
    exit(1);
}

fwrite(STDOUT, "Asset filter contract test PASSED (" . count($cases) . " cases)\n");
exit(0);
