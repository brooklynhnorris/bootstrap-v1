<?php
$clientId     = getenv('GOOGLE_CLIENT_ID');
$clientSecret = getenv('GOOGLE_CLIENT_SECRET');
$refreshToken = getenv('GOOGLE_REFRESH_TOKEN');

$token = file_get_contents('https://oauth2.googleapis.com/token', false, stream_context_create([
    'http' => [
        'method'  => 'POST',
        'header'  => 'Content-Type: application/x-www-form-urlencoded',
        'content' => http_build_query([
            'client_id'     => $clientId,
            'client_secret' => $clientSecret,
            'refresh_token' => $refreshToken,
            'grant_type'    => 'refresh_token',
        ]),
    ],
]));

$t  = json_decode($token, true);
$at = $t['access_token'] ?? null;
echo "Access token: " . ($at ? substr($at, 0, 20) . "..." : "MISSING") . "\n";

$response = file_get_contents(
    'https://www.googleapis.com/webmasters/v3/sites/' . urlencode('https://www.doubledtrailers.com') . '/searchAnalytics/query',
    false,
    stream_context_create([
        'http' => [
            'method'      => 'POST',
            'header'      => "Authorization: Bearer {$at}\r\nContent-Type: application/json",
            'content'     => json_encode(['startDate' => '2026-01-01', 'endDate' => '2026-01-28', 'dimensions' => ['query']]),
            'ignore_errors' => true,
        ],
    ])
);

echo "GSC Response: " . $response . "\n";