<?php

namespace App\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Doctrine\DBAL\Connection;

#[AsCommand(name: 'app:fetch-ga4', description: 'Fetch GA4 data for Double D Trailers')]
class FetchGa4Command extends Command
{
    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $clientId     = $_ENV['GOOGLE_CLIENT_ID'];
        $clientSecret = $_ENV['GOOGLE_CLIENT_SECRET'];
        $refreshToken = $_ENV['GOOGLE_REFRESH_TOKEN'];
        $propertyId   = $_ENV['GA4_PROPERTY_ID'];

        $output->writeln('Getting Google access token...');

        $tokenResponse = file_get_contents('https://oauth2.googleapis.com/token', false, stream_context_create([
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

        $tokenData   = json_decode($tokenResponse, true);
        $accessToken = $tokenData['access_token'] ?? null;

        if (!$accessToken) {
            $output->writeln('ERROR: No access token');
            return Command::FAILURE;
        }

        $output->writeln('Got access token. Fetching GA4 data...');

        $endDate   = date('Y-m-d');
        $startDate = date('Y-m-d', strtotime('-28 days'));

        $body = json_encode([
            'dateRanges' => [['startDate' => $startDate, 'endDate' => $endDate]],
            'dimensions' => [['name' => 'pagePath']],
            'metrics'    => [
                ['name' => 'sessions'],
                ['name' => 'screenPageViews'],
                ['name' => 'bounceRate'],
                ['name' => 'conversions'],
            ],
            'limit' => 100,
        ]);

        $response = file_get_contents(
            "https://analyticsdata.googleapis.com/v1beta/properties/{$propertyId}:runReport",
            false,
            stream_context_create([
                'http' => [
                    'method'        => 'POST',
                    'header'        => "Authorization: Bearer {$accessToken}\r\nContent-Type: application/json",
                    'content'       => $body,
                    'ignore_errors' => true,
                ],
            ])
        );

        $data = json_decode($response, true);

        if (isset($data['error'])) {
            $output->writeln('ERROR: ' . $data['error']['message']);
            return Command::FAILURE;
        }

        $rows = $data['rows'] ?? [];
        $output->writeln('Got ' . count($rows) . ' rows from GA4');

        $this->db->executeStatement('CREATE TABLE IF NOT EXISTS ga4_snapshots (
            id SERIAL PRIMARY KEY,
            page_path TEXT NOT NULL,
            sessions INT DEFAULT 0,
            pageviews INT DEFAULT 0,
            bounce_rate FLOAT DEFAULT 0,
            conversions INT DEFAULT 0,
            fetched_at TIMESTAMP NOT NULL
        )');

        $this->db->executeStatement("DELETE FROM ga4_snapshots WHERE fetched_at::date = CURRENT_DATE");

        foreach ($rows as $row) {
            $this->db->executeStatement(
                'INSERT INTO ga4_snapshots (page_path, sessions, pageviews, bounce_rate, conversions, fetched_at)
                 VALUES (:page_path, :sessions, :pageviews, :bounce_rate, :conversions, NOW())',
                [
                    'page_path'   => $row['dimensionValues'][0]['value'] ?? '',
                    'sessions'    => $row['metricValues'][0]['value'] ?? 0,
                    'pageviews'   => $row['metricValues'][1]['value'] ?? 0,
                    'bounce_rate' => $row['metricValues'][2]['value'] ?? 0,
                    'conversions' => $row['metricValues'][3]['value'] ?? 0,
                ]
            );
        }

        $output->writeln('Done! Saved ' . count($rows) . ' GA4 rows to database.');

        return Command::SUCCESS;
    }
}