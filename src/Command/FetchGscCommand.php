<?php

namespace App\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Doctrine\DBAL\Connection;

#[AsCommand(name: 'app:fetch-gsc', description: 'Fetch Google Search Console data for Double D Trailers')]
class FetchGscCommand extends Command
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
        $siteUrl      = $_ENV['GSC_SITE_URL'];

        $output->writeln('Getting Google access token...');

        // Step 1: Exchange refresh token for access token
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

        if ($tokenResponse === false) {
            $output->writeln('ERROR: Could not get access token');
            return Command::FAILURE;
        }

        $tokenData   = json_decode($tokenResponse, true);
        $accessToken = $tokenData['access_token'] ?? null;

        if (!$accessToken) {
            $output->writeln('ERROR: No access token in response: ' . $tokenResponse);
            return Command::FAILURE;
        }

        $output->writeln('Got access token. Fetching GSC data...');

        // Step 2: Fetch top queries from GSC
        $endDate   = date('Y-m-d');
        $startDate = date('Y-m-d', strtotime('-28 days'));

        $body = json_encode([
            'startDate'  => $startDate,
            'endDate'    => $endDate,
            'dimensions' => ['query', 'page'],
            'rowLimit'   => 100,
        ]);

        $gscResponse = file_get_contents(
            'https://www.googleapis.com/webmasters/v3/sites/' . urlencode($siteUrl) . '/searchAnalytics/query',
            false,
            stream_context_create([
                'http' => [
                    'method'  => 'POST',
                    'header'  => "Authorization: Bearer {$accessToken}\r\nContent-Type: application/json",
                    'content' => $body,
                ],
            ])
        );

        if ($gscResponse === false) {
            $output->writeln('ERROR: Could not reach GSC API');
            return Command::FAILURE;
        }

        $gscData = json_decode($gscResponse, true);
        $rows    = $gscData['rows'] ?? [];

        $output->writeln('Got ' . count($rows) . ' rows from GSC');

        // Step 3: Create table if needed and store data
        $this->db->executeStatement('CREATE TABLE IF NOT EXISTS gsc_snapshots (
            id SERIAL PRIMARY KEY,
            query VARCHAR(500) NOT NULL,
            page TEXT NOT NULL,
            clicks INT DEFAULT 0,
            impressions INT DEFAULT 0,
            ctr FLOAT DEFAULT 0,
            position FLOAT DEFAULT 0,
            fetched_at TIMESTAMP NOT NULL
        )');

        // Clear today's data before re-inserting
        $this->db->executeStatement("DELETE FROM gsc_snapshots WHERE fetched_at::date = CURRENT_DATE");

        foreach ($rows as $row) {
            $this->db->executeStatement(
                'INSERT INTO gsc_snapshots (query, page, clicks, impressions, ctr, position, fetched_at)
                 VALUES (:query, :page, :clicks, :impressions, :ctr, :position, NOW())',
                [
                    'query'       => $row['keys'][0] ?? '',
                    'page'        => $row['keys'][1] ?? '',
                    'clicks'      => $row['clicks'] ?? 0,
                    'impressions' => $row['impressions'] ?? 0,
                    'ctr'         => $row['ctr'] ?? 0,
                    'position'    => $row['position'] ?? 0,
                ]
            );
        }

        $output->writeln('Done! Saved ' . count($rows) . ' GSC rows to database.');

        return Command::SUCCESS;
    }
}