<?php

namespace App\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Doctrine\DBAL\Connection;

#[AsCommand(name: 'app:fetch-semrush', description: 'Fetch SEMrush data for Double D Trailers')]
class FetchSemrushCommand extends Command
{
    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $apiKey = $_ENV['SEMRUSH_API_KEY'];
        $domain = 'doubledtrailers.com';

        $output->writeln('Fetching SEMrush data for ' . $domain);

        $url = "https://api.semrush.com/?type=domain_ranks&key={$apiKey}&export_columns=Db,Dn,Rk,Or,Ot,Oc,Ad,At,Ac&domain={$domain}&database=us";

        $response = file_get_contents($url);

        if ($response === false) {
            $output->writeln('ERROR: Could not reach SEMrush API');
            return Command::FAILURE;
        }

        $lines = explode("\n", trim($response));
        if (count($lines) < 2) {
            $output->writeln('ERROR: Unexpected response format');
            return Command::FAILURE;
        }

        $headers = str_getcsv($lines[0], ';');
        $values  = str_getcsv($lines[1], ';');
        $data    = array_combine($headers, $values);

        $this->db->executeStatement(
            'INSERT INTO semrush_snapshots (domain, organic_keywords, organic_traffic, fetched_at)
             VALUES (:domain, :keywords, :traffic, NOW())',
            [
                'domain'   => $domain,
                'keywords' => $data['Organic Keywords'] ?? 0,
                'traffic'  => $data['Organic Traffic'] ?? 0,
            ]
        );

        $output->writeln('Done! Keywords: ' . ($data['Organic Keywords'] ?? 'n/a') . ' | Traffic: ' . ($data['Organic Traffic'] ?? 'n/a'));

        return Command::SUCCESS;
    }
}