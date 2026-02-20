<?php

namespace App\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(
    name: 'app:env-check',
    description: 'Checks that required env vars exist.'
)]
class EnvCheckCommand extends Command
{
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $vars = [
            'GOOGLE_CLIENT_ID',
            'GOOGLE_CLIENT_SECRET',
            'GOOGLE_REDIRECT_URI',
            'ANTHROPIC_API_KEY',
            'SEMRUSH_API_KEY',
        ];

        foreach ($vars as $v) {
            $val = $_ENV[$v] ?? getenv($v);
            $output->writeln(sprintf('%s: %s', $v, $val ? '✅ FOUND' : '❌ MISSING'));
        }

        return Command::SUCCESS;
    }
}