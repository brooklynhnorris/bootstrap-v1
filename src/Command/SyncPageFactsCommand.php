<?php

namespace App\Command;

use App\Service\PageFactsSyncService;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:sync-page-facts', description: 'Sync normalized page facts from crawl and analytics snapshots')]
class SyncPageFactsCommand extends Command
{
    public function __construct(private PageFactsSyncService $pageFactsSyncService)
    {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $count = $this->pageFactsSyncService->sync();
        $output->writeln("Synced {$count} page facts.");

        return Command::SUCCESS;
    }
}
