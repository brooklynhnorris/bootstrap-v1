<?php

namespace App\Command;

use App\Service\CrawlOrchestratorService;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:refresh-nightly-data', description: 'Refresh nightly SEO evidence so page snapshots and page facts stay fresh.')]
class RefreshNightlyDataCommand extends Command
{
    public function __construct(private CrawlOrchestratorService $crawlOrchestratorService)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('crawl-limit', null, InputOption::VALUE_OPTIONAL, 'Maximum URLs for the HTML crawl step', '250')
            ->addOption('skip-html-crawl', null, InputOption::VALUE_NONE, 'Skip the slower HTML crawl and only refresh WordPress + page facts')
            ->addOption('max-age-hours', null, InputOption::VALUE_OPTIONAL, 'Freshness threshold for post-run validation', '24');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $crawlLimit = max(1, min((int) $input->getOption('crawl-limit'), 1000));
        $includeHtmlCrawl = !$input->getOption('skip-html-crawl');
        $maxAgeHours = max(1, min((int) $input->getOption('max-age-hours'), 168));

        $result = $this->crawlOrchestratorService->runNightlyRefresh($crawlLimit, $includeHtmlCrawl);
        $freshness = $this->crawlOrchestratorService->checkFreshness([], $maxAgeHours);

        $output->writeln('Nightly data refresh complete.');
        foreach (($result['steps'] ?? []) as $step) {
            $output->writeln('- ' . ($step['command'] ?? 'unknown command'));
        }

        $output->writeln(sprintf(
            'Freshness check: %d checked | %d stale | threshold %dh',
            (int) ($freshness['checked_urls'] ?? 0),
            (int) ($freshness['stale_count'] ?? 0),
            $maxAgeHours
        ));

        if (($freshness['stale_count'] ?? 0) > 0) {
            $output->writeln('Some URLs remain stale after the nightly refresh.');
            return Command::FAILURE;
        }

        return Command::SUCCESS;
    }
}
