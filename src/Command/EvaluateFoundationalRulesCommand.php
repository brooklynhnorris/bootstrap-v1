<?php

namespace App\Command;

use App\Service\PageFactsSyncService;
use App\Service\RuleEvaluationService;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:evaluate-foundational-rules', description: 'Run deterministic foundational content rule evaluation')]
class EvaluateFoundationalRulesCommand extends Command
{
    public function __construct(
        private PageFactsSyncService $pageFactsSyncService,
        private RuleEvaluationService $ruleEvaluationService
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('skip-sync', null, InputOption::VALUE_NONE, 'Skip page facts sync before evaluating rules');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if (!$input->getOption('skip-sync')) {
            $synced = $this->pageFactsSyncService->sync();
            $output->writeln("Synced {$synced} page facts.");
        }

        $result = $this->ruleEvaluationService->evaluateFoundationalContentRules();
        $output->writeln(
            sprintf(
                'Stored %d rule violations in snapshot %d.',
                $result['inserted'] ?? 0,
                $result['snapshot_version'] ?? 0
            )
        );

        return Command::SUCCESS;
    }
}
