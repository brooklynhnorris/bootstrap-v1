<?php

namespace App\Command;

use App\Service\TaskReviewService;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:review-board', description: 'Review pending tasks and print a daily board-health summary')]
class ReviewBoardCommand extends Command
{
    public function __construct(private TaskReviewService $taskReviewService)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('assignee', null, InputOption::VALUE_OPTIONAL, 'Only review tasks assigned to a specific persona')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Maximum pending tasks to review', '100')
            ->addOption('json', null, InputOption::VALUE_NONE, 'Output the summary as JSON');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $assignee = $input->getOption('assignee');
        $limit = max(1, min((int) $input->getOption('limit'), 200));
        $summary = $this->taskReviewService->buildDailySummary(is_string($assignee) ? $assignee : null, $limit);

        if ($input->getOption('json')) {
            $output->writeln(json_encode($summary, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
            return Command::SUCCESS;
        }

        $health = $summary['board_health'] ?? [];
        $output->writeln('');
        $output->writeln('Logiri Reviewer Summary');
        $output->writeln('Generated: ' . ($summary['generated_at'] ?? date('c')));
        if (!empty($summary['assignee'])) {
            $output->writeln('Assignee: ' . $summary['assignee']);
        }

        $output->writeln('');
        $output->writeln(sprintf(
            'Board health: %d reviewed | %d do | %d reject | %d wait | %d revise_rule',
            (int) ($health['pending_reviewed'] ?? 0),
            (int) ($health['do'] ?? 0),
            (int) ($health['reject'] ?? 0),
            (int) ($health['wait'] ?? 0),
            (int) ($health['revise_rule'] ?? 0)
        ));

        $output->writeln('');
        $output->writeln('Top reason codes:');
        foreach (($summary['top_reason_codes'] ?? []) as $row) {
            $output->writeln(sprintf('  - %s (%d)', $row['key'] ?? 'unknown', (int) ($row['count'] ?? 0)));
        }

        $output->writeln('');
        $output->writeln('Noisiest rules:');
        foreach (($summary['noisiest_rules'] ?? []) as $row) {
            $output->writeln(sprintf('  - %s (%d)', $row['key'] ?? 'unknown', (int) ($row['count'] ?? 0)));
        }

        $output->writeln('');
        $output->writeln('Best work now:');
        foreach (($summary['best_work_now'] ?? []) as $row) {
            $output->writeln(sprintf(
                '  - #%d %s [%s] (%s)',
                (int) ($row['task_id'] ?? 0),
                (string) ($row['title'] ?? ''),
                (string) ($row['verdict'] ?? 'do'),
                implode(', ', $row['reason_codes'] ?? [])
            ));
        }

        $output->writeln('');
        $output->writeln('Cleanup queue:');
        foreach (($summary['cleanup_queue'] ?? []) as $row) {
            $output->writeln(sprintf(
                '  - #%d %s [%s] (%s)',
                (int) ($row['task_id'] ?? 0),
                (string) ($row['title'] ?? ''),
                (string) ($row['verdict'] ?? 'review'),
                implode(', ', $row['reason_codes'] ?? [])
            ));
        }

        $output->writeln('');

        return Command::SUCCESS;
    }
}
