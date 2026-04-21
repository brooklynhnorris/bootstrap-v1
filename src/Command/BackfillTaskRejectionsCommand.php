<?php

namespace App\Command;

use App\Service\TaskRejectionGuardrailClassifier;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:backfill-task-rejections', description: 'Backfill structured task rejection records from historical dismissed tasks')]
class BackfillTaskRejectionsCommand extends Command
{
    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Maximum number of historical tasks to inspect', '500')
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Preview inserts and updates without writing changes');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $limit = max(1, (int) $input->getOption('limit'));
        $dryRun = (bool) $input->getOption('dry-run');

        $output->writeln('');
        $output->writeln('Backfilling structured task rejections...');
        $output->writeln('  Limit: ' . $limit . ($dryRun ? ' (dry run)' : ''));

        $tasks = $this->db->fetchAllAssociative(
            "SELECT t.id, t.rule_id, t.title, t.status, t.recheck_result, t.recheck_criteria, t.completed_at
             FROM tasks t
             LEFT JOIN task_rejections tr ON tr.task_id = t.id
             WHERE t.status = 'closed'
               AND t.recheck_result IS NOT NULL
               AND tr.id IS NULL
            ORDER BY t.completed_at DESC NULLS LAST, t.id DESC
             LIMIT :limit",
            ['limit' => $limit],
            ['limit' => ParameterType::INTEGER]
        );

        $inserted = 0;
        foreach ($tasks as $task) {
            $dismissType = (string) ($task['recheck_result'] ?? '');
            if (!in_array($dismissType, ['invalid', 'not_applicable', 'false_positive', 'duplicate', 'wont_fix'], true)) {
                continue;
            }

            $taskUrl = $this->extractTaskUrl($task);
            $pageContext = $taskUrl !== '' ? $this->fetchLatestPageContext($taskUrl) : [];
            $reason = (string) ($task['recheck_criteria'] ?? '');
            $guardrailCode = TaskRejectionGuardrailClassifier::classify($dismissType, 'task_only', $reason, $task, $pageContext);
            $scope = TaskRejectionGuardrailClassifier::inferScope($dismissType, $guardrailCode, $reason);

            if (!$dryRun) {
                $this->db->insert('task_rejections', [
                    'task_id' => (int) $task['id'],
                    'rule_id' => $task['rule_id'] ?: null,
                    'url' => $taskUrl ?: null,
                    'page_type' => $pageContext['page_type'] ?? null,
                    'target_query' => $pageContext['target_query'] ?? null,
                    'reason_code' => $dismissType,
                    'reason_text' => $reason !== '' ? $reason : 'Historical backfill from dismissed task.',
                    'guardrail_code' => $guardrailCode,
                    'scope' => $scope,
                    'created_by' => 'historical_backfill',
                    'created_at' => $task['completed_at'] ?: date('Y-m-d H:i:s'),
                ]);
            }

            $inserted++;
        }

        $rowsNeedingGuardrails = $this->db->fetchAllAssociative(
            "SELECT id, task_id, rule_id, url, page_type, target_query, reason_code, reason_text, scope
             FROM task_rejections
             WHERE guardrail_code IS NULL
             ORDER BY created_at DESC
             LIMIT :limit",
            ['limit' => $limit],
            ['limit' => ParameterType::INTEGER]
        );

        $updated = 0;
        foreach ($rowsNeedingGuardrails as $row) {
            $task = [
                'title' => '',
                'rule_id' => $row['rule_id'] ?? null,
            ];
            $pageContext = [
                'page_type' => $row['page_type'] ?? null,
                'target_query' => $row['target_query'] ?? null,
            ];
            $guardrailCode = TaskRejectionGuardrailClassifier::classify(
                (string) ($row['reason_code'] ?? ''),
                (string) ($row['scope'] ?? 'task_only'),
                (string) ($row['reason_text'] ?? ''),
                $task,
                $pageContext
            );

            if (!$dryRun) {
                $this->db->update('task_rejections', [
                    'guardrail_code' => $guardrailCode,
                ], ['id' => (int) $row['id']]);
            }

            $updated++;
        }

        $output->writeln('  Historical tasks inserted: ' . $inserted);
        $output->writeln('  Existing rows normalized: ' . $updated);
        $output->writeln($dryRun ? 'Dry run complete.' : 'Backfill complete.');

        return Command::SUCCESS;
    }

    private function extractTaskUrl(array $task): string
    {
        if (preg_match('|(/[a-z0-9][a-z0-9_/-]*/)|i', (string) ($task['title'] ?? ''), $urlMatch)) {
            return $this->normalizeUrl((string) $urlMatch[1]);
        }

        return '';
    }

    private function normalizeUrl(string $url): string
    {
        $url = trim($url);
        if ($url === '') {
            return '';
        }

        if (!str_starts_with($url, '/')) {
            $url = '/' . $url;
        }

        return $url === '/' ? '/' : rtrim($url, '/') . '/';
    }

    private function fetchLatestPageContext(string $url): array
    {
        return $this->db->fetchAssociative(
            "SELECT page_type, target_query
             FROM page_crawl_snapshots
             WHERE url = :url
             ORDER BY crawled_at DESC NULLS LAST, id DESC
             LIMIT 1",
            ['url' => $url]
        ) ?: [];
    }
}
