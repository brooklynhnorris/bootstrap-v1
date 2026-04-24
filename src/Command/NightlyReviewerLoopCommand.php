<?php

namespace App\Command;

use App\Service\CrawlOrchestratorService;
use App\Service\ReviewerActionService;
use App\Service\TaskReviewService;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:nightly-review-loop', description: 'Run a bounded nightly crawl-review-feedback-recrawl loop for a few rounds.')]
class NightlyReviewerLoopCommand extends Command
{
    public function __construct(
        private CrawlOrchestratorService $crawlOrchestratorService,
        private TaskReviewService $taskReviewService,
        private ReviewerActionService $reviewerActionService,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('assignee', null, InputOption::VALUE_OPTIONAL, 'Persona to review', 'Brook')
            ->addOption('limit', null, InputOption::VALUE_OPTIONAL, 'Maximum pending tasks to review each round', '200')
            ->addOption('rounds', null, InputOption::VALUE_OPTIONAL, 'Maximum review/crawl rounds to run', '3')
            ->addOption('sleep-minutes', null, InputOption::VALUE_OPTIONAL, 'Minutes to sleep between rounds', '30')
            ->addOption('crawl-limit', null, InputOption::VALUE_OPTIONAL, 'Maximum URLs for the initial HTML crawl step', '250')
            ->addOption('targeted-recrawl-limit', null, InputOption::VALUE_OPTIONAL, 'Maximum URLs to target in each follow-up recrawl', '12')
            ->addOption('skip-initial-html-crawl', null, InputOption::VALUE_NONE, 'Skip the initial HTML crawl in the first refresh round')
            ->addOption('json', null, InputOption::VALUE_NONE, 'Print the final loop report as JSON');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $assignee = trim((string) $input->getOption('assignee'));
        $limit = max(1, min((int) $input->getOption('limit'), 500));
        $rounds = max(1, min((int) $input->getOption('rounds'), 12));
        $sleepMinutes = max(0, min((int) $input->getOption('sleep-minutes'), 240));
        $crawlLimit = max(1, min((int) $input->getOption('crawl-limit'), 1000));
        $targetedRecrawlLimit = max(1, min((int) $input->getOption('targeted-recrawl-limit'), 50));
        $skipInitialHtmlCrawl = (bool) $input->getOption('skip-initial-html-crawl');

        $report = [
            'generated_at' => date('c'),
            'assignee' => $assignee !== '' ? $assignee : null,
            'rounds_requested' => $rounds,
            'rounds_completed' => 0,
            'sleep_minutes' => $sleepMinutes,
            'round_reports' => [],
        ];

        for ($round = 1; $round <= $rounds; $round++) {
            $includeHtmlCrawl = $round === 1 ? !$skipInitialHtmlCrawl : false;
            $output->writeln(sprintf('Round %d/%d: refreshing evidence.', $round, $rounds));

            $refresh = $this->crawlOrchestratorService->runNightlyRefresh($crawlLimit, $includeHtmlCrawl);
            $output->writeln(sprintf('Round %d/%d: verifying overdue rechecks.', $round, $rounds));
            $outcomeVerification = $this->crawlOrchestratorService->runOutcomeVerification(0);
            $brief = $this->taskReviewService->buildMorningBrief($assignee !== '' ? $assignee : null, $limit);

            $closedBuckets = [];
            foreach (($brief['reject_reason_buckets'] ?? []) as $bucket) {
                $reasonCode = (string) ($bucket['reason_code'] ?? '');
                $tasks = is_array($bucket['tasks'] ?? null) ? $bucket['tasks'] : [];
                $taskIds = array_values(array_filter(array_map(
                    static fn (array $task): int => (int) ($task['task_id'] ?? 0),
                    $tasks
                )));

                if ($taskIds === []) {
                    continue;
                }

                $scope = in_array($reasonCode, ['malformed_task_url', 'asset_url_false_positive'], true)
                    ? 'url_only'
                    : 'task_only';

                $reasonText = match ($reasonCode) {
                    'no_active_violation' => 'Closed automatically by nightly reviewer loop because the latest evidence shows no active violation.',
                    'malformed_task_url' => 'Closed automatically by nightly reviewer loop because the task URL is malformed and should be regenerated only after normalization.',
                    'asset_url_false_positive' => 'Closed automatically by nightly reviewer loop because the task targets an asset URL rather than a content page.',
                    default => 'Closed automatically by nightly reviewer loop.',
                };

                if (!in_array($reasonCode, ['no_active_violation', 'malformed_task_url', 'asset_url_false_positive'], true)) {
                    continue;
                }

                $closedBuckets[] = $this->reviewerActionService->closeTasks(
                    $taskIds,
                    $reasonText,
                    $reasonCode,
                    $scope,
                    'nightly-review-loop'
                );
            }

            $feedback = [];
            foreach (($brief['rules_to_revise'] ?? []) as $ruleReview) {
                $ruleId = trim((string) ($ruleReview['rule_id'] ?? ''));
                if ($ruleId === '') {
                    continue;
                }

                $feedback[] = $this->reviewerActionService->submitRuleFeedback(
                    $ruleId,
                    isset($ruleReview['task_id']) ? (int) $ruleReview['task_id'] : null,
                    isset($ruleReview['url']) && is_string($ruleReview['url']) ? $ruleReview['url'] : null,
                    'REVISE_RULE',
                    null,
                    (string) ($ruleReview['summary'] ?? 'Nightly reviewer loop flagged this task as a rule-revision candidate.'),
                    (string) ($ruleReview['summary'] ?? 'Nightly reviewer loop flagged this task as a rule-revision candidate.'),
                    'review_rule',
                    'nightly-review-loop'
                );
            }

            foreach (($brief['reject_reason_buckets'] ?? []) as $bucket) {
                $reasonCode = (string) ($bucket['reason_code'] ?? '');
                if (!in_array($reasonCode, ['malformed_task_url', 'asset_url_false_positive'], true)) {
                    continue;
                }

                $tasks = is_array($bucket['tasks'] ?? null) ? $bucket['tasks'] : [];
                foreach ($tasks as $task) {
                    $ruleId = trim((string) ($task['rule_id'] ?? ''));
                    if ($ruleId === '') {
                        continue;
                    }

                    $feedback[] = $this->reviewerActionService->submitRuleFeedback(
                        $ruleId,
                        isset($task['task_id']) ? (int) $task['task_id'] : null,
                        isset($task['url']) && is_string($task['url']) ? $task['url'] : null,
                        'REJECTED',
                        null,
                        sprintf('Nightly reviewer loop rejected this task as %s.', $reasonCode),
                        match ($reasonCode) {
                            'malformed_task_url' => 'Normalize the URL before title generation and dedupe key creation.',
                            'asset_url_false_positive' => 'Suppress asset URLs before page-level rules are evaluated.',
                            default => null,
                        },
                        $reasonCode === 'malformed_task_url' ? 'modify_action' : 'refine_threshold',
                        'nightly-review-loop'
                    );
                }
            }

            $recrawlUrls = [];
            foreach (array_merge($brief['wait_for_crawl'] ?? [], $brief['investigate'] ?? []) as $item) {
                $url = trim((string) ($item['url'] ?? ''));
                if ($url === '') {
                    continue;
                }
                $recrawlUrls[] = $url;
            }
            $recrawlUrls = array_slice(array_values(array_unique($recrawlUrls)), 0, $targetedRecrawlLimit);

            $recrawl = null;
            if ($recrawlUrls !== []) {
                $output->writeln(sprintf('Round %d: targeted recrawl for %d URL(s).', $round, count($recrawlUrls)));
                $recrawl = $this->reviewerActionService->triggerCrawl('targeted', $recrawlUrls, $crawlLimit, true);
            }

            $roundReport = [
                'round' => $round,
                'refresh' => $refresh,
                'outcome_verification' => $outcomeVerification,
                'board_health' => $brief['board_health'] ?? [],
                'closed_buckets' => $closedBuckets,
                'rule_feedback' => $feedback,
                'targeted_recrawl' => $recrawl,
                'wait_for_crawl_count' => (int) ($brief['infrastructure_blockers']['stale_crawl_data'] ?? 0),
                'missing_context_count' => (int) ($brief['infrastructure_blockers']['missing_crawl_context'] ?? 0),
                'rules_to_revise_count' => count($brief['rules_to_revise'] ?? []),
            ];

            $report['round_reports'][] = $roundReport;
            $report['rounds_completed'] = $round;

            $staleRemaining = (int) ($brief['infrastructure_blockers']['stale_crawl_data'] ?? 0);
            $missingRemaining = (int) ($brief['infrastructure_blockers']['missing_crawl_context'] ?? 0);

            if ($round >= $rounds) {
                break;
            }

            if ($recrawlUrls === [] && $staleRemaining === 0 && $missingRemaining === 0) {
                $output->writeln('No more stale or missing-context URLs remain. Ending the loop early.');
                break;
            }

            if ($sleepMinutes > 0) {
                $output->writeln(sprintf('Sleeping %d minute(s) before the next review round.', $sleepMinutes));
                sleep($sleepMinutes * 60);
            }
        }

        $this->storeReport($report);

        if ($input->getOption('json')) {
            $output->writeln(json_encode($report, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
        } else {
            $output->writeln(sprintf(
                'Nightly reviewer loop completed %d round(s).',
                (int) ($report['rounds_completed'] ?? 0)
            ));
        }

        return Command::SUCCESS;
    }

    private function storeReport(array $report): void
    {
        $dir = dirname(__DIR__, 2) . DIRECTORY_SEPARATOR . 'var' . DIRECTORY_SEPARATOR . 'log';
        if (!is_dir($dir)) {
            @mkdir($dir, 0777, true);
        }

        $filename = $dir . DIRECTORY_SEPARATOR . 'nightly-review-loop-' . date('Ymd-His') . '.json';
        @file_put_contents($filename, json_encode($report, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
    }
}
