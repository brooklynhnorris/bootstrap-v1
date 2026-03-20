<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:verify-outcomes', description: 'Check if implemented SEO fixes actually moved the needle in GSC')]
class VerifyOutcomesCommand extends Command
{
    // Minimum days after fix before we expect GSC signal
    private const MIN_DAYS_AFTER_FIX = 14;

    // Outcome thresholds per rule category prefix
    private const THRESHOLDS = [
        // On-Page Content — expect impression/position movement
        'OPQ'       => ['metric' => 'impressions', 'pass' => 20,  'partial' => 10,  'window' => 28],
        // Technical SEO — expect indexing/impression recovery
        'TECH'      => ['metric' => 'impressions', 'pass' => 15,  'partial' => 5,   'window' => 28],
        // Schema — expect CTR improvement from rich results
        'DDT-SD'    => ['metric' => 'ctr',         'pass' => 10,  'partial' => 5,   'window' => 28],
        // Internal Links — expect position improvement
        'ILA'       => ['metric' => 'position',    'pass' => -2,  'partial' => -1,  'window' => 28],
        // Keyword/Intent — expect position and impression gains
        'KIA'       => ['metric' => 'position',    'pass' => -3,  'partial' => -1,  'window' => 28],
        // E-E-A-T — expect impression/trust gains
        'DDT-EEAT'  => ['metric' => 'impressions', 'pass' => 15,  'partial' => 5,   'window' => 28],
        // Entity Authority — expect impression gains
        'ETA'       => ['metric' => 'impressions', 'pass' => 20,  'partial' => 10,  'window' => 28],
        // User Signals — expect CTR improvement
        'USE'       => ['metric' => 'ctr',         'pass' => 10,  'partial' => 5,   'window' => 28],
        // Competitive — expect position recovery
        'CI'        => ['metric' => 'position',    'pass' => -3,  'partial' => -1,  'window' => 28],
        // Content Freshness — expect impression recovery
        'CFL'       => ['metric' => 'impressions', 'pass' => 15,  'partial' => 5,   'window' => 28],
        // Local SEO — expect impression gains
        'DDT-LOCAL' => ['metric' => 'impressions', 'pass' => 10,  'partial' => 5,   'window' => 28],
        // Media — expect CTR from image/video rich results
        'MAO'       => ['metric' => 'ctr',         'pass' => 5,   'partial' => 2,   'window' => 28],
        // Default fallback
        'DEFAULT'   => ['metric' => 'impressions', 'pass' => 10,  'partial' => 5,   'window' => 28],
    ];

    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('rule',      null, InputOption::VALUE_OPTIONAL, 'Check outcomes for a specific rule ID only')
            ->addOption('force',     null, InputOption::VALUE_NONE,     'Re-evaluate outcomes even if already checked')
            ->addOption('min-days',  null, InputOption::VALUE_OPTIONAL, 'Minimum days after fix before checking (default: 14)', 14);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $ruleFilter = $input->getOption('rule');
        $force      = (bool) $input->getOption('force');
        $minDays    = (int) ($input->getOption('min-days') ?? self::MIN_DAYS_AFTER_FIX);

        $this->ensureSchema();

        $output->writeln('');
        $output->writeln('+==========================================+');
        $output->writeln('|     LOGIRI OUTCOMES VERIFIER             |');
        $output->writeln('|   Did the fix actually move the needle?  |');
        $output->writeln('+==========================================+');
        $output->writeln('');

        // Pull accepted rule_reviews older than $minDays
        $reviews = $this->getReviewsReadyForVerification($minDays, $ruleFilter, $force);

        if (empty($reviews)) {
            $output->writeln("No completed fixes ready for outcome verification.");
            $output->writeln("  Requirements: task marked done + fix_implemented_at >= {$minDays} days ago");
            $output->writeln("  Run with --force to re-verify already-checked outcomes.");
            $output->writeln("  Run with --min-days=7 to check earlier.");
            return Command::SUCCESS;
        }

        $output->writeln("Found " . count($reviews) . " fix(es) ready for verification.");
        $output->writeln('');

        $totalPass    = 0;
        $totalPartial = 0;
        $totalFail    = 0;

        foreach ($reviews as $review) {
            $ruleId   = $review['rule_id'];
            $url      = $review['url'];
            $fixDate  = $review['fix_implemented_at'];
            $daysAgo  = (int) ((time() - strtotime($fixDate)) / 86400);

            $output->writeln(">> {$ruleId} | {$url}");
            $output->writeln("   Fix implemented: {$fixDate} ({$daysAgo} days ago)");

            // Pull GSC data for this URL — before and after fix date
            $before = $this->getGscMetrics($url, $fixDate, 'before');
            $after  = $this->getGscMetrics($url, $fixDate, 'after');

            if (empty($before) && empty($after)) {
                $output->writeln("   [?] No GSC data found for this URL — skipping.");
                $output->writeln('');
                continue;
            }

            // Calculate changes
            $changes  = $this->calculateChanges($before, $after);
            $verdict  = $this->determineVerdict($ruleId, $changes);
            $icon     = match($verdict['status']) {
                'PASS'    => '[PASS]',
                'PARTIAL' => '[PARTIAL]',
                default   => '[FAIL]',
            };

            $output->writeln("   {$icon} Outcome: {$verdict['status']}");
            $output->writeln("   " . $verdict['reason']);
            $output->writeln('');
            $output->writeln('   GSC Before → After:');

            foreach ($changes as $metric => $change) {
                $dir   = $change['delta'] >= 0 ? '+' : '';
                $arrow = $change['delta'] >= 0 ? '↑' : '↓';
                $output->writeln("     {$metric}: {$change['before']} → {$change['after']}  ({$arrow}{$dir}{$change['delta_pct']}%)");
            }

            // Store outcome
            $this->storeOutcome($review, $changes, $verdict, $daysAgo);

            // ── LEARNING LOOP: LLM reviews the outcome and proposes next steps ──
            $assignee = $review['assigned_to'] ?? 'Team';
            $feedback = $this->generateLearningFeedback($review, $changes, $verdict, $output);

            if ($feedback) {
                $output->writeln('');
                $output->writeln("   ── Learning Feedback for {$assignee} ──");
                $output->writeln("   " . str_replace("\n", "\n   ", $feedback['summary']));

                if (!empty($feedback['rule_proposal'])) {
                    $output->writeln('');
                    $output->writeln("   ⚡ PROPOSED RULE CHANGE:");
                    $output->writeln("   " . str_replace("\n", "\n   ", $feedback['rule_proposal']));
                }

                // Store feedback
                $this->storeLearningFeedback($review, $verdict, $feedback);
            }

            // Next action recommendation
            $output->writeln('');
            $output->writeln('   Next action: ' . $verdict['next_action']);
            $output->writeln('');

            match($verdict['status']) {
                'PASS'    => $totalPass++,
                'PARTIAL' => $totalPartial++,
                default   => $totalFail++,
            };
        }

        // Summary
        $output->writeln('==============================================');
        $output->writeln("OUTCOMES: {$totalPass} PASS | {$totalPartial} PARTIAL | {$totalFail} FAIL");
        $output->writeln('');

        if ($totalFail > 0) {
            $output->writeln("  Failed fixes need re-evaluation. Run: php bin/console app:evaluate-rule");
        }
        if ($totalPass > 0) {
            $output->writeln("  PASS outcomes stored in rule_outcomes table.");
        }

        $output->writeln("  View all: SELECT * FROM rule_outcomes ORDER BY verified_at DESC;");

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  GET REVIEWS READY FOR VERIFICATION
    // ─────────────────────────────────────────────

    private function getReviewsReadyForVerification(int $minDays, ?string $ruleFilter, bool $force): array
    {
        try {
            $cutoffDate = date('Y-m-d', strtotime("-{$minDays} days"));
            $rows = [];

            // PRIMARY SOURCE: tasks table — completed tasks with recheck_date that has arrived
            try {
                $sql = "SELECT
                            id as review_id,
                            rule_id,
                            title,
                            description,
                            completed_at as fix_implemented_at,
                            recheck_date,
                            recheck_criteria,
                            recheck_verified,
                            'task' as source_type
                        FROM tasks
                        WHERE status = 'done'
                        AND recheck_date IS NOT NULL
                        AND recheck_date <= CURRENT_DATE
                        AND completed_at IS NOT NULL";

                if ($ruleFilter) {
                    $sql .= " AND rule_id = :rule_id";
                }
                if (!$force) {
                    $sql .= " AND (recheck_verified = FALSE OR recheck_verified IS NULL)";
                }
                $sql .= " ORDER BY recheck_date ASC";

                $params = [];
                if ($ruleFilter) $params['rule_id'] = strtoupper($ruleFilter);

                $taskRows = $this->db->fetchAllAssociative($sql, $params);

                // Extract URLs from task titles (format: [RULE_ID] Title — /url-path/)
                foreach ($taskRows as $task) {
                    $url = '';
                    if (preg_match('#(/[a-z0-9\-/]+/)#', $task['title'], $m)) {
                        $url = $m[1];
                    }
                    if (!$url && preg_match('#(/[a-z0-9\-/]+/)#', $task['description'] ?? '', $m)) {
                        $url = $m[1];
                    }
                    if ($url) {
                        $task['url'] = $url;
                        $rows[] = $task;
                    }
                }
            } catch (\Exception $e) {
                // tasks table query failed — continue to fallback
            }

            // SECONDARY SOURCE: rule_reviews table
            try {
                $tables = $this->db->fetchFirstColumn(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                );

                if (in_array('rule_reviews', $tables)) {
                    $sql = "SELECT
                                id as review_id,
                                rule_id,
                                feedback as title,
                                '' as description,
                                fix_implemented_at,
                                NULL as recheck_date,
                                NULL as recheck_criteria,
                                FALSE as recheck_verified,
                                'review' as source_type
                            FROM rule_reviews
                            WHERE fix_implemented_at IS NOT NULL
                            AND fix_implemented_at <= :cutoff
                            AND status IN ('accepted', 'fix_implemented', 'done')";

                    if ($ruleFilter) {
                        $sql .= " AND rule_id = :rule_id";
                    }
                    if (!$force) {
                        $sql .= " AND (outcome_verified_at IS NULL OR outcome_verified_at < fix_implemented_at)";
                    }

                    $params = ['cutoff' => $cutoffDate];
                    if ($ruleFilter) $params['rule_id'] = strtoupper($ruleFilter);

                    $reviewRows = $this->db->fetchAllAssociative($sql, $params);
                    foreach ($reviewRows as $r) {
                        if (!empty($r['url'])) {
                            $r['url'] = $r['url'];
                            $rows[] = $r;
                        }
                    }
                }
            } catch (\Exception $e) {
                // rule_reviews fallback failed — continue
            }

            return $rows;
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  GET GSC METRICS FOR A URL BEFORE/AFTER FIX
    // ─────────────────────────────────────────────

    private function getGscMetrics(string $url, string $fixDate, string $period): array
    {
        try {
            // Normalise URL — GSC stores full URLs
            $urlPattern = '%' . ltrim($url, '/');

            if ($period === 'before') {
                // 28 days prior to fix date
                $sql = "SELECT
                            AVG(position)    as avg_position,
                            SUM(clicks)      as total_clicks,
                            SUM(impressions) as total_impressions,
                            AVG(ctr) * 100   as avg_ctr_pct
                        FROM gsc_snapshots
                        WHERE page LIKE :url
                        AND fetched_at < :fix_date
                        AND date_range = '28d'";
            } else {
                // Most recent 28d data after fix
                $sql = "SELECT
                            AVG(position)    as avg_position,
                            SUM(clicks)      as total_clicks,
                            SUM(impressions) as total_impressions,
                            AVG(ctr) * 100   as avg_ctr_pct
                        FROM gsc_snapshots
                        WHERE page LIKE :url
                        AND fetched_at >= :fix_date
                        AND date_range = '28d'";
            }

            $row = $this->db->fetchAssociative($sql, ['url' => $urlPattern, 'fix_date' => $fixDate]);

            if (!$row || is_null($row['total_impressions'])) {
                return [];
            }

            return [
                'position'    => round((float) ($row['avg_position']    ?? 0), 1),
                'clicks'      => (int)   ($row['total_clicks']      ?? 0),
                'impressions' => (int)   ($row['total_impressions']  ?? 0),
                'ctr'         => round((float) ($row['avg_ctr_pct']     ?? 0), 2),
            ];
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  CALCULATE METRIC CHANGES
    // ─────────────────────────────────────────────

    private function calculateChanges(array $before, array $after): array
    {
        $changes = [];
        $metrics = ['impressions', 'clicks', 'ctr', 'position'];

        foreach ($metrics as $metric) {
            $bVal = $before[$metric] ?? 0;
            $aVal = $after[$metric]  ?? 0;
            $delta = $aVal - $bVal;

            // Position: lower is better, so invert for display
            $deltaPct = $bVal != 0
                ? round(($delta / abs($bVal)) * 100, 1)
                : ($aVal > 0 ? 100 : 0);

            $changes[$metric] = [
                'before'    => $bVal,
                'after'     => $aVal,
                'delta'     => $delta,
                'delta_pct' => $deltaPct,
            ];
        }

        return $changes;
    }

    // ─────────────────────────────────────────────
    //  DETERMINE VERDICT
    // ─────────────────────────────────────────────

    private function determineVerdict(string $ruleId, array $changes): array
    {
        // Match threshold by rule ID prefix (e.g., OPQ-001 matches 'OPQ', DDT-SD-002 matches 'DDT-SD')
        $threshold = self::THRESHOLDS['DEFAULT'];
        foreach (self::THRESHOLDS as $prefix => $t) {
            if ($prefix !== 'DEFAULT' && str_starts_with($ruleId, $prefix)) {
                $threshold = $t;
                break;
            }
        }
        $metric    = $threshold['metric'];
        $passThreshold    = $threshold['pass'];
        $partialThreshold = $threshold['partial'];

        $deltaPct = $changes[$metric]['delta_pct'] ?? 0;
        $delta    = $changes[$metric]['delta']     ?? 0;

        // Position is special — negative delta = improvement (moved up)
        $improvement = ($metric === 'position') ? ($delta * -1) : $delta;
        $improvPct   = ($metric === 'position') ? ($deltaPct * -1) : $deltaPct;

        if ($improvPct >= $passThreshold || $improvement >= $passThreshold) {
            $status     = 'PASS';
            $reason     = "Fix succeeded: {$metric} improved by {$improvPct}% (threshold: {$passThreshold}%).";
            $nextAction = "Close task. Schedule 30-day stability check.";
        } elseif ($improvPct >= $partialThreshold || $improvement >= $partialThreshold) {
            $status     = 'PARTIAL';
            $reason     = "Partial improvement: {$metric} up {$improvPct}% (pass threshold: {$passThreshold}%, partial: {$partialThreshold}%).";
            $nextAction = "Strengthen fix — check internal links, title/H1 alignment, and page quality. Re-verify in 14 days.";
        } else {
            $status     = 'FAIL';
            $reason     = "No meaningful improvement in {$metric} ({$improvPct}% change). Fix may not have been indexed or is insufficient.";
            $nextAction = "Re-run app:evaluate-rule for this rule. Check if fix is live via app:crawl-pages --url={url}. Consider stronger intervention.";
        }

        return ['status' => $status, 'reason' => $reason, 'next_action' => $nextAction, 'metric' => $metric, 'improvement_pct' => $improvPct];
    }

    // ─────────────────────────────────────────────
    //  STORE OUTCOME
    // ─────────────────────────────────────────────

    private function storeOutcome(array $review, array $changes, array $verdict, int $daysAfterFix): void
    {
        try {
            $this->db->insert('rule_outcomes', [
                'rule_id'           => $review['rule_id'],
                'url'               => $review['url'],
                'review_id'         => $review['review_id'] ?? null,
                'fix_implemented_at'=> $review['fix_implemented_at'],
                'days_after_fix'    => $daysAfterFix,
                'metric_tracked'    => $verdict['metric'],
                'impressions_before'=> $changes['impressions']['before'] ?? 0,
                'impressions_after' => $changes['impressions']['after']  ?? 0,
                'clicks_before'     => $changes['clicks']['before']      ?? 0,
                'clicks_after'      => $changes['clicks']['after']       ?? 0,
                'position_before'   => $changes['position']['before']    ?? 0,
                'position_after'    => $changes['position']['after']     ?? 0,
                'ctr_before'        => $changes['ctr']['before']         ?? 0,
                'ctr_after'         => $changes['ctr']['after']          ?? 0,
                'improvement_pct'   => $verdict['improvement_pct'],
                'outcome_status'    => $verdict['status'],
                'outcome_reason'    => $verdict['reason'],
                'next_action'       => $verdict['next_action'],
                'verified_at'       => date('Y-m-d H:i:s'),
            ]);

            // Update the task on the Playbook Board
            $sourceType = $review['source_type'] ?? '';
            $taskId     = $review['review_id'] ?? null;

            if ($sourceType === 'task' && $taskId) {
                // Mark the task as verified
                $this->db->update('tasks', [
                    'recheck_verified' => true,
                    'recheck_result'   => strtolower($verdict['status']),
                ], ['id' => $taskId]);

                // Log activity
                try {
                    $this->db->insert('activity_log', [
                        'actor'        => 'Logiri',
                        'action'       => 'verified_outcome',
                        'target_type'  => 'task',
                        'target_id'    => $taskId,
                        'target_title' => $review['title'] ?? '',
                        'details'      => "{$verdict['status']}: {$verdict['reason']}",
                        'created_at'   => date('Y-m-d H:i:s'),
                    ]);
                } catch (\Exception $e) {}

                // For PARTIAL or FAIL: create a follow-up task on the board
                if (in_array($verdict['status'], ['PARTIAL', 'FAIL'])) {
                    $followUpTitle = "[RECHECK-{$verdict['status']}] {$review['rule_id']} — {$review['url']}";
                    $followUpDesc  = "PREVIOUS FIX RESULT: {$verdict['status']}\n"
                        . "Reason: {$verdict['reason']}\n\n"
                        . "GSC BEFORE → AFTER:\n"
                        . "- Impressions: {$changes['impressions']['before']} → {$changes['impressions']['after']}\n"
                        . "- Clicks: {$changes['clicks']['before']} → {$changes['clicks']['after']}\n"
                        . "- Position: {$changes['position']['before']} → {$changes['position']['after']}\n"
                        . "- CTR: {$changes['ctr']['before']}% → {$changes['ctr']['after']}%\n\n"
                        . "NEXT ACTION: {$verdict['next_action']}";

                    $priority = $verdict['status'] === 'FAIL' ? 'critical' : 'high';

                    // Check for existing follow-up to avoid duplicates
                    $existing = $this->db->fetchAssociative(
                        "SELECT id FROM tasks WHERE title LIKE :title AND status != 'done'",
                        ['title' => '%' . $review['url'] . '%RECHECK%']
                    );

                    if (!$existing) {
                        try {
                            $this->db->insert('tasks', [
                                'title'           => substr($followUpTitle, 0, 500),
                                'description'     => $followUpDesc,
                                'rule_id'         => $review['rule_id'],
                                'assigned_to'     => null,
                                'status'          => 'pending',
                                'priority'        => $priority,
                                'estimated_hours' => 2,
                                'logged_hours'    => 0,
                                'recheck_type'    => 'on_page_fix',
                                'created_at'      => date('Y-m-d H:i:s'),
                            ]);
                        } catch (\Exception $e) {}
                    }
                }
            }

            // Mark the source review as verified if from rule_reviews
            if ($sourceType === 'review' && isset($review['review_id'])) {
                try {
                    $this->db->executeStatement(
                        "UPDATE rule_reviews SET outcome_verified_at = :now, outcome_status = :status WHERE id = :id",
                        ['now' => date('Y-m-d H:i:s'), 'status' => $verdict['status'], 'id' => $review['review_id']]
                    );
                } catch (\Exception $e) {}
            }

            // ── LEARNING LOOP: Store feedback for LLM training ──
            try {
                $gscBefore = json_encode([
                    'impressions' => $changes['impressions']['before'] ?? 0,
                    'clicks'      => $changes['clicks']['before'] ?? 0,
                    'position'    => $changes['position']['before'] ?? 0,
                    'ctr'         => $changes['ctr']['before'] ?? 0,
                ]);
                $gscAfter = json_encode([
                    'impressions' => $changes['impressions']['after'] ?? 0,
                    'clicks'      => $changes['clicks']['after'] ?? 0,
                    'position'    => $changes['position']['after'] ?? 0,
                    'ctr'         => $changes['ctr']['after'] ?? 0,
                ]);

                $whatWorked = null;
                $whatDidntWork = null;
                $proposedChange = null;
                $changeType = 'none';

                if ($verdict['status'] === 'PASS') {
                    $whatWorked = "Fix for {$review['rule_id']} on {$review['url']} improved {$verdict['metric']} by {$verdict['improvement_pct']}%. Rule validated — keep as-is.";
                    $changeType = 'none';
                } elseif ($verdict['status'] === 'PARTIAL') {
                    $whatWorked = "{$verdict['metric']} showed some improvement ({$verdict['improvement_pct']}%) but below the pass threshold.";
                    $whatDidntWork = "The fix was directionally correct but insufficient. Play brief may need to be more specific or the rule threshold may be too aggressive.";
                    $proposedChange = "REVIEW_PLAY_BRIEF: Consider strengthening the play brief for {$review['rule_id']} — the current approach moved the needle but not enough. Try a more targeted fix.";
                    $changeType = 'refine_play';
                } else {
                    $whatDidntWork = "Fix for {$review['rule_id']} on {$review['url']} showed no improvement in {$verdict['metric']} ({$verdict['improvement_pct']}% change). Either the fix wasn't indexed, wasn't implemented correctly, or the rule's theory is wrong.";
                    $proposedChange = "REVIEW_RULE: {$review['rule_id']} failed to produce results on {$review['url']}. Either: (1) verify the fix is live and indexed, (2) try a different approach, or (3) if multiple URLs fail for this rule, consider modifying the rule's diagnosis and action output.";
                    $changeType = 'review_rule';
                }

                // Get assigned_to from the task if available
                $assignedTo = null;
                if ($taskId) {
                    try {
                        $task = $this->db->fetchAssociative("SELECT assigned_to FROM tasks WHERE id = ?", [$taskId]);
                        $assignedTo = $task['assigned_to'] ?? null;
                    } catch (\Exception $e) {}
                }

                $this->db->insert('rule_feedback', [
                    'rule_id'          => $review['rule_id'],
                    'url'              => $review['url'],
                    'task_id'          => $taskId,
                    'assigned_to'      => $assignedTo,
                    'outcome_status'   => $verdict['status'],
                    'fix_description'  => $review['title'] ?? $review['description'] ?? '',
                    'gsc_before'       => $gscBefore,
                    'gsc_after'        => $gscAfter,
                    'what_worked'      => $whatWorked,
                    'what_didnt_work'  => $whatDidntWork,
                    'proposed_change'  => $proposedChange,
                    'change_type'      => $changeType,
                    'created_at'       => date('Y-m-d H:i:s'),
                ]);
            } catch (\Exception $e) {
                // Non-fatal — feedback storage failure doesn't block verification
            }
        } catch (\Exception $e) {
            // Non-fatal — outcome display still works even if storage fails
        }
    }

    // ─────────────────────────────────────────────
    //  ENSURE DB SCHEMA
    // ─────────────────────────────────────────────

    private function ensureSchema(): void
    {
        try {
            // Main outcomes table
            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS rule_outcomes (
                    id                  SERIAL PRIMARY KEY,
                    rule_id             VARCHAR(20) NOT NULL,
                    url                 TEXT NOT NULL,
                    review_id           INT,
                    fix_implemented_at  TIMESTAMP,
                    days_after_fix      INT DEFAULT 0,
                    metric_tracked      VARCHAR(30),
                    impressions_before  INT DEFAULT 0,
                    impressions_after   INT DEFAULT 0,
                    clicks_before       INT DEFAULT 0,
                    clicks_after        INT DEFAULT 0,
                    position_before     NUMERIC(6,1) DEFAULT 0,
                    position_after      NUMERIC(6,1) DEFAULT 0,
                    ctr_before          NUMERIC(6,2) DEFAULT 0,
                    ctr_after           NUMERIC(6,2) DEFAULT 0,
                    improvement_pct     NUMERIC(6,1) DEFAULT 0,
                    outcome_status      VARCHAR(20),
                    outcome_reason      TEXT,
                    next_action         TEXT,
                    verified_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");

            // Add outcome verification columns to rule_reviews if that table exists
            $tables = $this->db->fetchFirstColumn(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            );

            if (in_array('rule_reviews', $tables)) {
                $this->db->executeStatement("ALTER TABLE rule_reviews ADD COLUMN IF NOT EXISTS fix_implemented_at TIMESTAMP");
                $this->db->executeStatement("ALTER TABLE rule_reviews ADD COLUMN IF NOT EXISTS outcome_verified_at TIMESTAMP");
                $this->db->executeStatement("ALTER TABLE rule_reviews ADD COLUMN IF NOT EXISTS outcome_status VARCHAR(20)");
            }

            // Rule feedback table — stores what worked/didn't so LLMs learn from past outcomes
            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS rule_feedback (
                    id                  SERIAL PRIMARY KEY,
                    rule_id             VARCHAR(30) NOT NULL,
                    url                 TEXT NOT NULL,
                    task_id             INT,
                    assigned_to         VARCHAR(100),
                    outcome_status      VARCHAR(20) NOT NULL,
                    fix_description     TEXT,
                    gsc_before          JSONB DEFAULT '{}',
                    gsc_after           JSONB DEFAULT '{}',
                    what_worked         TEXT,
                    what_didnt_work     TEXT,
                    proposed_change     TEXT,
                    change_type         VARCHAR(30) DEFAULT 'none',
                    change_approved     BOOLEAN DEFAULT NULL,
                    approved_by         VARCHAR(100),
                    approved_at         TIMESTAMP,
                    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ");
        } catch (\Exception $e) {
            // Table may already exist
        }
    }

    // ─────────────────────────────────────────────
    //  GENERATE LEARNING FEEDBACK VIA LLM
    // ─────────────────────────────────────────────

    private function generateLearningFeedback(array $review, array $changes, array $verdict, OutputInterface $output): ?array
    {
        $claudeKey = $_ENV['ANTHROPIC_API_KEY'] ?? '';
        if (!$claudeKey) return null;

        $ruleId    = $review['rule_id'] ?? 'UNKNOWN';
        $url       = $review['url'] ?? '';
        $assignee  = $review['assigned_to'] ?? 'Team';
        $taskTitle = $review['title'] ?? '';
        $taskDesc  = substr($review['description'] ?? '', 0, 500);
        $status    = $verdict['status'];

        $gscSummary = "";
        foreach ($changes as $metric => $c) {
            $dir = $c['delta'] >= 0 ? '+' : '';
            $gscSummary .= "  {$metric}: {$c['before']} → {$c['after']} ({$dir}{$c['delta_pct']}%)\n";
        }

        $prompt = <<<PROMPT
You are the Logiri SEO learning engine for Double D Trailers (doubledtrailers.com).

A task was completed and the outcome has been verified against Google Search Console data.

RULE: {$ruleId}
URL: {$url}
TASK: {$taskTitle}
ORIGINAL PLAY BRIEF (summary): {$taskDesc}
OUTCOME: {$status}
COMPLETED BY: {$assignee}

GSC BEFORE → AFTER (28-day window):
{$gscSummary}

Based on this outcome, respond with EXACTLY this JSON structure (no markdown, no backticks):
{
  "summary": "2-3 sentence personalized feedback for {$assignee}. If PASS: what worked and why. If PARTIAL: what partially worked and what to try next. If FAIL: honest assessment of why it didn't work.",
  "what_worked": "Specific element of the fix that drove improvement (or 'Nothing measurable' for FAIL)",
  "what_didnt_work": "What didn't produce expected results (or 'N/A' for PASS)",
  "winning_pattern": "If PASS or PARTIAL: a reusable pattern other pages can follow. If FAIL: null",
  "rule_proposal": "If FAIL: propose a specific modification to rule {$ruleId} — what should the rule check differently? If PASS/PARTIAL: null",
  "change_type": "One of: none, refine_threshold, modify_diagnosis, modify_action, deprecate_rule, split_rule"
}

IMPORTANT:
- Be specific to Double D Trailers: reference Z-Frame, SafeTack, SafeBump, SafeKick where relevant
- For PASS: identify the exact signal that improved (not generic "good job")
- For FAIL: be honest — was the rule's theory wrong, or was the fix insufficient?
- Keep summary under 100 words
- Do NOT wrap in markdown code blocks
PROMPT;

        try {
            $ch = curl_init('https://api.anthropic.com/v1/messages');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode([
                    'model'      => 'claude-sonnet-4-6',
                    'max_tokens' => 800,
                    'messages'   => [['role' => 'user', 'content' => $prompt]],
                ]),
                CURLOPT_HTTPHEADER => [
                    'Content-Type: application/json',
                    'x-api-key: ' . $claudeKey,
                    'anthropic-version: 2023-06-01',
                ],
                CURLOPT_TIMEOUT => 60,
            ]);

            $response = curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            if ($httpCode !== 200 || !$response) return null;

            $data = json_decode($response, true);
            $text = $data['content'][0]['text'] ?? '';

            // Strip markdown fences if present
            $text = preg_replace('/^```json\s*/', '', $text);
            $text = preg_replace('/\s*```$/', '', $text);

            $parsed = json_decode($text, true);
            if (!$parsed || !isset($parsed['summary'])) return null;

            return $parsed;
        } catch (\Exception $e) {
            $output->writeln("   [WARN] Learning feedback LLM call failed: " . substr($e->getMessage(), 0, 80));
            return null;
        }
    }

    // ─────────────────────────────────────────────
    //  STORE LEARNING FEEDBACK
    // ─────────────────────────────────────────────

    private function storeLearningFeedback(array $review, array $verdict, array $feedback): void
    {
        try {
            $this->db->insert('rule_feedback', [
                'rule_id'         => $review['rule_id'],
                'url'             => $review['url'] ?? '',
                'task_id'         => $review['review_id'] ?? null,
                'assigned_to'     => $review['assigned_to'] ?? null,
                'outcome_status'  => $verdict['status'],
                'fix_description' => substr($review['title'] ?? '', 0, 500),
                'gsc_before'      => json_encode($review['gsc_before'] ?? []),
                'gsc_after'       => json_encode($review['gsc_after'] ?? []),
                'what_worked'     => $feedback['what_worked'] ?? null,
                'what_didnt_work' => $feedback['what_didnt_work'] ?? null,
                'proposed_change' => $feedback['rule_proposal'] ?? null,
                'change_type'     => $feedback['change_type'] ?? 'none',
                'created_at'      => date('Y-m-d H:i:s'),
            ]);
        } catch (\Exception $e) {
            // Non-fatal
        }
    }
}
    
