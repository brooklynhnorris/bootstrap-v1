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

    // Outcome thresholds per rule category
    private const THRESHOLDS = [
        // FC rules — on-page structural fixes
        'FC-R1'  => ['metric' => 'impressions', 'pass' => 20,  'partial' => 10,  'window' => 28],
        'FC-R2'  => ['metric' => 'impressions', 'pass' => 15,  'partial' => 5,   'window' => 28],
        'FC-R3'  => ['metric' => 'position',    'pass' => -2,  'partial' => -1,  'window' => 28],
        'FC-R5'  => ['metric' => 'clicks',      'pass' => 15,  'partial' => 5,   'window' => 28],
        'FC-R6'  => ['metric' => 'position',    'pass' => -2,  'partial' => -1,  'window' => 28],
        'FC-R7'  => ['metric' => 'ctr',         'pass' => 10,  'partial' => 5,   'window' => 28],
        'FC-R8'  => ['metric' => 'impressions', 'pass' => 15,  'partial' => 5,   'window' => 28],
        'FC-R9'  => ['metric' => 'impressions', 'pass' => 20,  'partial' => 10,  'window' => 28],
        'FC-R10' => ['metric' => 'clicks',      'pass' => 20,  'partial' => 10,  'window' => 28],
        // Default fallback
        'DEFAULT' => ['metric' => 'impressions', 'pass' => 10, 'partial' => 5,   'window' => 28],
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
            // Pull from rule_reviews if it exists, otherwise fall back to rule_evaluations
            // rule_reviews = tasks that have been marked as fix implemented
            // We look for: status = 'accepted' OR tasks marked done with a fix date
            $cutoffDate = date('Y-m-d H:i:s', strtotime("-{$minDays} days"));

            // Try rule_reviews first (the proper task table)
            $tables = $this->db->fetchFirstColumn(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            );

            $source = in_array('rule_reviews', $tables) ? 'rule_reviews' : null;

            if ($source) {
                $sql = "SELECT * FROM rule_reviews
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

                return $this->db->fetchAllAssociative($sql, $params);
            }

            // Fallback: use rule_evaluations — find FLAGGED rules that were evaluated
            // and assume a fix was attempted if the rule stops firing
            $sql = "SELECT
                        re.rule_id,
                        su.url,
                        re.evaluated_at as fix_implemented_at,
                        re.sample_urls,
                        re.id as review_id,
                        re.consensus_status
                    FROM rule_evaluations re
                    CROSS JOIN LATERAL (
                        SELECT jsonb_array_elements_text(sample_urls::jsonb) as url
                    ) su
                    WHERE re.consensus_status = 'FLAGGED'
                    AND re.evaluated_at <= :cutoff";

            if ($ruleFilter) {
                $sql .= " AND re.rule_id = :rule_id";
            }

            $params = ['cutoff' => $cutoffDate];
            if ($ruleFilter) $params['rule_id'] = strtoupper($ruleFilter);

            $rows = $this->db->fetchAllAssociative($sql, $params);

            // If no rule_reviews table, synthesise minimal review records from evaluations
            if (empty($rows)) {
                // Try simpler fallback without LATERAL join
                $evalSql = "SELECT * FROM rule_evaluations
                            WHERE consensus_status = 'FLAGGED'
                            AND evaluated_at <= :cutoff";
                if ($ruleFilter) $evalSql .= " AND rule_id = :rule_id";

                $evals = $this->db->fetchAllAssociative($evalSql, $params);
                $rows  = [];
                foreach ($evals as $eval) {
                    $urls = json_decode($eval['sample_urls'] ?? '[]', true);
                    foreach (array_slice($urls, 0, 3) as $url) {
                        $rows[] = [
                            'rule_id'             => $eval['rule_id'],
                            'url'                 => $url,
                            'fix_implemented_at'  => $eval['evaluated_at'],
                            'review_id'           => $eval['id'],
                            'consensus_status'    => $eval['consensus_status'],
                        ];
                    }
                }
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
        $threshold = self::THRESHOLDS[$ruleId] ?? self::THRESHOLDS['DEFAULT'];
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

            // Mark the source review as verified if it exists
            if (isset($review['review_id'])) {
                try {
                    $this->db->executeStatement(
                        "UPDATE rule_reviews SET outcome_verified_at = :now, outcome_status = :status WHERE id = :id",
                        ['now' => date('Y-m-d H:i:s'), 'status' => $verdict['status'], 'id' => $review['review_id']]
                    );
                } catch (\Exception $e) {
                    // rule_reviews may not have these columns yet — non-fatal
                }
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
        } catch (\Exception $e) {
            // Table may already exist
        }
    }
}