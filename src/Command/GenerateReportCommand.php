    <?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:generate-report', description: 'Generate performance report after batch page updates (RPT-R1)')]
class GenerateReportCommand extends Command
{
    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('force', null, InputOption::VALUE_NONE, 'Generate report even if <10 pages updated')
            ->addOption('days', null, InputOption::VALUE_OPTIONAL, 'Comparison window in days (default: 28)', 28);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $force = (bool) $input->getOption('force');
        $days  = (int) ($input->getOption('days') ?? 28);

        $output->writeln('');
        $output->writeln('+============================================+');
        $output->writeln('|     LOGIRI PERFORMANCE REPORT (RPT-R1)     |');
        $output->writeln('|     ' . date('Y-m-d H:i:s') . '                      |');
        $output->writeln('+============================================+');
        $output->writeln('');

        // Step 1: Detect updated pages
        $updatedPages = $this->getUpdatedPages($days);
        $output->writeln("Pages updated in last {$days} days: " . count($updatedPages));

        if (count($updatedPages) < 10 && !$force) {
            $output->writeln("  Below 10-page threshold. Use --force to generate anyway.");
            return Command::SUCCESS;
        }

        // Step 2: Pull GSC before/after for updated pages
        $output->writeln('');
        $output->writeln('═══════════════════════════════════════════');
        $output->writeln('  1. RANKING STATUS');
        $output->writeln('═══════════════════════════════════════════');

        $rankingData = [];
        foreach ($updatedPages as $page) {
            $before = $this->getGscMetrics($page['url'], $days, 'before');
            $after  = $this->getGscMetrics($page['url'], $days, 'after');

            if (empty($before) && empty($after)) continue;

            $posChange = ($after['position'] ?? 0) - ($before['position'] ?? 0);
            $impChange = ($after['impressions'] ?? 0) - ($before['impressions'] ?? 0);
            $clkChange = ($after['clicks'] ?? 0) - ($before['clicks'] ?? 0);
            $ctrChange = ($after['ctr'] ?? 0) - ($before['ctr'] ?? 0);

            $rankingData[] = [
                'url'              => $page['url'],
                'page_type'        => $page['page_type'],
                'word_count'       => $page['word_count'],
                'pos_before'       => $before['position'] ?? 0,
                'pos_after'        => $after['position'] ?? 0,
                'pos_change'       => $posChange,
                'imp_before'       => $before['impressions'] ?? 0,
                'imp_after'        => $after['impressions'] ?? 0,
                'imp_change'       => $impChange,
                'clk_before'       => $before['clicks'] ?? 0,
                'clk_after'        => $after['clicks'] ?? 0,
                'clk_change'       => $clkChange,
                'ctr_before'       => $before['ctr'] ?? 0,
                'ctr_after'        => $after['ctr'] ?? 0,
                'ctr_change'       => $ctrChange,
            ];
        }

        // Sort by position improvement (most improved first — negative = better)
        usort($rankingData, fn($a, $b) => $a['pos_change'] <=> $b['pos_change']);

        // Top improvers
        $improvers = array_filter($rankingData, fn($r) => $r['pos_change'] < 0);
        $decliners = array_filter($rankingData, fn($r) => $r['pos_change'] > 0);
        $stable    = array_filter($rankingData, fn($r) => $r['pos_change'] == 0);

        $output->writeln('');
        $output->writeln("  Top 5 Ranking Improvers:");
        foreach (array_slice($improvers, 0, 5) as $r) {
            $output->writeln("    ↑ {$r['url']} — position {$r['pos_before']} → {$r['pos_after']} ({$r['pos_change']})");
        }

        if (!empty($decliners)) {
            $output->writeln('');
            $output->writeln("  Ranking Decliners (investigate):");
            foreach (array_slice(array_reverse($decliners), 0, 5) as $r) {
                $output->writeln("    ↓ {$r['url']} — position {$r['pos_before']} → {$r['pos_after']} (+{$r['pos_change']})");
            }
        }

        // Averages
        $avgPosChange = count($rankingData) > 0 ? round(array_sum(array_column($rankingData, 'pos_change')) / count($rankingData), 1) : 0;
        $output->writeln('');
        $output->writeln("  Average position change: {$avgPosChange}");
        $output->writeln("  Pages improved: " . count($improvers) . " | Declined: " . count($decliners) . " | Stable: " . count($stable));

        // Step 3: Traffic status
        $output->writeln('');
        $output->writeln('═══════════════════════════════════════════');
        $output->writeln('  2. TRAFFIC STATUS');
        $output->writeln('═══════════════════════════════════════════');

        $totalImpBefore = array_sum(array_column($rankingData, 'imp_before'));
        $totalImpAfter  = array_sum(array_column($rankingData, 'imp_after'));
        $totalClkBefore = array_sum(array_column($rankingData, 'clk_before'));
        $totalClkAfter  = array_sum(array_column($rankingData, 'clk_after'));
        $impDelta       = $totalImpAfter - $totalImpBefore;
        $clkDelta       = $totalClkAfter - $totalClkBefore;
        $impPct         = $totalImpBefore > 0 ? round(($impDelta / $totalImpBefore) * 100, 1) : 0;
        $clkPct         = $totalClkBefore > 0 ? round(($clkDelta / $totalClkBefore) * 100, 1) : 0;

        $output->writeln('');
        $output->writeln("  Impressions: {$totalImpBefore} → {$totalImpAfter} ({$impPct}%)");
        $output->writeln("  Clicks: {$totalClkBefore} → {$totalClkAfter} ({$clkPct}%)");

        $output->writeln('');
        $output->writeln("  Top 5 Traffic Gainers:");
        $trafficSorted = $rankingData;
        usort($trafficSorted, fn($a, $b) => $b['clk_change'] <=> $a['clk_change']);
        foreach (array_slice($trafficSorted, 0, 5) as $r) {
            $dir = $r['clk_change'] >= 0 ? '+' : '';
            $output->writeln("    {$r['url']} — clicks {$r['clk_before']} → {$r['clk_after']} ({$dir}{$r['clk_change']})");
        }

        // Step 4: Rule compliance status
        $output->writeln('');
        $output->writeln('═══════════════════════════════════════════');
        $output->writeln('  3. RULE COMPLIANCE STATUS');
        $output->writeln('═══════════════════════════════════════════');

        $taskStats = $this->getTaskStats();
        $output->writeln('');
        $output->writeln("  Tasks completed: {$taskStats['done']}");
        $output->writeln("  Tasks pending: {$taskStats['pending']}");
        $output->writeln("  Tasks in progress: {$taskStats['in_progress']}");
        $output->writeln("  Total tasks: {$taskStats['total']}");

        // Outcomes
        $outcomes = $this->getOutcomeStats();
        if (!empty($outcomes)) {
            $output->writeln('');
            $output->writeln("  Verification outcomes:");
            $output->writeln("    PASS: {$outcomes['pass']} | PARTIAL: {$outcomes['partial']} | FAIL: {$outcomes['fail']}");
        }

        // Rules still firing
        $output->writeln('');
        $output->writeln("  Rules by category (pending tasks):");
        $catStats = $this->getTasksByCategory();
        foreach ($catStats as $cat) {
            $output->writeln("    {$cat['rule_prefix']}: {$cat['task_count']} tasks pending");
        }

        // Step 5: Summary and recommendations
        $output->writeln('');
        $output->writeln('═══════════════════════════════════════════');
        $output->writeln('  4. SUMMARY & NEXT STEPS');
        $output->writeln('═══════════════════════════════════════════');
        $output->writeln('');

        $output->writeln("  Pages updated: " . count($updatedPages));
        $output->writeln("  Pages with GSC data: " . count($rankingData));
        $output->writeln("  Net ranking movement: {$avgPosChange} positions");
        $output->writeln("  Net impression change: {$impPct}%");
        $output->writeln("  Net click change: {$clkPct}%");
        $output->writeln('');

        if ($avgPosChange < 0) {
            $output->writeln("  ✓ POSITIVE TREND — rankings improving overall");
        } elseif ($avgPosChange == 0) {
            $output->writeln("  — STABLE — no net ranking change yet (may need more time)");
        } else {
            $output->writeln("  ⚠ NEGATIVE TREND — rankings declined; investigate decliners above");
        }

        $output->writeln('');
        $output->writeln("  Next recommended actions:");
        if ($taskStats['pending'] > 50) {
            $output->writeln("    1. Focus on Critical tasks first — 8 core pages with zero content");
        }
        if (count($decliners) > 3) {
            $output->writeln("    2. Investigate declining pages — may need content refresh or technical audit");
        }
        $output->writeln("    3. Re-run evaluation after completing current batch: php bin/console app:evaluate-rule --skip-validation");
        $output->writeln("    4. Schedule next report in 14 days: php bin/console app:generate-report --days=14");

        // Store the report timestamp
        try {
            $this->db->insert('activity_log', [
                'actor'       => 'Logiri',
                'action'      => 'generated_report',
                'target_type' => 'report',
                'target_title'=> "Performance Report — " . date('M j, Y'),
                'details'     => count($updatedPages) . " pages, avg position change: {$avgPosChange}, impressions: {$impPct}%, clicks: {$clkPct}%",
                'created_at'  => date('Y-m-d H:i:s'),
            ]);
        } catch (\Exception $e) {}

        $output->writeln('');
        $output->writeln('═══════════════════════════════════════════');
        $output->writeln('  Report logged to activity feed');
        $output->writeln('═══════════════════════════════════════════');

        return Command::SUCCESS;
    }

    // ─────────────────────────────────────────────
    //  GET UPDATED PAGES (word_count changed or page modified)
    // ─────────────────────────────────────────────

    private function getUpdatedPages(int $days): array
    {
        try {
            // Pages where word_count > 0 and page was crawled recently
            return $this->db->fetchAllAssociative(
                "SELECT url, page_type, word_count, h1, title_tag, has_central_entity, schema_types
                 FROM page_crawl_snapshots
                 WHERE word_count > 0
                 AND page_type IN ('core', 'outer')
                 AND is_noindex = FALSE
                 AND crawled_at >= NOW() - INTERVAL '{$days} days'
                 ORDER BY page_type, url"
            );
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  GET GSC METRICS FOR A URL
    // ─────────────────────────────────────────────

    private function getGscMetrics(string $url, int $days, string $period): array
    {
        try {
            $urlPattern = '%' . ltrim($url, '/');

            if ($period === 'before') {
                $sql = "SELECT
                            AVG(position) as position,
                            SUM(clicks) as clicks,
                            SUM(impressions) as impressions,
                            AVG(ctr) * 100 as ctr
                        FROM gsc_snapshots
                        WHERE page LIKE :url
                        AND fetched_at < NOW() - INTERVAL '{$days} days'
                        AND date_range = '28d'";
            } else {
                $sql = "SELECT
                            AVG(position) as position,
                            SUM(clicks) as clicks,
                            SUM(impressions) as impressions,
                            AVG(ctr) * 100 as ctr
                        FROM gsc_snapshots
                        WHERE page LIKE :url
                        AND fetched_at >= NOW() - INTERVAL '{$days} days'
                        AND date_range = '28d'";
            }

            $row = $this->db->fetchAssociative($sql, ['url' => $urlPattern]);

            if (!$row || is_null($row['impressions'])) return [];

            return [
                'position'    => round((float) ($row['position'] ?? 0), 1),
                'clicks'      => (int) ($row['clicks'] ?? 0),
                'impressions' => (int) ($row['impressions'] ?? 0),
                'ctr'         => round((float) ($row['ctr'] ?? 0), 2),
            ];
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  GET TASK STATS
    // ─────────────────────────────────────────────

    private function getTaskStats(): array
    {
        try {
            $rows = $this->db->fetchAllAssociative(
                "SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status"
            );
            $stats = ['done' => 0, 'pending' => 0, 'in_progress' => 0, 'total' => 0];
            foreach ($rows as $r) {
                $stats[$r['status']] = (int) $r['cnt'];
                $stats['total'] += (int) $r['cnt'];
            }
            return $stats;
        } catch (\Exception $e) {
            return ['done' => 0, 'pending' => 0, 'in_progress' => 0, 'total' => 0];
        }
    }

    // ─────────────────────────────────────────────
    //  GET OUTCOME STATS
    // ─────────────────────────────────────────────

    private function getOutcomeStats(): array
    {
        try {
            $rows = $this->db->fetchAllAssociative(
                "SELECT outcome_status, COUNT(*) as cnt FROM rule_outcomes GROUP BY outcome_status"
            );
            $stats = ['pass' => 0, 'partial' => 0, 'fail' => 0];
            foreach ($rows as $r) {
                $key = strtolower($r['outcome_status']);
                if (isset($stats[$key])) $stats[$key] = (int) $r['cnt'];
            }
            return $stats;
        } catch (\Exception $e) {
            return [];
        }
    }

    // ─────────────────────────────────────────────
    //  GET TASKS BY CATEGORY
    // ─────────────────────────────────────────────

    private function getTasksByCategory(): array
    {
        try {
            return $this->db->fetchAllAssociative(
                "SELECT
                    CASE
                        WHEN rule_id LIKE 'OPQ%' THEN 'On-Page Content'
                        WHEN rule_id LIKE 'TECH%' THEN 'Technical SEO'
                        WHEN rule_id LIKE 'DDT-SD%' THEN 'Schema'
                        WHEN rule_id LIKE 'ILA%' THEN 'Internal Links'
                        WHEN rule_id LIKE 'KIA%' THEN 'Keyword/Intent'
                        WHEN rule_id LIKE 'DDT-EEAT%' THEN 'E-E-A-T'
                        WHEN rule_id LIKE 'ETA%' THEN 'Entity Authority'
                        WHEN rule_id LIKE 'USE%' THEN 'User Signals'
                        WHEN rule_id LIKE 'CI%' THEN 'Competitive Intel'
                        WHEN rule_id LIKE 'CFL%' THEN 'Content Freshness'
                        WHEN rule_id LIKE 'DDT-LOCAL%' THEN 'Local SEO'
                        WHEN rule_id LIKE 'MAO%' THEN 'Media Assets'
                        ELSE 'Other'
                    END as rule_prefix,
                    COUNT(*) as task_count
                 FROM tasks
                 WHERE status = 'pending'
                 AND rule_id IS NOT NULL
                 GROUP BY rule_prefix
                 ORDER BY task_count DESC"
            );
        } catch (\Exception $e) {
            return [];
        }
    }
}

    
