    <?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:seed-rules', description: 'Import rules from system-prompt.txt into seo_rules database table')]
class SeedRulesCommand extends Command
{
    public function __construct(private Connection $db)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('force', null, InputOption::VALUE_NONE, 'Re-import all rules even if table is populated');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $force = $input->getOption('force');

        // Create table if not exists
        $this->db->executeStatement("
            CREATE TABLE IF NOT EXISTS seo_rules (
                id SERIAL PRIMARY KEY,
                rule_id VARCHAR(30) NOT NULL UNIQUE,
                name TEXT NOT NULL DEFAULT '',
                category VARCHAR(100) DEFAULT NULL,
                tier VARCHAR(50) DEFAULT 'A',
                trigger_source TEXT DEFAULT 'page_crawl_snapshots',
                trigger_condition TEXT DEFAULT '',
                trigger_sql TEXT DEFAULT '',
                threshold TEXT DEFAULT '',
                diagnosis TEXT DEFAULT '',
                action_output TEXT DEFAULT '',
                priority VARCHAR(20) DEFAULT 'Medium',
                assigned VARCHAR(100) DEFAULT 'Brook',
                ai_relevance TEXT DEFAULT '',
                full_text TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT NOW(),
                updated_by VARCHAR(100) DEFAULT 'system',
                created_at TIMESTAMP DEFAULT NOW()
            )
        ");

        // Check if already populated
        $count = (int) $this->db->fetchOne('SELECT COUNT(*) FROM seo_rules');
        if ($count > 0 && !$force) {
            $output->writeln("seo_rules already has {$count} rules. Use --force to re-import.");
            return Command::SUCCESS;
        }

        // Read system-prompt.txt
        $promptPath = dirname(__DIR__, 2) . '/system-prompt.txt';
        if (!file_exists($promptPath)) {
            $output->writeln("system-prompt.txt not found at {$promptPath}");
            return Command::FAILURE;
        }

        $content = file_get_contents($promptPath);
        $output->writeln("Read system-prompt.txt: " . strlen($content) . " bytes");

        // Parse rules using the same regex as loadRules()
        preg_match_all('/\n([A-Z][A-Z0-9]+(?:-[A-Z0-9]+)*-[A-Z]?\d+[a-z]?)\s*\|\s*([^\n]+)\n(.*?)(?=\n[A-Z][A-Z0-9]+(?:-[A-Z0-9]+)*-[A-Z]?\d+[a-z]?\s*\||\nSECTION\s+\d+|\nRESULTS VERIFICATION|\n={10,}|\z)/s', $content, $matches, PREG_SET_ORDER);

        $output->writeln("Found " . count($matches) . " rules to import.");

        $imported = 0;
        $skipped  = 0;

        foreach ($matches as $match) {
            $ruleId   = trim($match[1]);
            $name     = trim($match[2]);
            $ruleText = trim($match[3]);

            // Parse fields
            $triggerSource    = '';
            $triggerCondition = '';
            $triggerSql       = '';
            $threshold        = '';
            $diagnosis        = '';
            $actionOutput     = '';
            $priority         = '';
            $assigned         = '';
            $aiRelevance      = '';
            $tier             = 'A';
            $category         = '';

            if (preg_match('/Trigger Source:\s*([^\n]+)/', $ruleText, $m)) $triggerSource = trim($m[1]);
            if (preg_match('/Trigger Condition:\s*(.*?)(?=\nThreshold:|$)/s', $ruleText, $m)) {
                $triggerCondition = trim($m[1]);
                $triggerSql = preg_replace('/```sql\s*/', '', $triggerCondition);
                $triggerSql = preg_replace('/```\s*/', '', $triggerSql);
                $triggerSql = trim($triggerSql);
            }
            if (preg_match('/Threshold:\s*(.*?)(?=\nDiagnosis:|$)/s', $ruleText, $m)) $threshold = trim($m[1]);
            if (preg_match('/Diagnosis:\s*(.*?)(?=\nAction Output:|$)/s', $ruleText, $m)) $diagnosis = trim($m[1]);
            if (preg_match('/Action Output:\s*(.*?)(?=\nPriority:|$)/s', $ruleText, $m)) $actionOutput = trim($m[1]);
            if (preg_match('/Priority:\s*([^\n]+)/', $ruleText, $m)) $priority = trim($m[1]);
            if (preg_match('/Assigned:\s*([^\n]+)/', $ruleText, $m)) $assigned = trim($m[1]);
            if (preg_match('/AI Search Relevance:\s*(.*?)(?=\nData Needed:|Tier:|$)/s', $ruleText, $m)) $aiRelevance = trim($m[1]);
            if (preg_match('/Tier:\s*([^\n]+)/', $ruleText, $m)) $tier = trim($m[1]);

            // Infer category from rule ID prefix
            $category = match(true) {
                str_starts_with($ruleId, 'OPQ')       => 'On-Page Content Quality',
                str_starts_with($ruleId, 'TECH')      => 'Technical SEO',
                str_starts_with($ruleId, 'SCH'),
                str_starts_with($ruleId, 'DDT-SD')    => 'Schema & Structured Data',
                str_starts_with($ruleId, 'ILA')       => 'Internal Link Architecture',
                str_starts_with($ruleId, 'KIA')       => 'Keyword & Intent Alignment',
                str_starts_with($ruleId, 'DDT-EEAT')  => 'E-E-A-T & Trust Signals',
                str_starts_with($ruleId, 'ETA')       => 'Entity & Topical Authority',
                str_starts_with($ruleId, 'USE')       => 'User Signals & Engagement',
                str_starts_with($ruleId, 'CI')        => 'Competitive Intelligence',
                str_starts_with($ruleId, 'CFL'),
                str_starts_with($ruleId, 'CF-')       => 'Content Freshness & Lifecycle',
                str_starts_with($ruleId, 'DDT-LOCAL') => 'Local & Dealer SEO',
                str_starts_with($ruleId, 'MAO')       => 'Media & Asset Optimization',
                str_starts_with($ruleId, 'AIS')       => 'AI Search & Citation Eligibility',
                str_starts_with($ruleId, 'CWV')       => 'Core Web Vitals & Performance',
                str_starts_with($ruleId, 'CTA')       => 'Conversion Path & CTA',
                str_starts_with($ruleId, 'RPT')       => 'Reporting',
                default                               => 'Other',
            };

            try {
                if ($force) {
                    // Upsert
                    $this->db->executeStatement("
                        INSERT INTO seo_rules (rule_id, name, category, tier, trigger_source, trigger_condition, trigger_sql, threshold, diagnosis, action_output, priority, assigned, ai_relevance, full_text, updated_at, updated_by)
                        VALUES (:rule_id, :name, :category, :tier, :trigger_source, :trigger_condition, :trigger_sql, :threshold, :diagnosis, :action_output, :priority, :assigned, :ai_relevance, :full_text, NOW(), 'seed')
                        ON CONFLICT (rule_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            category = EXCLUDED.category,
                            tier = EXCLUDED.tier,
                            trigger_source = EXCLUDED.trigger_source,
                            trigger_condition = EXCLUDED.trigger_condition,
                            trigger_sql = EXCLUDED.trigger_sql,
                            threshold = EXCLUDED.threshold,
                            diagnosis = EXCLUDED.diagnosis,
                            action_output = EXCLUDED.action_output,
                            priority = EXCLUDED.priority,
                            assigned = EXCLUDED.assigned,
                            ai_relevance = EXCLUDED.ai_relevance,
                            full_text = EXCLUDED.full_text,
                            updated_at = NOW(),
                            updated_by = 'seed'
                    ", [
                        'rule_id'           => $ruleId,
                        'name'              => $name,
                        'category'          => $category,
                        'tier'              => $tier,
                        'trigger_source'    => $triggerSource,
                        'trigger_condition' => $triggerCondition,
                        'trigger_sql'       => $triggerSql,
                        'threshold'         => $threshold,
                        'diagnosis'         => $diagnosis,
                        'action_output'     => $actionOutput,
                        'priority'          => $priority,
                        'assigned'          => $assigned,
                        'ai_relevance'      => $aiRelevance,
                        'full_text'         => $ruleText,
                    ]);
                } else {
                    $this->db->insert('seo_rules', [
                        'rule_id'           => $ruleId,
                        'name'              => $name,
                        'category'          => $category,
                        'tier'              => $tier,
                        'trigger_source'    => $triggerSource,
                        'trigger_condition' => $triggerCondition,
                        'trigger_sql'       => $triggerSql,
                        'threshold'         => $threshold,
                        'diagnosis'         => $diagnosis,
                        'action_output'     => $actionOutput,
                        'priority'          => $priority,
                        'assigned'          => $assigned,
                        'ai_relevance'      => $aiRelevance,
                        'full_text'         => $ruleText,
                        'updated_by'        => 'seed',
                    ]);
                }
                $imported++;
            } catch (\Exception $e) {
                $output->writeln("  SKIP {$ruleId}: " . substr($e->getMessage(), 0, 80));
                $skipped++;
            }
        }

        $output->writeln("Imported: {$imported} | Skipped: {$skipped}");
        return Command::SUCCESS;
    }
}

    
