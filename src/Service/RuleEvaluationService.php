<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class RuleEvaluationService
{
    public function __construct(private Connection $db)
    {
    }

    public function evaluateFoundationalContentRules(): array
    {
        if (!$this->tableExists('page_facts') || !$this->tableExists('rule_violations')) {
            return ['snapshot_version' => 0, 'inserted' => 0];
        }

        $snapshotVersion = (int) $this->db->fetchOne('SELECT COALESCE(MAX(snapshot_version), 0) + 1 FROM rule_violations');
        $pages = $this->db->fetchAllAssociative('SELECT * FROM page_facts ORDER BY url');
        $suppressionTable = $this->tableExists('suppressed_tasks');

        $inserted = 0;
        foreach ($pages as $page) {
            $violations = $this->determineViolationsForPage($page);
            foreach ($violations as $violation) {
                $status = 'fail';
                if ($suppressionTable && $this->isSuppressed((string) $page['url'], $violation['rule_id'])) {
                    $status = 'suppressed';
                }

                $this->db->insert('rule_violations', [
                    'rule_id' => $violation['rule_id'],
                    'url' => $page['url'],
                    'status' => $status,
                    'severity' => $violation['severity'],
                    'assignee' => $violation['assignee'],
                    'triage' => $this->determineTriage((int) ($page['target_query_impressions'] ?? 0)),
                    'evidence_json' => json_encode($violation['evidence'], JSON_UNESCAPED_SLASHES),
                    'explanation_short' => $violation['message'],
                    'detected_at' => date('Y-m-d H:i:s'),
                    'snapshot_version' => $snapshotVersion,
                ]);
                $inserted++;
            }
        }

        if ($this->tableExists('data_sources')) {
            $now = date('Y-m-d H:i:s');
            $this->db->executeStatement(
                "INSERT INTO data_sources (source_key, last_fetched_at, last_success_at, last_status, row_count, notes)
                 VALUES ('rule_eval_foundational', :now, :now, 'ok', :row_count, NULL)
                 ON CONFLICT (source_key) DO UPDATE SET
                    last_fetched_at = EXCLUDED.last_fetched_at,
                    last_success_at = EXCLUDED.last_success_at,
                    last_status = EXCLUDED.last_status,
                    row_count = EXCLUDED.row_count,
                    notes = EXCLUDED.notes",
                ['now' => $now, 'row_count' => $inserted]
            );
        }

        return ['snapshot_version' => $snapshotVersion, 'inserted' => $inserted];
    }

    private function determineViolationsForPage(array $page): array
    {
        $violations = [];
        $isIndexable = $this->toBool($page['is_indexable'] ?? true);
        $pageType = strtolower((string) ($page['page_type'] ?? ''));
        $impressions = (int) ($page['target_query_impressions'] ?? 0);
        $hasCoreLink = $this->toBool($page['has_core_link'] ?? false);
        $schemaTypes = json_decode((string) ($page['schema_types'] ?? '[]'), true);
        $h1 = trim((string) ($page['h1'] ?? ''));

        if ($isIndexable && !$this->toBool($page['has_central_entity'] ?? false)) {
            $violations[] = $this->buildViolation(
                'FC-R1',
                'high',
                'Brook',
                'Indexed page is missing the central entity.',
                ['has_central_entity' => $page['has_central_entity'], 'page_type' => $pageType]
            );
        }

        if ($pageType === 'core' && (int) ($page['word_count'] ?? 0) < 500) {
            $violations[] = $this->buildViolation(
                'FC-R3',
                'high',
                'Brook',
                'Core page is below minimum word count.',
                ['word_count' => (int) ($page['word_count'] ?? 0), 'minimum' => 500]
            );
        }

        if ($pageType === 'outer' && !$hasCoreLink && $impressions >= 50) {
            $violations[] = $this->buildViolation(
                'FC-R5',
                'high',
                'Brook',
                'Traffic-bearing outer page is missing a core link.',
                ['has_core_link' => $page['has_core_link'], 'impressions' => $impressions, 'minimum' => 50]
            );
        }

        if ($isIndexable && ($h1 === '' || !$this->toBool($page['h1_matches_title'] ?? false))) {
            $violations[] = $this->buildViolation(
                'FC-R7',
                'high',
                'Brook',
                'Page H1 is missing or does not match the title tag.',
                ['h1' => $page['h1'], 'title_tag' => $page['title_tag'], 'h1_matches_title' => $page['h1_matches_title']]
            );
        }

        if ($pageType === 'core' && (int) ($page['h2_count'] ?? 0) < 1) {
            $violations[] = $this->buildViolation(
                'FC-R8',
                'medium',
                'Brook',
                'Core page is missing H2 headings.',
                ['h2_count' => (int) ($page['h2_count'] ?? 0), 'minimum' => 1]
            );
        }

        if ($pageType === 'core' && (!is_array($schemaTypes) || empty($schemaTypes))) {
            $violations[] = $this->buildViolation(
                'FC-R9',
                'medium',
                'Brad',
                'Core page is missing schema markup.',
                ['schema_types' => $schemaTypes]
            );
        }

        if ($pageType === 'outer' && !$hasCoreLink && $impressions >= 100) {
            $violations[] = $this->buildViolation(
                'FC-R10',
                'high',
                'Brook',
                'High-traffic outer page is missing a core link.',
                ['has_core_link' => $page['has_core_link'], 'impressions' => $impressions, 'minimum' => 100]
            );
        }

        return $violations;
    }

    private function buildViolation(string $ruleId, string $severity, string $assignee, string $message, array $evidence): array
    {
        return [
            'rule_id' => $ruleId,
            'severity' => $severity,
            'assignee' => $assignee,
            'message' => $message,
            'evidence' => $evidence,
        ];
    }

    private function determineTriage(int $impressions): string
    {
        return match (true) {
            $impressions >= 500 => 'high_value',
            $impressions >= 50 => 'optimize',
            $impressions > 0 => 'low_value',
            default => 'strategic_review',
        };
    }

    private function isSuppressed(string $url, string $ruleId): bool
    {
        $count = (int) $this->db->fetchOne(
            "SELECT COUNT(*) FROM suppressed_tasks
             WHERE url = ? AND (rule_id = ? OR rule_id = '__ALL__')",
            [$url, $ruleId]
        );

        return $count > 0;
    }

    private function toBool(mixed $value): bool
    {
        return $value === true || $value === 1 || $value === '1' || $value === 't' || $value === 'true';
    }

    private function tableExists(string $tableName): bool
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
            [$tableName]
        );

        return !empty($tables);
    }
}
