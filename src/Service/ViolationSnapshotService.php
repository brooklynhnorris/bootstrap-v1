<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class ViolationSnapshotService
{
    public function __construct(private Connection $db)
    {
    }

    public function getLatestSnapshotVersion(): int
    {
        if (!$this->tableExists('rule_violations')) {
            return 0;
        }

        return (int) $this->db->fetchOne('SELECT COALESCE(MAX(snapshot_version), 0) FROM rule_violations');
    }

    public function findActiveViolation(string $url, string $ruleId): ?array
    {
        $snapshotVersion = $this->getLatestSnapshotVersion();
        if ($snapshotVersion <= 0) {
            return null;
        }

        $violation = $this->db->fetchAssociative(
            "SELECT url, rule_id, severity, assignee, triage, explanation_short
             FROM rule_violations
             WHERE snapshot_version = ?
               AND rule_id = ?
               AND url = ?
               AND status = 'fail'
             LIMIT 1",
            [$snapshotVersion, strtoupper(trim($ruleId)), $this->normalizeUrl($url)]
        );

        return $violation !== false ? $violation : null;
    }

    public function normalizeUrl(string $url): string
    {
        $normalized = '/' . trim($url, '/');
        return $normalized === '/' ? '/' : $normalized . '/';
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
