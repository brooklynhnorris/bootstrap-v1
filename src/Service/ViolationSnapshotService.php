<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class ViolationSnapshotService
{
    private ?int $latestSnapshotVersion = null;

    public function __construct(private Connection $db)
    {
    }

    public function getLatestSnapshotVersion(): int
    {
        if ($this->latestSnapshotVersion !== null) {
            return $this->latestSnapshotVersion;
        }

        if (!$this->tableExists('rule_violations')) {
            $this->latestSnapshotVersion = 0;
            return 0;
        }

        $this->latestSnapshotVersion = (int) $this->db->fetchOne('SELECT COALESCE(MAX(snapshot_version), 0) FROM rule_violations');

        return $this->latestSnapshotVersion;
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
        $url = trim(html_entity_decode($url, ENT_QUOTES | ENT_HTML5));
        if ($url === '') {
            return '';
        }

        if (preg_match('#^[a-z][a-z0-9+.-]*://#i', $url) === 1) {
            $parsedPath = parse_url($url, PHP_URL_PATH);
            $url = is_string($parsedPath) && $parsedPath !== '' ? $parsedPath : '/';
        }

        $url = str_replace('\\', '/', $url);
        $url = preg_replace('#[?#].*$#', '', $url) ?? $url;

        if (preg_match('#^//[^/]+(?P<path>/.*)?$#', $url, $matches) === 1) {
            $url = $matches['path'] ?? '/';
        }

        $url = ltrim($url, '/');
        $url = preg_replace('#^(?:https?:/+)?(?:www\.)?doubledtrailers\.com/?#i', '', $url) ?? $url;
        $url = preg_replace('#/+#', '/', $url) ?? $url;

        $normalized = '/' . ltrim($url, '/');
        $normalized = preg_replace('#/+#', '/', $normalized) ?? $normalized;

        return $normalized === '/' ? '/' : rtrim($normalized, '/') . '/';
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
