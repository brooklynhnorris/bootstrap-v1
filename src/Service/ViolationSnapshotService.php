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
        $url = $this->stripQueryAndFragment($url);

        if (preg_match('#^//[^/]+(?P<path>/.*)?$#', $url, $matches) === 1) {
            $url = $matches['path'] ?? '/';
        }

        $url = ltrim($url, '/');
        $lowerUrl = strtolower($url);
        if (str_starts_with($lowerUrl, 'http://doubledtrailers.com/')) {
            $url = substr($url, strlen('http://doubledtrailers.com/'));
        } elseif (str_starts_with($lowerUrl, 'https://doubledtrailers.com/')) {
            $url = substr($url, strlen('https://doubledtrailers.com/'));
        } elseif (str_starts_with($lowerUrl, 'http://www.doubledtrailers.com/')) {
            $url = substr($url, strlen('http://www.doubledtrailers.com/'));
        } elseif (str_starts_with($lowerUrl, 'https://www.doubledtrailers.com/')) {
            $url = substr($url, strlen('https://www.doubledtrailers.com/'));
        } elseif ($lowerUrl === 'doubledtrailers.com' || $lowerUrl === 'www.doubledtrailers.com') {
            $url = '';
        } elseif (str_starts_with($lowerUrl, 'doubledtrailers.com/')) {
            $url = substr($url, strlen('doubledtrailers.com/'));
        } elseif (str_starts_with($lowerUrl, 'www.doubledtrailers.com/')) {
            $url = substr($url, strlen('www.doubledtrailers.com/'));
        }

        $url = $this->collapseSlashes($url);

        $normalized = '/' . ltrim($url, '/');
        $normalized = $this->collapseSlashes($normalized);

        return $normalized === '/' ? '/' : rtrim($normalized, '/') . '/';
    }

    private function stripQueryAndFragment(string $url): string
    {
        $questionPos = strpos($url, '?');
        $hashPos = strpos($url, '#');

        $cutPos = false;
        if ($questionPos !== false && $hashPos !== false) {
            $cutPos = min($questionPos, $hashPos);
        } elseif ($questionPos !== false) {
            $cutPos = $questionPos;
        } elseif ($hashPos !== false) {
            $cutPos = $hashPos;
        }

        return $cutPos === false ? $url : substr($url, 0, $cutPos);
    }

    private function collapseSlashes(string $url): string
    {
        while (str_contains($url, '//')) {
            $url = str_replace('//', '/', $url);
        }

        return $url;
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
