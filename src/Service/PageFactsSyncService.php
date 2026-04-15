<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class PageFactsSyncService
{
    public function __construct(private Connection $db)
    {
    }

    public function sync(): int
    {
        if (!$this->tableExists('page_crawl_snapshots') || !$this->tableExists('page_facts')) {
            return 0;
        }

        $rows = $this->db->fetchAllAssociative(
            "SELECT p.url, p.page_type, p.is_noindex, p.word_count, p.h1, p.title_tag,
                    p.h1_matches_title, p.h2s, p.schema_types, p.schema_errors,
                    p.has_central_entity, p.has_core_link, p.internal_link_count,
                    p.target_query, p.target_query_impressions, p.target_query_clicks,
                    p.target_query_position, p.crawled_at
             FROM page_crawl_snapshots p
             INNER JOIN (
                SELECT url, MAX(crawled_at) AS max_crawled_at
                FROM page_crawl_snapshots
                GROUP BY url
             ) latest
                ON latest.url = p.url
               AND latest.max_crawled_at = p.crawled_at
             ORDER BY p.url"
        );

        if (empty($rows)) {
            $this->recordDataSource('crawl', 0, 'empty', 'No page crawl snapshots found to sync.');
            return 0;
        }

        $ga4Lookup = $this->loadGa4Lookup();
        $now = date('Y-m-d H:i:s');

        foreach ($rows as $row) {
            $url = (string) ($row['url'] ?? '');
            if ($url === '') {
                continue;
            }

            $ga4 = $ga4Lookup[$url] ?? null;
            $schemaTypes = $this->normalizeJsonValue($row['schema_types'] ?? null, '[]');
            $schemaErrors = $this->normalizeJsonValue($row['schema_errors'] ?? null, '[]');

            $this->db->executeStatement(
                "INSERT INTO page_facts (
                    url, page_type, page_subtype, is_indexable, word_count, h1, title_tag,
                    h1_matches_title, h2_count, schema_types, schema_errors,
                    has_central_entity, has_core_link, internal_link_count,
                    target_query, target_query_impressions, target_query_clicks, target_query_position,
                    sessions_28d, pageviews_28d, conversions_28d, bounce_rate_28d, avg_engagement_time_28d,
                    last_crawled_at, last_ga4_at, updated_at
                ) VALUES (
                    :url, :page_type, NULL, :is_indexable, :word_count, :h1, :title_tag,
                    :h1_matches_title, :h2_count, CAST(:schema_types AS JSONB), CAST(:schema_errors AS JSONB),
                    :has_central_entity, :has_core_link, :internal_link_count,
                    :target_query, :target_query_impressions, :target_query_clicks, :target_query_position,
                    :sessions_28d, :pageviews_28d, :conversions_28d, :bounce_rate_28d, :avg_engagement_time_28d,
                    :last_crawled_at, :last_ga4_at, :updated_at
                )
                ON CONFLICT (url) DO UPDATE SET
                    page_type = EXCLUDED.page_type,
                    is_indexable = EXCLUDED.is_indexable,
                    word_count = EXCLUDED.word_count,
                    h1 = EXCLUDED.h1,
                    title_tag = EXCLUDED.title_tag,
                    h1_matches_title = EXCLUDED.h1_matches_title,
                    h2_count = EXCLUDED.h2_count,
                    schema_types = EXCLUDED.schema_types,
                    schema_errors = EXCLUDED.schema_errors,
                    has_central_entity = EXCLUDED.has_central_entity,
                    has_core_link = EXCLUDED.has_core_link,
                    internal_link_count = EXCLUDED.internal_link_count,
                    target_query = EXCLUDED.target_query,
                    target_query_impressions = EXCLUDED.target_query_impressions,
                    target_query_clicks = EXCLUDED.target_query_clicks,
                    target_query_position = EXCLUDED.target_query_position,
                    sessions_28d = EXCLUDED.sessions_28d,
                    pageviews_28d = EXCLUDED.pageviews_28d,
                    conversions_28d = EXCLUDED.conversions_28d,
                    bounce_rate_28d = EXCLUDED.bounce_rate_28d,
                    avg_engagement_time_28d = EXCLUDED.avg_engagement_time_28d,
                    last_crawled_at = EXCLUDED.last_crawled_at,
                    last_ga4_at = EXCLUDED.last_ga4_at,
                    updated_at = EXCLUDED.updated_at",
                [
                    'url' => $url,
                    'page_type' => $row['page_type'] ?? null,
                    'is_indexable' => !$this->toBool($row['is_noindex'] ?? false),
                    'word_count' => $row['word_count'] !== null ? (int) $row['word_count'] : null,
                    'h1' => $row['h1'] ?? null,
                    'title_tag' => $row['title_tag'] ?? null,
                    'h1_matches_title' => $row['h1_matches_title'] !== null ? $this->toBool($row['h1_matches_title']) : null,
                    'h2_count' => $this->countHeadings($row['h2s'] ?? null),
                    'schema_types' => $schemaTypes,
                    'schema_errors' => $schemaErrors,
                    'has_central_entity' => $row['has_central_entity'] !== null ? $this->toBool($row['has_central_entity']) : null,
                    'has_core_link' => $row['has_core_link'] !== null ? $this->toBool($row['has_core_link']) : null,
                    'internal_link_count' => $row['internal_link_count'] !== null ? (int) $row['internal_link_count'] : null,
                    'target_query' => $row['target_query'] ?? null,
                    'target_query_impressions' => $row['target_query_impressions'] !== null ? (int) $row['target_query_impressions'] : null,
                    'target_query_clicks' => $row['target_query_clicks'] !== null ? (int) $row['target_query_clicks'] : null,
                    'target_query_position' => $row['target_query_position'] !== null ? (float) $row['target_query_position'] : null,
                    'sessions_28d' => $ga4['sessions'] ?? null,
                    'pageviews_28d' => $ga4['pageviews'] ?? null,
                    'conversions_28d' => $ga4['conversions'] ?? null,
                    'bounce_rate_28d' => $ga4['bounce_rate'] ?? null,
                    'avg_engagement_time_28d' => $ga4['avg_engagement_time'] ?? null,
                    'last_crawled_at' => $row['crawled_at'] ?? null,
                    'last_ga4_at' => $ga4['fetched_at'] ?? null,
                    'updated_at' => $now,
                ]
            );
        }

        $this->recordDataSource('crawl', count($rows), 'ok');
        if (!empty($ga4Lookup)) {
            $this->recordDataSource('ga4', count($ga4Lookup), 'ok');
        }

        return count($rows);
    }

    private function loadGa4Lookup(): array
    {
        if (!$this->tableExists('ga4_snapshots')) {
            return [];
        }

        $rows = $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, pageviews, conversions, bounce_rate, avg_engagement_time, fetched_at
             FROM ga4_snapshots
             WHERE date_range = '28d'"
        );

        $lookup = [];
        foreach ($rows as $row) {
            $lookup[$this->normalizePath((string) ($row['page_path'] ?? ''))] = [
                'sessions' => (int) ($row['sessions'] ?? 0),
                'pageviews' => (int) ($row['pageviews'] ?? 0),
                'conversions' => (int) ($row['conversions'] ?? 0),
                'bounce_rate' => $row['bounce_rate'] !== null ? (float) $row['bounce_rate'] : null,
                'avg_engagement_time' => $row['avg_engagement_time'] !== null ? (float) $row['avg_engagement_time'] : null,
                'fetched_at' => $row['fetched_at'] ?? null,
            ];
        }

        return $lookup;
    }

    private function normalizePath(string $path): string
    {
        $trimmed = '/' . trim(parse_url($path, PHP_URL_PATH) ?? $path, '/');
        return $trimmed === '/' ? '/' : $trimmed . '/';
    }

    private function countHeadings(mixed $rawValue): int
    {
        if ($rawValue === null || $rawValue === '' || $rawValue === '[]') {
            return 0;
        }

        $decoded = json_decode((string) $rawValue, true);
        if (is_array($decoded)) {
            return count(array_filter($decoded, static fn($value) => trim((string) $value) !== ''));
        }

        return count(array_filter(array_map('trim', preg_split('/\r\n|\r|\n/', (string) $rawValue) ?: [])));
    }

    private function normalizeJsonValue(mixed $value, string $default): string
    {
        if ($value === null || $value === '' || $value === 'null') {
            return $default;
        }

        json_decode((string) $value, true);
        return json_last_error() === JSON_ERROR_NONE ? (string) $value : $default;
    }

    private function recordDataSource(string $sourceKey, int $rowCount, string $status, ?string $notes = null): void
    {
        if (!$this->tableExists('data_sources')) {
            return;
        }

        $now = date('Y-m-d H:i:s');
        $this->db->executeStatement(
            "INSERT INTO data_sources (source_key, last_fetched_at, last_success_at, last_status, row_count, notes)
             VALUES (:source_key, :last_fetched_at, :last_success_at, :last_status, :row_count, :notes)
             ON CONFLICT (source_key) DO UPDATE SET
                last_fetched_at = EXCLUDED.last_fetched_at,
                last_success_at = EXCLUDED.last_success_at,
                last_status = EXCLUDED.last_status,
                row_count = EXCLUDED.row_count,
                notes = EXCLUDED.notes",
            [
                'source_key' => $sourceKey,
                'last_fetched_at' => $now,
                'last_success_at' => $status === 'ok' ? $now : null,
                'last_status' => $status,
                'row_count' => $rowCount,
                'notes' => $notes,
            ]
        );
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
