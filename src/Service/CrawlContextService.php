<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class CrawlContextService
{
    public function __construct(
        private Connection $db,
        private ViolationSnapshotService $violationSnapshotService
    )
    {
    }

    public function loadCrawlData(): array
    {
        try {
            $deterministicRows = $this->loadDeterministicCrawlData();
            if (!empty($deterministicRows)) {
                return $deterministicRows;
            }

            if (!$this->tableExists('page_crawl_snapshots')) {
                return [];
            }

            $rows = $this->db->fetchAllAssociative(
                "SELECT p.url, p.page_type, p.has_central_entity, p.has_core_link,
                        p.word_count, p.h1, p.title_tag, p.h1_matches_title, p.h2s,
                        p.schema_types, p.is_noindex, p.internal_link_count,
                        p.image_count, p.has_faq_section, p.has_product_image,
                        p.schema_errors, p.crawled_at,
                        p.target_query, p.target_query_impressions, p.target_query_position, p.target_query_clicks
                 FROM page_crawl_snapshots p
                 WHERE p.crawled_at >= (SELECT MAX(crawled_at) - INTERVAL '1 hour' FROM page_crawl_snapshots)
                   AND (
                     p.has_central_entity = FALSE
                     OR (p.page_type = 'core' AND p.word_count < 500)
                     OR p.h1_matches_title = FALSE
                     OR (p.page_type = 'core' AND (p.h2s IS NULL OR p.h2s = '' OR p.h2s = '[]'))
                     OR (p.page_type = 'core' AND (p.schema_types IS NULL OR p.schema_types = '' OR p.schema_types = '[]'))
                     OR (p.page_type = 'outer' AND p.has_core_link = FALSE)
                     OR (p.schema_errors IS NOT NULL AND p.schema_errors != 'null' AND p.schema_errors != '[]')
                     OR p.internal_link_count > 3
                   )
                 ORDER BY p.page_type, p.url
                 LIMIT 25"
            );

            return $this->applyTriage($rows);
        } catch (\Exception $e) {
            return [];
        }
    }

    public function loadAllCrawledUrls(): array
    {
        try {
            if ($this->tableExists('page_facts')) {
                $rows = $this->db->fetchAllAssociative(
                    "SELECT url, page_type, word_count, CASE WHEN is_indexable = TRUE THEN FALSE ELSE TRUE END AS is_noindex
                     FROM page_facts
                     WHERE is_indexable = TRUE
                     ORDER BY page_type, url"
                );
                if (!empty($rows)) {
                    return $rows;
                }
            }

            if (!$this->tableExists('page_crawl_snapshots')) {
                return [];
            }

            return $this->db->fetchAllAssociative(
                "SELECT url, page_type, word_count, is_noindex
                 FROM page_crawl_snapshots
                 WHERE crawled_at >= (SELECT MAX(crawled_at) - INTERVAL '1 hour' FROM page_crawl_snapshots)
                   AND is_noindex = FALSE
                 ORDER BY page_type, url"
            );
        } catch (\Exception $e) {
            return [];
        }
    }

    public function loadPlayUrlData(string $playUrl): ?array
    {
        try {
            if ($this->tableExists('page_facts')) {
                $row = $this->db->fetchAssociative(
                    "SELECT pf.url, pf.page_type, pf.has_central_entity, pf.has_core_link,
                            pf.word_count, pf.h1, pf.title_tag, pf.h1_matches_title,
                            CASE WHEN pf.h2_count > 0 THEN '[\"present\"]' ELSE '[]' END AS h2s,
                            COALESCE(CAST(pf.schema_types AS TEXT), '[]') AS schema_types,
                            CASE WHEN pf.is_indexable = TRUE THEN FALSE ELSE TRUE END AS is_noindex,
                            pf.internal_link_count,
                            COALESCE(p.image_count, 0) AS image_count,
                            COALESCE(p.has_faq_section, FALSE) AS has_faq_section,
                            COALESCE(p.has_product_image, FALSE) AS has_product_image,
                            COALESCE(CAST(pf.schema_errors AS TEXT), '[]') AS schema_errors,
                            p.meta_description, p.first_sentence_text, p.body_text_snippet,
                            COALESCE(p.images_without_alt, 0) AS images_without_alt,
                            COALESCE(p.images_with_generic_alt, 0) AS images_with_generic_alt,
                            p.image_alt_data,
                            pf.target_query, pf.target_query_impressions, pf.target_query_position, pf.target_query_clicks
                     FROM page_facts pf
                     LEFT JOIN page_crawl_snapshots p
                       ON p.url = pf.url
                      AND p.crawled_at = (
                            SELECT MAX(p2.crawled_at)
                            FROM page_crawl_snapshots p2
                            WHERE p2.url = pf.url
                        )
                     WHERE pf.url = ?",
                    [$playUrl]
                );
                if ($row !== false) {
                    return $row;
                }
            }

            if (!$this->tableExists('page_crawl_snapshots')) {
                return null;
            }

            $row = $this->db->fetchAssociative(
                "SELECT url, page_type, has_central_entity, has_core_link,
                        word_count, h1, title_tag, h1_matches_title, h2s,
                        schema_types, is_noindex, internal_link_count,
                        image_count, has_faq_section, has_product_image,
                        schema_errors, meta_description, first_sentence_text,
                        body_text_snippet, images_without_alt, images_with_generic_alt,
                        image_alt_data,
                        target_query, target_query_impressions, target_query_position, target_query_clicks
                 FROM page_crawl_snapshots
                 WHERE url = ?
                 ORDER BY crawled_at DESC
                 LIMIT 1",
                [$playUrl]
            );

            return $row !== false ? $row : null;
        } catch (\Exception $e) {
            return null;
        }
    }

    private function loadDeterministicCrawlData(): array
    {
        try {
            if (!$this->tableExists('page_facts') || !$this->tableExists('rule_violations')) {
                return [];
            }

            $snapshotVersion = $this->violationSnapshotService->getLatestSnapshotVersion();
            if ($snapshotVersion <= 0) {
                return [];
            }

            $rows = $this->db->fetchAllAssociative(
                "SELECT pf.url, pf.page_type, pf.has_central_entity, pf.has_core_link,
                        pf.word_count, pf.h1, pf.title_tag, pf.h1_matches_title,
                        CASE WHEN pf.h2_count > 0 THEN '[\"present\"]' ELSE '[]' END AS h2s,
                        COALESCE(CAST(pf.schema_types AS TEXT), '[]') AS schema_types,
                        CASE WHEN pf.is_indexable = TRUE THEN FALSE ELSE TRUE END AS is_noindex,
                        pf.internal_link_count,
                        0 AS image_count,
                        FALSE AS has_faq_section,
                        FALSE AS has_product_image,
                        COALESCE(CAST(pf.schema_errors AS TEXT), '[]') AS schema_errors,
                        pf.last_crawled_at AS crawled_at,
                        pf.target_query, pf.target_query_impressions, pf.target_query_position, pf.target_query_clicks,
                        MAX(rv.triage) AS triage,
                        STRING_AGG(rv.rule_id, ',' ORDER BY rv.rule_id) AS rule_ids
                 FROM page_facts pf
                 INNER JOIN rule_violations rv
                    ON rv.url = pf.url
                   AND rv.snapshot_version = :snapshot_version
                   AND rv.status IN ('fail', 'suppressed')
                 GROUP BY pf.url, pf.page_type, pf.has_central_entity, pf.has_core_link,
                        pf.word_count, pf.h1, pf.title_tag, pf.h1_matches_title, pf.h2_count,
                        pf.schema_types, pf.is_indexable, pf.internal_link_count, pf.schema_errors,
                        pf.last_crawled_at, pf.target_query, pf.target_query_impressions,
                        pf.target_query_position, pf.target_query_clicks
                 ORDER BY COALESCE(pf.target_query_impressions, 0) DESC, pf.url
                 LIMIT 25",
                ['snapshot_version' => $snapshotVersion]
            );

            return $this->applyTriage($rows);
        } catch (\Exception $e) {
            return [];
        }
    }

    private function applyTriage(array $rows): array
    {
        foreach ($rows as &$row) {
            $impressions = (int) ($row['target_query_impressions'] ?? 0);
            $row['page_impressions'] = $impressions;
            $row['page_clicks'] = (int) ($row['target_query_clicks'] ?? 0);

            if (!empty($row['triage'])) {
                continue;
            }

            if ($impressions >= 500) {
                $row['triage'] = 'high_value';
            } elseif ($impressions >= 50) {
                $row['triage'] = 'optimize';
            } elseif ($impressions > 0) {
                $row['triage'] = 'low_value';
            } else {
                $row['triage'] = 'strategic_review';
            }
        }
        unset($row);

        return $rows;
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
