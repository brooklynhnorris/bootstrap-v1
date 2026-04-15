<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260415000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add deterministic signals and rule evaluation tables';
    }

    public function up(Schema $schema): void
    {
        $this->addSql("
            CREATE TABLE IF NOT EXISTS data_sources (
                source_key VARCHAR(50) PRIMARY KEY,
                last_fetched_at TIMESTAMP DEFAULT NULL,
                last_success_at TIMESTAMP DEFAULT NULL,
                last_status VARCHAR(20) NOT NULL DEFAULT 'unknown',
                row_count INT NOT NULL DEFAULT 0,
                notes TEXT DEFAULT NULL
            )
        ");

        $this->addSql("
            CREATE TABLE IF NOT EXISTS page_facts (
                url TEXT PRIMARY KEY,
                page_type VARCHAR(20) DEFAULT NULL,
                page_subtype VARCHAR(30) DEFAULT NULL,
                is_indexable BOOLEAN NOT NULL DEFAULT TRUE,
                word_count INT DEFAULT NULL,
                h1 TEXT DEFAULT NULL,
                title_tag TEXT DEFAULT NULL,
                h1_matches_title BOOLEAN DEFAULT NULL,
                h2_count INT NOT NULL DEFAULT 0,
                schema_types JSONB DEFAULT NULL,
                schema_errors JSONB DEFAULT NULL,
                has_central_entity BOOLEAN DEFAULT NULL,
                has_core_link BOOLEAN DEFAULT NULL,
                internal_link_count INT DEFAULT NULL,
                target_query TEXT DEFAULT NULL,
                target_query_impressions INT DEFAULT NULL,
                target_query_clicks INT DEFAULT NULL,
                target_query_position NUMERIC(8,2) DEFAULT NULL,
                sessions_28d INT DEFAULT NULL,
                pageviews_28d INT DEFAULT NULL,
                conversions_28d INT DEFAULT NULL,
                bounce_rate_28d NUMERIC(8,4) DEFAULT NULL,
                avg_engagement_time_28d NUMERIC(10,2) DEFAULT NULL,
                last_crawled_at TIMESTAMP DEFAULT NULL,
                last_ga4_at TIMESTAMP DEFAULT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ");
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_page_facts_type ON page_facts (page_type)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_page_facts_indexable ON page_facts (is_indexable)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_page_facts_impressions ON page_facts (target_query_impressions)');

        $this->addSql("
            CREATE TABLE IF NOT EXISTS rule_violations (
                id BIGSERIAL PRIMARY KEY,
                rule_id VARCHAR(30) NOT NULL,
                url TEXT NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'fail',
                severity VARCHAR(20) NOT NULL DEFAULT 'medium',
                assignee VARCHAR(50) DEFAULT NULL,
                triage VARCHAR(30) DEFAULT NULL,
                evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                explanation_short TEXT DEFAULT NULL,
                detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                snapshot_version BIGINT NOT NULL DEFAULT 1
            )
        ");
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_violations_rule ON rule_violations (rule_id)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_violations_url ON rule_violations (url)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_violations_status ON rule_violations (status)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_violations_snapshot ON rule_violations (snapshot_version)');
        $this->addSql('CREATE UNIQUE INDEX IF NOT EXISTS uniq_rule_violations_snapshot_rule_url ON rule_violations (snapshot_version, rule_id, url)');

        $this->addSql("
            CREATE TABLE IF NOT EXISTS action_requests (
                id BIGSERIAL PRIMARY KEY,
                action_type VARCHAR(50) NOT NULL,
                target_type VARCHAR(50) NOT NULL,
                target_id TEXT DEFAULT NULL,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                requested_by VARCHAR(20) NOT NULL DEFAULT 'llm',
                approval_status VARCHAR(20) NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                approved_at TIMESTAMP DEFAULT NULL,
                executed_at TIMESTAMP DEFAULT NULL
            )
        ");
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_action_requests_status ON action_requests (approval_status)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE IF EXISTS action_requests');
        $this->addSql('DROP TABLE IF EXISTS rule_violations');
        $this->addSql('DROP TABLE IF EXISTS page_facts');
        $this->addSql('DROP TABLE IF EXISTS data_sources');
    }
}
