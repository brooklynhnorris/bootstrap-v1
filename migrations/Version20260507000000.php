<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260507000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Week 2 PR #2: add seo_rules.action_family and seo_rules.business_multiplier with deterministic category backfill.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql("ALTER TABLE seo_rules ADD COLUMN IF NOT EXISTS action_family VARCHAR(50) DEFAULT NULL");
        $this->addSql("ALTER TABLE seo_rules ADD COLUMN IF NOT EXISTS business_multiplier NUMERIC(6,3) NOT NULL DEFAULT 1.000");

        $this->addSql("
            UPDATE seo_rules
            SET action_family = CASE
                WHEN category IS NULL OR BTRIM(category) = '' THEN 'general_fix'
                WHEN LOWER(category) = LOWER('Technical SEO') THEN 'technical_fix'
                WHEN LOWER(category) = LOWER('Core Web Vitals & Performance') THEN 'performance_fix'
                WHEN LOWER(category) = LOWER('Schema & Structured Data') THEN 'schema_impl'
                WHEN LOWER(category) = LOWER('On-Page Content Quality') THEN 'content_expand'
                WHEN LOWER(category) = LOWER('Entity & Topical Authority') THEN 'content_expand'
                WHEN LOWER(category) = LOWER('Content Freshness & Lifecycle') THEN 'content_expand'
                WHEN LOWER(category) = LOWER('AI Search & Citation Eligibility') THEN 'content_expand'
                WHEN LOWER(category) = LOWER('Media & Asset Optimization') THEN 'image_fix'
                WHEN LOWER(category) = LOWER('Keyword & Intent Alignment') THEN 'metadata_fix'
                WHEN LOWER(category) = LOWER('Internal Link Architecture') THEN 'link_add'
                WHEN LOWER(category) = LOWER('User Signals & Engagement') THEN 'ux_conversion'
                WHEN LOWER(category) = LOWER('Conversion Path & CTA') THEN 'ux_conversion'
                WHEN LOWER(category) = LOWER('E-E-A-T & Trust Signals') THEN 'trust_signal_add'
                WHEN LOWER(category) = LOWER('Local & Dealer SEO') THEN 'local_optimization'
                WHEN LOWER(category) = LOWER('Competitive Intelligence') THEN 'competitive_research'
                WHEN LOWER(category) = LOWER('Reporting') THEN 'reporting'
                ELSE 'general_fix'
            END
            WHERE action_family IS NULL OR BTRIM(action_family) = ''
        ");

        $this->addSql("
            UPDATE seo_rules
            SET business_multiplier = CASE
                WHEN business_multiplier IS NULL THEN 1.000
                ELSE business_multiplier
            END
        ");

        $this->addSql("CREATE INDEX IF NOT EXISTS idx_seo_rules_action_family ON seo_rules (action_family)");
    }

    public function down(Schema $schema): void
    {
        $this->addSql("DROP INDEX IF EXISTS idx_seo_rules_action_family");
        $this->addSql("ALTER TABLE seo_rules DROP COLUMN IF EXISTS business_multiplier");
        $this->addSql("ALTER TABLE seo_rules DROP COLUMN IF EXISTS action_family");
    }
}
