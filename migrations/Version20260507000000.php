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
                WHEN LOWER(category) IN ('technical', 'tech', 'cwv', 'schema', 'structured_data') THEN 'technical_fix'
                WHEN LOWER(category) IN ('content', 'eta', 'ais', 'opq', 'mao') THEN 'content_update'
                WHEN LOWER(category) IN ('internal_linking', 'ila', 'linking') THEN 'internal_linking'
                WHEN LOWER(category) IN ('keyword', 'kia', 'query') THEN 'keyword_alignment'
                WHEN LOWER(category) IN ('ux', 'use', 'conversion', 'cta') THEN 'ux_conversion'
                WHEN LOWER(category) IN ('local', 'ddt-local') THEN 'local_optimization'
                WHEN LOWER(category) IN ('compliance', 'policy', 'trust', 'eeat', 'ddt-eeat') THEN 'trust_compliance'
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
        $this->addSql("CREATE INDEX IF NOT EXISTS idx_seo_rules_business_multiplier ON seo_rules (business_multiplier)");
    }

    public function down(Schema $schema): void
    {
        $this->addSql("DROP INDEX IF EXISTS idx_seo_rules_business_multiplier");
        $this->addSql("DROP INDEX IF EXISTS idx_seo_rules_action_family");
        $this->addSql("ALTER TABLE seo_rules DROP COLUMN IF EXISTS business_multiplier");
        $this->addSql("ALTER TABLE seo_rules DROP COLUMN IF EXISTS action_family");
    }
}

