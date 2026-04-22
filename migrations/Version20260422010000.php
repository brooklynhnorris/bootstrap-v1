<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260422010000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add GSC comparison-window indexes and historical Core Web Vitals logging';
    }

    public function up(Schema $schema): void
    {
        $this->addSql("CREATE INDEX IF NOT EXISTS idx_gsc_date_query_page ON gsc_snapshots (date_range, query, page)");
        $this->addSql("CREATE INDEX IF NOT EXISTS idx_gsc_date_page ON gsc_snapshots (date_range, page)");

        $this->addSql("ALTER TABLE core_web_vitals ADD COLUMN IF NOT EXISTS inp_ms INT DEFAULT NULL");
        $this->addSql("ALTER TABLE core_web_vitals ADD COLUMN IF NOT EXISTS lcp_seconds DECIMAL(8,3) DEFAULT NULL");

        $this->addSql('CREATE TABLE IF NOT EXISTS core_web_vitals_log (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            strategy VARCHAR(10) NOT NULL DEFAULT \'mobile\',
            lcp_seconds DECIMAL(8,3) DEFAULT NULL,
            cls_score DECIMAL(6,4) DEFAULT NULL,
            inp_milliseconds INT DEFAULT NULL,
            ttfb_ms INT DEFAULT NULL,
            performance_score INT DEFAULT NULL,
            lcp_ms INT DEFAULT NULL,
            field_lcp_ms INT DEFAULT NULL,
            field_inp_ms INT DEFAULT NULL,
            field_ttfb_ms INT DEFAULT NULL,
            source_type VARCHAR(20) DEFAULT NULL,
            captured_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )');

        $this->addSql("CREATE INDEX IF NOT EXISTS idx_cwv_log_url_strategy ON core_web_vitals_log (url, strategy, captured_at DESC)");
        $this->addSql("CREATE INDEX IF NOT EXISTS idx_cwv_log_captured_at ON core_web_vitals_log (captured_at DESC)");
    }

    public function down(Schema $schema): void
    {
        $this->addSql("DROP INDEX IF EXISTS idx_gsc_date_query_page");
        $this->addSql("DROP INDEX IF EXISTS idx_gsc_date_page");
        $this->addSql("DROP INDEX IF EXISTS idx_cwv_log_url_strategy");
        $this->addSql("DROP INDEX IF EXISTS idx_cwv_log_captured_at");
        $this->addSql("DROP TABLE IF EXISTS core_web_vitals_log");
        $this->addSql("ALTER TABLE core_web_vitals DROP COLUMN IF EXISTS inp_ms");
        $this->addSql("ALTER TABLE core_web_vitals DROP COLUMN IF EXISTS lcp_seconds");
    }
}
