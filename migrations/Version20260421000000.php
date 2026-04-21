<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260421000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add structured task rejection memory for evaluator suppression and learning';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('CREATE TABLE IF NOT EXISTS task_rejections (
            id SERIAL PRIMARY KEY,
            task_id INT NOT NULL,
            rule_id VARCHAR(20) DEFAULT NULL,
            url TEXT DEFAULT NULL,
            page_type VARCHAR(50) DEFAULT NULL,
            target_query TEXT DEFAULT NULL,
            reason_code VARCHAR(50) NOT NULL,
            reason_text TEXT DEFAULT NULL,
            scope VARCHAR(50) NOT NULL DEFAULT \'task_only\',
            created_by VARCHAR(100) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_task_rejections_rule_created ON task_rejections (rule_id, created_at DESC)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_task_rejections_url_created ON task_rejections (url, created_at DESC)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_task_rejections_scope_created ON task_rejections (scope, created_at DESC)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_task_rejections_rule_page_reason ON task_rejections (rule_id, page_type, reason_code, created_at DESC)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE IF EXISTS task_rejections');
    }
}
