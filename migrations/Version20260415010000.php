<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260415010000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Move HomeController runtime schema changes into migrations';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('CREATE TABLE IF NOT EXISTS activity_log (
            id SERIAL PRIMARY KEY,
            actor VARCHAR(100) NOT NULL,
            action VARCHAR(50) NOT NULL,
            target_type VARCHAR(50) DEFAULT NULL,
            target_id INT DEFAULT NULL,
            target_title TEXT DEFAULT NULL,
            details TEXT DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_activity_log_created ON activity_log (created_at DESC)');

        $this->addSql('CREATE TABLE IF NOT EXISTS custom_rules (
            id SERIAL PRIMARY KEY,
            rule_id VARCHAR(20) NOT NULL UNIQUE,
            rule_name TEXT NOT NULL,
            category VARCHAR(100) DEFAULT NULL,
            trigger_condition TEXT DEFAULT NULL,
            threshold TEXT DEFAULT NULL,
            diagnosis TEXT DEFAULT NULL,
            action_output TEXT DEFAULT NULL,
            priority VARCHAR(20) DEFAULT \'medium\',
            assigned_to VARCHAR(100) DEFAULT NULL,
            created_by VARCHAR(100) DEFAULT NULL,
            status VARCHAR(20) DEFAULT \'active\',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )');

        $this->addSql('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recheck_days INT DEFAULT NULL');
        $this->addSql('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recheck_criteria TEXT DEFAULT NULL');
        $this->addSql('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS attempt_number INT DEFAULT 1');
        $this->addSql('ALTER TABLE conversations ADD COLUMN IF NOT EXISTS persona_name VARCHAR(50) DEFAULT NULL');
        $this->addSql('ALTER TABLE rule_change_proposals ADD COLUMN IF NOT EXISTS applied_at TIMESTAMP DEFAULT NULL');

        $this->addSql('CREATE TABLE IF NOT EXISTS suppressed_tasks (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            rule_id VARCHAR(20) DEFAULT NULL,
            reason TEXT DEFAULT NULL,
            suppressed_by VARCHAR(100) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(url, rule_id)
        )');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_suppressed_tasks_url ON suppressed_tasks (url)');

        $this->addSql('CREATE TABLE IF NOT EXISTS chat_learnings (
            id SERIAL PRIMARY KEY,
            learning TEXT NOT NULL,
            category VARCHAR(50) DEFAULT \'general\',
            confidence INT DEFAULT 5,
            learned_from VARCHAR(255) DEFAULT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        )');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_chat_learnings_active ON chat_learnings (is_active)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE IF EXISTS chat_learnings');
        $this->addSql('DROP TABLE IF EXISTS suppressed_tasks');
        $this->addSql('DROP TABLE IF EXISTS custom_rules');
        $this->addSql('DROP TABLE IF EXISTS activity_log');
    }
}
