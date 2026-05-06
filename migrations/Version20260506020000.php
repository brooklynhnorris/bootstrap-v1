<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260506020000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Extend tasks with idempotency, lifecycle, source violation, and run lineage fields.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(64) DEFAULT NULL');
        $this->addSql('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS source_violation_id INT DEFAULT NULL');
        $this->addSql('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP DEFAULT NULL');
        $this->addSql('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS lifecycle_state VARCHAR(20) NOT NULL DEFAULT \'active\'');
        $this->addSql('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS run_id INT DEFAULT NULL');

        $this->addSql('CREATE UNIQUE INDEX IF NOT EXISTS uniq_tasks_idempotency_key ON tasks (idempotency_key) WHERE idempotency_key IS NOT NULL');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_tasks_source_violation ON tasks (source_violation_id)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_tasks_lifecycle_state ON tasks (lifecycle_state)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_tasks_last_seen ON tasks (last_seen_at DESC)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP INDEX IF EXISTS idx_tasks_last_seen');
        $this->addSql('DROP INDEX IF EXISTS idx_tasks_lifecycle_state');
        $this->addSql('DROP INDEX IF EXISTS idx_tasks_source_violation');
        $this->addSql('DROP INDEX IF EXISTS uniq_tasks_idempotency_key');
        $this->addSql('ALTER TABLE tasks DROP COLUMN IF EXISTS run_id');
        $this->addSql('ALTER TABLE tasks DROP COLUMN IF EXISTS lifecycle_state');
        $this->addSql('ALTER TABLE tasks DROP COLUMN IF EXISTS last_seen_at');
        $this->addSql('ALTER TABLE tasks DROP COLUMN IF EXISTS source_violation_id');
        $this->addSql('ALTER TABLE tasks DROP COLUMN IF EXISTS idempotency_key');
    }
}

