<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260506000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add rule_runs table for per-run lineage of rule evaluation pipeline.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('
            CREATE TABLE IF NOT EXISTS rule_runs (
                id SERIAL PRIMARY KEY,
                started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                ended_at TIMESTAMP DEFAULT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'running\',
                triggered_by VARCHAR(50) NOT NULL DEFAULT \'cron\',
                rules_attempted INT DEFAULT 0,
                rules_succeeded INT DEFAULT 0,
                rules_failed INT DEFAULT 0,
                violations_recorded INT DEFAULT 0,
                tasks_promoted INT DEFAULT 0,
                tasks_suppressed INT DEFAULT 0,
                summary_json JSONB DEFAULT NULL,
                notes TEXT DEFAULT NULL
            )
        ');

        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_runs_started ON rule_runs (started_at DESC)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_runs_status ON rule_runs (status)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE IF EXISTS rule_runs');
    }
}

