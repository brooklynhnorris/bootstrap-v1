<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260507010000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Week 2 PR #3: add AVR score columns to rule_violations and tasks.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql("ALTER TABLE rule_violations ADD COLUMN IF NOT EXISTS avr_score INT DEFAULT NULL");
        $this->addSql("ALTER TABLE rule_violations ADD COLUMN IF NOT EXISTS avr_breakdown_json JSONB DEFAULT NULL");
        $this->addSql("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS avr_score INT DEFAULT NULL");

        $this->addSql("CREATE INDEX IF NOT EXISTS idx_rule_violations_avr_score ON rule_violations (avr_score)");
        $this->addSql("CREATE INDEX IF NOT EXISTS idx_tasks_avr_score ON tasks (avr_score)");
    }

    public function down(Schema $schema): void
    {
        $this->addSql("DROP INDEX IF EXISTS idx_tasks_avr_score");
        $this->addSql("DROP INDEX IF EXISTS idx_rule_violations_avr_score");
        $this->addSql("ALTER TABLE tasks DROP COLUMN IF EXISTS avr_score");
        $this->addSql("ALTER TABLE rule_violations DROP COLUMN IF EXISTS avr_breakdown_json");
        $this->addSql("ALTER TABLE rule_violations DROP COLUMN IF EXISTS avr_score");
    }
}

