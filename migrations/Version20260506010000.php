<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260506010000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Extend rule_violations with per-run lineage and suppression decision tracking.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('ALTER TABLE rule_violations ADD COLUMN IF NOT EXISTS run_id INT DEFAULT NULL');
        $this->addSql('ALTER TABLE rule_violations ADD COLUMN IF NOT EXISTS decision VARCHAR(20) DEFAULT \'pending\'');
        $this->addSql('ALTER TABLE rule_violations ADD COLUMN IF NOT EXISTS suppression_reason_code VARCHAR(50) DEFAULT NULL');
        $this->addSql('ALTER TABLE rule_violations ADD COLUMN IF NOT EXISTS suppression_reason_text TEXT DEFAULT NULL');
        $this->addSql('ALTER TABLE rule_violations ADD COLUMN IF NOT EXISTS candidate_hash VARCHAR(64) DEFAULT NULL');
        $this->addSql('ALTER TABLE rule_violations ADD COLUMN IF NOT EXISTS action_family VARCHAR(50) DEFAULT NULL');

        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_violations_run_id ON rule_violations (run_id)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_violations_decision ON rule_violations (decision)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_violations_candidate_hash ON rule_violations (candidate_hash)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP INDEX IF EXISTS idx_rule_violations_candidate_hash');
        $this->addSql('DROP INDEX IF EXISTS idx_rule_violations_decision');
        $this->addSql('DROP INDEX IF EXISTS idx_rule_violations_run_id');
        $this->addSql('ALTER TABLE rule_violations DROP COLUMN IF EXISTS action_family');
        $this->addSql('ALTER TABLE rule_violations DROP COLUMN IF EXISTS candidate_hash');
        $this->addSql('ALTER TABLE rule_violations DROP COLUMN IF EXISTS suppression_reason_text');
        $this->addSql('ALTER TABLE rule_violations DROP COLUMN IF EXISTS suppression_reason_code');
        $this->addSql('ALTER TABLE rule_violations DROP COLUMN IF EXISTS decision');
        $this->addSql('ALTER TABLE rule_violations DROP COLUMN IF EXISTS run_id');
    }
}

