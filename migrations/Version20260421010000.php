<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260421010000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add reason-aware guardrail code to task rejections';
    }

    public function up(Schema $schema): void
    {
        $this->addSql("ALTER TABLE task_rejections ADD COLUMN IF NOT EXISTS guardrail_code VARCHAR(80) DEFAULT NULL");
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_task_rejections_guardrail ON task_rejections (guardrail_code, created_at DESC)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('ALTER TABLE task_rejections DROP COLUMN IF EXISTS guardrail_code');
    }
}
