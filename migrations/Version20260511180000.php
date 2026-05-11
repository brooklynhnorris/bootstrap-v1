<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260511180000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add canonical_url and canonical_resolved_at columns to page_facts for redirect canonicalization.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('ALTER TABLE page_facts ADD COLUMN IF NOT EXISTS canonical_url TEXT');
        $this->addSql('ALTER TABLE page_facts ADD COLUMN IF NOT EXISTS canonical_resolved_at TIMESTAMP');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_page_facts_canonical_url ON page_facts (canonical_url) WHERE canonical_url IS NOT NULL');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP INDEX IF EXISTS idx_page_facts_canonical_url');
        $this->addSql('ALTER TABLE page_facts DROP COLUMN IF EXISTS canonical_url');
        $this->addSql('ALTER TABLE page_facts DROP COLUMN IF EXISTS canonical_resolved_at');
    }
}

