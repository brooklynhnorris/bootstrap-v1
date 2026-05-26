<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260526090000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add is_soft_404 flags to page_crawl_snapshots and page_facts.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql("ALTER TABLE page_crawl_snapshots ADD COLUMN IF NOT EXISTS is_soft_404 BOOLEAN DEFAULT FALSE");
        $this->addSql("CREATE INDEX IF NOT EXISTS idx_page_crawl_snapshots_soft_404 ON page_crawl_snapshots (is_soft_404) WHERE is_soft_404 = TRUE");
        $this->addSql("ALTER TABLE page_facts ADD COLUMN IF NOT EXISTS is_soft_404 BOOLEAN DEFAULT FALSE");
    }

    public function down(Schema $schema): void
    {
        $this->addSql('ALTER TABLE page_facts DROP COLUMN IF EXISTS is_soft_404');
        $this->addSql('DROP INDEX IF EXISTS idx_page_crawl_snapshots_soft_404');
        $this->addSql('ALTER TABLE page_crawl_snapshots DROP COLUMN IF EXISTS is_soft_404');
    }
}

