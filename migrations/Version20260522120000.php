<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260522120000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add schema_parse_status to page_crawl_snapshots for JSON-LD parse visibility.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql("ALTER TABLE page_crawl_snapshots ADD COLUMN IF NOT EXISTS schema_parse_status VARCHAR(20) DEFAULT 'ok'");
    }

    public function down(Schema $schema): void
    {
        $this->addSql('ALTER TABLE page_crawl_snapshots DROP COLUMN IF EXISTS schema_parse_status');
    }
}

