<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260223000000 extends AbstractMigration
{
    public function up(Schema $schema): void
    {
        $this->addSql('CREATE TABLE IF NOT EXISTS semrush_snapshots (id SERIAL PRIMARY KEY, domain VARCHAR(255) NOT NULL, organic_keywords INT DEFAULT 0, organic_traffic INT DEFAULT 0, fetched_at TIMESTAMP NOT NULL)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE IF EXISTS semrush_snapshots');
    }
}