<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260427000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Widen GSC snapshot columns so long queries and pages do not fail nightly ingestion';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('ALTER TABLE gsc_snapshots ALTER COLUMN query TYPE TEXT');
        $this->addSql('ALTER TABLE gsc_snapshots ALTER COLUMN page TYPE TEXT');
        $this->addSql('ALTER TABLE gsc_snapshots ALTER COLUMN date_range TYPE VARCHAR(100)');
    }

    public function down(Schema $schema): void
    {
        // Intentionally irreversible because narrowing these columns could truncate production data.
    }
}
