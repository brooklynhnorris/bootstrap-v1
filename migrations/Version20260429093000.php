<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260429093000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Widen tasks.recheck_result to prevent nightly reviewer truncation failures';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('ALTER TABLE tasks ALTER COLUMN recheck_result TYPE VARCHAR(50)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('ALTER TABLE tasks ALTER COLUMN recheck_result TYPE VARCHAR(20)');
    }
}
