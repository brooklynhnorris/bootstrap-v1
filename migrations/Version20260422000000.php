<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260422000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add reviewer bulk-close endpoint support and relax seo_rules text columns';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('ALTER TABLE seo_rules ALTER COLUMN category TYPE TEXT');
        $this->addSql('ALTER TABLE seo_rules ALTER COLUMN assigned TYPE TEXT');
        $this->addSql('ALTER TABLE seo_rules ALTER COLUMN priority TYPE TEXT');
        $this->addSql('ALTER TABLE seo_rules ALTER COLUMN updated_by TYPE TEXT');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('ALTER TABLE seo_rules ALTER COLUMN category TYPE VARCHAR(100)');
        $this->addSql('ALTER TABLE seo_rules ALTER COLUMN assigned TYPE VARCHAR(255)');
        $this->addSql('ALTER TABLE seo_rules ALTER COLUMN priority TYPE VARCHAR(50)');
        $this->addSql('ALTER TABLE seo_rules ALTER COLUMN updated_by TYPE VARCHAR(255)');
    }
}
