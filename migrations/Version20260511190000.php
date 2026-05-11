<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260511190000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add deterministic ILA-005 support fields and mark ILA-005 deterministic.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('ALTER TABLE page_facts ADD COLUMN IF NOT EXISTS body_internal_link_count INT');
        $this->addSql('ALTER TABLE page_facts ADD COLUMN IF NOT EXISTS body_link_extraction_confident BOOLEAN');
        $this->addSql('ALTER TABLE page_facts ADD COLUMN IF NOT EXISTS is_noindex BOOLEAN');

        $this->addSql('ALTER TABLE seo_rules ADD COLUMN IF NOT EXISTS deterministic_since TIMESTAMP');
        $this->addSql("UPDATE seo_rules SET deterministic_since = NOW() WHERE rule_id = 'ILA-005'");
    }

    public function down(Schema $schema): void
    {
        $this->addSql('ALTER TABLE seo_rules DROP COLUMN IF EXISTS deterministic_since');
        $this->addSql('ALTER TABLE page_facts DROP COLUMN IF EXISTS body_internal_link_count');
        $this->addSql('ALTER TABLE page_facts DROP COLUMN IF EXISTS body_link_extraction_confident');
        $this->addSql('ALTER TABLE page_facts DROP COLUMN IF EXISTS is_noindex');
    }
}

