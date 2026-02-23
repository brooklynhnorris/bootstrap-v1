<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20250215000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Create google_oauth_tokens table';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('CREATE TABLE google_oauth_tokens (
            id SERIAL PRIMARY KEY,
            account_email VARCHAR(255) DEFAULT NULL,
            access_token TEXT NOT NULL,
            refresh_token TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE google_oauth_tokens');
    }
}