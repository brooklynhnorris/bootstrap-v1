<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260302000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add conversations, messages, rule_reviews, and user_overrides tables';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('CREATE TABLE IF NOT EXISTS conversations (
            id          SERIAL PRIMARY KEY,
            user_id     INT DEFAULT NULL,
            title       VARCHAR(255) DEFAULT NULL,
            created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            is_archived BOOLEAN DEFAULT FALSE
        )');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_conversations_user ON conversations (user_id)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_conversations_updated ON conversations (updated_at DESC)');

        $this->addSql('CREATE TABLE IF NOT EXISTS messages (
            id              SERIAL PRIMARY KEY,
            conversation_id INT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            role            VARCHAR(20) NOT NULL,
            content         TEXT NOT NULL,
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages (conversation_id)');

        $this->addSql('CREATE TABLE IF NOT EXISTS rule_reviews (
            id              SERIAL PRIMARY KEY,
            conversation_id INT DEFAULT NULL REFERENCES conversations(id) ON DELETE SET NULL,
            rule_id         VARCHAR(20) NOT NULL,
            verdict         VARCHAR(30) NOT NULL,
            feedback        TEXT DEFAULT NULL,
            reviewed_by     VARCHAR(100) DEFAULT NULL,
            reviewed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_reviews_rule ON rule_reviews (rule_id)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_rule_reviews_date ON rule_reviews (reviewed_at DESC)');

        $this->addSql('CREATE TABLE IF NOT EXISTS user_overrides (
            id             SERIAL PRIMARY KEY,
            url            TEXT NOT NULL,
            field          VARCHAR(50) NOT NULL,
            original_value TEXT DEFAULT NULL,
            override_value TEXT NOT NULL,
            reason         TEXT DEFAULT NULL,
            overridden_by  VARCHAR(100) DEFAULT NULL,
            created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(url, field)
        )');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_overrides_url ON user_overrides (url)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE IF EXISTS user_overrides');
        $this->addSql('DROP TABLE IF EXISTS rule_reviews');
        $this->addSql('DROP TABLE IF EXISTS messages');
        $this->addSql('DROP TABLE IF EXISTS conversations');
    }
}

    
