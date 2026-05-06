<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260506030000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'One-time cleanup: close known junk tasks, mark lifecycle invalid, and capture audit trail.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('
            CREATE TABLE IF NOT EXISTS tasks_invalidation_audit (
                id SERIAL PRIMARY KEY,
                task_id INT NOT NULL,
                title TEXT,
                description TEXT,
                prior_status VARCHAR(50),
                prior_lifecycle_state VARCHAR(20),
                invalidated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                reason_code VARCHAR(50) NOT NULL,
                migration_version VARCHAR(50) NOT NULL DEFAULT \'Version20260506030000\'
            )
        ');

        $this->invalidateTasks(
            "(description LIKE '%/wp-content/uploads/%' OR title LIKE '%/wp-content/uploads/%')",
            'ASSET_URL_WP_UPLOADS'
        );

        $this->invalidateTasks(
            "(description ~ '/scripts/[^[:space:]]*\\.html' OR title ~ '/scripts/[^[:space:]]*\\.html')",
            'ASSET_URL_SCRIPTS_HTML'
        );

        $this->invalidateTasks(
            "(description ~ '(?<!:)//' OR title ~ '(?<!:)//')",
            'INVALID_URL_DOUBLE_SLASH'
        );

        $this->invalidateTasks(
            "(title ILIKE '%Skip%Media Asset%' OR description ILIKE '%Skip%Media Asset%')",
            'SKIP_MEDIA_ASSET'
        );
    }

    public function down(Schema $schema): void
    {
        $this->addSql("
            UPDATE tasks t
            SET lifecycle_state = COALESCE(a.prior_lifecycle_state, 'active'),
                status = COALESCE(a.prior_status, t.status),
                last_seen_at = NULL,
                completed_at = NULL
            FROM tasks_invalidation_audit a
            WHERE t.id = a.task_id
              AND a.migration_version = 'Version20260506030000'
        ");
    }

    private function invalidateTasks(string $predicate, string $reasonCode): void
    {
        $this->addSql("
            INSERT INTO tasks_invalidation_audit (task_id, title, description, prior_status, prior_lifecycle_state, reason_code)
            SELECT id, title, description, status, lifecycle_state, '{$reasonCode}'
            FROM tasks
            WHERE {$predicate}
              AND status NOT IN ('done', 'closed', 'rejected')
              AND (lifecycle_state IS NULL OR lifecycle_state = 'active')
        ");

        // Keep this guard so rollback/replay remains safe in environments where `completed_at`
        // may not exist yet (for example, a partially migrated local/staging database).
        $this->addSql("
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'tasks'
                      AND column_name = 'completed_at'
                ) THEN
                    UPDATE tasks
                    SET status = 'closed',
                        lifecycle_state = 'invalid',
                        last_seen_at = NOW(),
                        completed_at = NOW()
                    WHERE {$predicate}
                      AND status NOT IN ('done', 'closed', 'rejected')
                      AND (lifecycle_state IS NULL OR lifecycle_state = 'active');
                ELSE
                    UPDATE tasks
                    SET status = 'closed',
                        lifecycle_state = 'invalid',
                        last_seen_at = NOW()
                    WHERE {$predicate}
                      AND status NOT IN ('done', 'closed', 'rejected')
                      AND (lifecycle_state IS NULL OR lifecycle_state = 'active');
                END IF;
            END $$;
        ");
    }
}
