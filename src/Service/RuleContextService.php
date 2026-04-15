<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class RuleContextService
{
    public function __construct(private Connection $db)
    {
    }

    public function loadRecentReviews(): array
    {
        try {
            if (!$this->tableExists('rule_reviews')) {
                return [];
            }

            return $this->db->fetchAllAssociative(
                "SELECT rule_id, verdict, feedback, reviewed_by, reviewed_at FROM rule_reviews ORDER BY reviewed_at DESC LIMIT 10"
            );
        } catch (\Exception $e) {
            return [];
        }
    }

    public function loadOverrideCount(): int
    {
        try {
            if (!$this->tableExists('user_overrides')) {
                return 0;
            }

            return (int) $this->db->fetchOne('SELECT COUNT(*) FROM user_overrides');
        } catch (\Exception $e) {
            return 0;
        }
    }

    public function loadVerificationResults(): array
    {
        try {
            return $this->db->fetchAllAssociative(
                "SELECT rule_id, url, outcome_status, outcome_reason, metric_tracked,
                        impressions_before, impressions_after, clicks_before, clicks_after,
                        position_before, position_after, verified_at
                 FROM rule_outcomes
                 ORDER BY verified_at DESC LIMIT 15"
            );
        } catch (\Exception $e) {
            return [];
        }
    }

    public function loadRuleFeedback(): array
    {
        try {
            return $this->db->fetchAllAssociative(
                "SELECT rule_id, url, outcome_status, what_worked, what_didnt_work,
                        proposed_change, change_type, created_at
                 FROM rule_feedback
                 WHERE change_type != 'none'
                 ORDER BY created_at DESC LIMIT 10"
            );
        } catch (\Exception $e) {
            return [];
        }
    }

    public function loadRuleProposals(): array
    {
        try {
            return $this->db->fetchAllAssociative(
                "SELECT rule_id, change_type, summary, rationale, status, created_at
                 FROM rule_change_proposals
                 WHERE status = 'pending'
                 ORDER BY created_at DESC LIMIT 5"
            );
        } catch (\Exception $e) {
            return [];
        }
    }

    private function tableExists(string $tableName): bool
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
            [$tableName]
        );

        return !empty($tables);
    }
}
