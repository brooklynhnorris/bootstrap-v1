<?php

namespace App\Service;

final class AvrScorer
{
    private const ACTION_FAMILY_EFFORT = [
        'metadata_fix' => 1,
        'image_fix' => 1,
        'link_add' => 1,
        'reporting' => 1,
        'schema_impl' => 2,
        'technical_fix' => 2,
        'ux_conversion' => 2,
        'trust_signal_add' => 2,
        'local_optimization' => 2,
        'competitive_research' => 2,
        'general_fix' => 2,
        'performance_fix' => 3,
        'content_expand' => 3,
    ];

    private const TIER_WEIGHT = [
        'tier_a' => 1.0,
        'tier_b' => 0.7,
        'tier_c' => 0.4,
    ];

    public function score(array $violation, array $pageFacts, array $rule): array
    {
        $impact = $this->computeImpact($pageFacts, $rule);
        $confidence = $this->computeConfidence($pageFacts, $violation);
        $urgency = $this->computeUrgency($violation);
        $revenueProximity = $this->computeRevenueProximity($pageFacts, $rule);
        $actionFamily = (string) ($rule['action_family'] ?? 'general_fix');
        $effort = $this->resolveEffort($actionFamily);

        $product = $impact * $confidence * $urgency * $revenueProximity;
        $geometricMean = $product > 0.0 ? pow($product, 0.25) : 0.0;
        $effortDivisor = max(1.0, 0.7 + ($effort * 0.15));
        $adjusted = $geometricMean / $effortDivisor;

        // Rebalanced 2026-05-20: apply severity boost after effort divisor so it
        // is not swallowed by effort penalties for fundamental fixes.
        $fundamentalActionFamilies = ['content_expand', 'schema_impl', 'technical_fix'];
        $severityBoost = in_array(strtolower($actionFamily), $fundamentalActionFamilies, true) ? 0.10 : 0.0;
        $adjusted = min(1.0, $adjusted + $severityBoost);

        $avrScore = (int) round(max(0.0, min(1.0, $adjusted)) * 100);

        return [
            'avr_score' => $avrScore,
            'breakdown' => [
                'impact' => round($impact, 4),
                'confidence' => round($confidence, 4),
                'urgency' => round($urgency, 4),
                'revenue_proximity' => round($revenueProximity, 4),
                'effort' => $effort,
            ],
            'inputs' => [
                'tier' => (string) ($rule['tier'] ?? ''),
                'action_family' => (string) ($rule['action_family'] ?? 'general_fix'),
                'business_multiplier' => (float) ($rule['business_multiplier'] ?? 1.0),
                'target_query_impressions' => (int) ($pageFacts['target_query_impressions'] ?? 0),
                'page_type' => (string) ($pageFacts['page_type'] ?? ''),
                'last_crawled_at' => $pageFacts['last_crawled_at'] ?? null,
                'is_indexable' => $this->toBool($pageFacts['is_indexable'] ?? false),
                'consecutive_violation_count' => (int) ($violation['consecutive_violation_count'] ?? 0),
                'position_delta' => (float) ($violation['position_delta'] ?? 0.0),
                'open_task_age_days' => (float) ($violation['open_task_age_days'] ?? 0.0),
                'conversions_28d' => (int) ($pageFacts['conversions_28d'] ?? 0),
                'asset_filter_clean' => $this->toBool($violation['asset_filter_clean'] ?? true),
            ],
        ];
    }

    private function computeImpact(array $pageFacts, array $rule): float
    {
        $tierKey = strtolower((string) ($rule['tier'] ?? 'tier_c'));
        $tierWeight = self::TIER_WEIGHT[$tierKey] ?? self::TIER_WEIGHT['tier_c'];
        $impressions = max(0, (int) ($pageFacts['target_query_impressions'] ?? 0));
        $impressionSignal = min(1.0, log10($impressions + 1) / 4.0);
        $pageTypeWeight = $this->resolvePageTypeWeight((string) ($pageFacts['page_type'] ?? ''));

        // Rebalanced 2026-05-19: dropped impressions weight from 0.4 to 0.15 to break
        // survivorship bias (broken pages can't earn impressions, so they scored low
        // and got suppressed). Tier rises 0.4 -> 0.5 and page_type rises 0.2 -> 0.35.
        // Weights still sum to 1.0.
        return (0.5 * $tierWeight) + (0.15 * $impressionSignal) + (0.35 * $pageTypeWeight);
    }

    private function computeConfidence(array $pageFacts, array $violation): float
    {
        $recency = $this->recencyScore($pageFacts['last_crawled_at'] ?? null);
        $isIndexable = $this->toBool($pageFacts['is_indexable'] ?? false) ? 1.0 : 0.0;
        $requiredFieldsPresent = $this->requiredFieldsPresent($pageFacts, (string) ($violation['rule_id'] ?? '')) ? 1.0 : 0.0;
        $assetFilterClean = $this->toBool($violation['asset_filter_clean'] ?? true) ? 1.0 : 0.0;

        return ($recency + $isIndexable + $requiredFieldsPresent + $assetFilterClean) / 4.0;
    }

    private function computeUrgency(array $violation): float
    {
        $consecutive = max(0, (int) ($violation['consecutive_violation_count'] ?? 0));
        $consecutiveScore = min(1.0, $consecutive / 12.0);

        $positionDelta = (float) ($violation['position_delta'] ?? 0.0);
        $positionWorsening = max(0.0, $positionDelta);
        $positionScore = min(1.0, $positionWorsening / 10.0);

        $taskAgeDays = max(0.0, (float) ($violation['open_task_age_days'] ?? 0.0));
        $taskAgeScore = min(1.0, $taskAgeDays / 14.0);

        return (0.5 * $consecutiveScore) + (0.3 * $positionScore) + (0.2 * $taskAgeScore);
    }

    private function computeRevenueProximity(array $pageFacts, array $rule): float
    {
        $moneyPage = $this->resolvePageTypeWeight((string) ($pageFacts['page_type'] ?? ''));

        // Rebalanced 2026-05-19: replaced binary 1.0/0.0 conversion signal with a
        // softer scale. Broken pages can't convert (because they can't rank), so
        // the binary version dropped 0.30 from revenue_proximity for the very
        // pages that most need attention. New version uses 0.3 floor for
        // non-converters and log-scaled credit for converters.
        $conversions = (int) ($pageFacts['conversions_28d'] ?? 0);
        $conversionSignal = $conversions > 0
            ? min(1.0, log10($conversions + 1) / 2.0)
            : 0.3;

        $businessMultiplier = (float) ($rule['business_multiplier'] ?? 1.0);
        $normalizedBusinessMultiplier = min(1.0, max(0.0, $businessMultiplier / 2.0));

        return (0.5 * $moneyPage) + (0.3 * $conversionSignal) + (0.2 * $normalizedBusinessMultiplier);
    }

    private function resolveEffort(string $actionFamily): int
    {
        $key = strtolower(trim($actionFamily));
        return self::ACTION_FAMILY_EFFORT[$key] ?? self::ACTION_FAMILY_EFFORT['general_fix'];
    }

    private function resolvePageTypeWeight(string $pageType): float
    {
        return match (strtolower(trim($pageType))) {
            'core' => 1.0,
            'outer' => 0.6,
            'utility' => 0.2,
            default => 0.4,
        };
    }

    private function recencyScore(mixed $lastCrawledAt): float
    {
        if (!is_string($lastCrawledAt) || trim($lastCrawledAt) === '') {
            return 0.0;
        }

        try {
            $crawlTs = strtotime($lastCrawledAt);
            if ($crawlTs === false) {
                return 0.0;
            }
            $ageDays = max(0.0, (time() - $crawlTs) / 86400);
            if ($ageDays <= 2) {
                return 1.0;
            }
            if ($ageDays <= 7) {
                return 0.7;
            }
            if ($ageDays <= 14) {
                return 0.4;
            }
            return 0.1;
        } catch (\Throwable) {
            return 0.0;
        }
    }

    private function requiredFieldsPresent(array $pageFacts, string $ruleId): bool
    {
        $ruleId = strtoupper(trim($ruleId));
        if (str_starts_with($ruleId, 'SCH')) {
            return trim((string) ($pageFacts['schema_types'] ?? '')) !== '';
        }
        if (str_starts_with($ruleId, 'ILA')) {
            return isset($pageFacts['internal_link_count']);
        }
        return trim((string) ($pageFacts['url'] ?? '')) !== '';
    }

    private function toBool(mixed $value): bool
    {
        return $value === true || $value === 1 || $value === '1' || $value === 't' || $value === 'true';
    }
}
