<?php

namespace App\Service;

final class TaskRejectionGuardrailClassifier
{
    public static function classify(string $dismissType, string $dismissScope, string $reason, array $task = [], array $pageContext = []): string
    {
        $text = strtolower(trim(implode("\n", [
            $dismissType,
            $dismissScope,
            $reason,
            (string) ($task['title'] ?? ''),
            (string) ($pageContext['page_type'] ?? ''),
            (string) ($pageContext['target_query'] ?? ''),
        ])));

        if (
            (str_contains($text, 'no video') || str_contains($text, 'there is not a video') || str_contains($text, 'no embed'))
            && str_contains($text, 'video')
        ) {
            return 'no_video_on_page';
        }

        if (
            str_contains($text, 'missing the actual copy block')
            || str_contains($text, 'missing the actual copy')
            || str_contains($text, 'nothing in the play brief')
            || str_contains($text, 'play brief is missing')
            || str_contains($text, 'schema should provide the schema')
            || str_contains($text, 'not include a paste-ready')
            || (str_contains($text, 'verbatim') && str_contains($text, 'missing'))
        ) {
            return 'missing_payload';
        }

        if (
            str_contains($text, 'find the generic')
            || str_contains($text, 'too generic')
            || str_contains($text, 'be more specific')
            || str_contains($text, 'what exactly to replace')
            || str_contains($text, 'not able to scan the body copy itself')
        ) {
            return 'vague_placement';
        }

        if (
            str_contains($text, 'search incognito')
            || str_contains($text, 'report back what you see')
            || str_contains($text, 'ai overview')
            || str_contains($text, 'featured snippet')
            || str_contains($text, 'name the competitor')
        ) {
            return 'manual_serp_check';
        }

        if (
            str_contains($text, '/wp-content/')
            || str_contains($text, 'raw image')
            || str_contains($text, 'attachment page')
            || str_contains($text, '.jpg')
            || str_contains($text, '.png')
            || str_contains($text, '.pdf')
        ) {
            return 'asset_or_bad_url';
        }

        if (
            str_contains($text, 'review survey page')
            || str_contains($text, 'single-anecdote review')
            || str_contains($text, 'customer review')
            || str_contains($text, '3d printing')
            || str_contains($text, 'horse jockeys')
            || str_contains($text, 'doesn\'t exist')
            || str_contains($text, 'redirects to articles')
            || str_contains($text, 'irrelevant')
        ) {
            return 'page_type_mismatch';
        }

        if (
            str_contains($text, 'crawl data')
            || str_contains($text, 'fresh crawl')
            || str_contains($text, 'not crawled')
            || str_contains($text, 'wrong crawl')
            || str_contains($text, 'stale data')
        ) {
            return 'crawl_data_mismatch';
        }

        if ($dismissType === 'duplicate') {
            return 'duplicate_task';
        }

        if ($dismissType === 'false_positive') {
            return 'rule_false_positive';
        }

        if (in_array($dismissType, ['invalid', 'not_applicable'], true)) {
            return 'rule_scope_mismatch';
        }

        return 'general_rejection';
    }

    public static function shouldCreateSuppressionRecord(string $guardrailCode, string $dismissType): bool
    {
        if ($dismissType === 'duplicate') {
            return false;
        }

        return in_array($guardrailCode, [
            'no_video_on_page',
            'missing_payload',
            'vague_placement',
            'manual_serp_check',
            'asset_or_bad_url',
            'page_type_mismatch',
            'rule_false_positive',
            'rule_scope_mismatch',
        ], true);
    }

    public static function inferScope(string $dismissType, string $guardrailCode, string $reason = ''): string
    {
        $reason = strtolower(trim($reason));

        if ($dismissType === 'duplicate' || $dismissType === 'wont_fix') {
            return 'task_only';
        }

        if (in_array($guardrailCode, ['page_type_mismatch', 'rule_scope_mismatch', 'asset_or_bad_url'], true)) {
            return 'rule_page_type';
        }

        if (in_array($guardrailCode, ['missing_payload', 'manual_serp_check'], true)) {
            return 'rule_global';
        }

        if (str_contains($reason, 'same rule') || str_contains($reason, 'similar pages')) {
            return 'rule_page_type';
        }

        return 'url_only';
    }
}
