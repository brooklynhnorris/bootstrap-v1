<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class LearningExtractionService
{
    public function __construct(
        private Connection $db,
        private ClaudeChatService $claudeChatService
    ) {
    }

    public function extractLearnings(array $messages, string $lastResponse, string $apiKey, string $userName): void
    {
        if ($apiKey === '') {
            return;
        }

        $conversationSummary = $this->buildConversationSummary($messages, $lastResponse, $userName);
        $existingLearnings = $this->buildExistingLearningsContext();

        $extractPrompt = <<<PROMPT
You are analyzing a conversation between an SEO tool (Logiri) and its user to extract learnable insights that should persist across future conversations.

CONVERSATION:
{$conversationSummary}
{$existingLearnings}

Extract ONLY genuinely NEW learnings not already covered above. Categories:
- preferences: How the user likes information presented (format, detail level, tone)
- corrections: Things the user corrected about the tool's output or assumptions
- workflow: How the user prefers to work (task size, bundling, approval patterns)
- domain_knowledge: Business-specific facts the user shared that aren't in the data
- rules_feedback: Opinions on specific SEO rules or approaches

RULES:
- CRITICAL: Check the ALREADY STORED LEARNINGS above. If your proposed learning says the same thing as an existing one (even with different wording), DO NOT include it.
- Only extract learnings that would change future behavior.
- Each learning must be a short, actionable statement (under 30 words).
- Max 2 learnings per conversation. Often there are 0 - that's fine. Return [] if nothing NEW.
- Do NOT extract one-time task instructions as permanent preferences.

Respond ONLY with a JSON array. No other text. Empty array [] if no new learnings.
Example: [{"learning":"User wants page hierarchy shown in task briefs","category":"preferences","confidence":8}]
PROMPT;

        $text = $this->claudeChatService->sendPrompt($extractPrompt, $apiKey, 500);
        if (!$text) {
            return;
        }

        $text = preg_replace('/```json\s*/', '', $text);
        $text = preg_replace('/```\s*/', '', $text);
        $text = trim((string) $text);

        $learnings = json_decode($text, true);
        if (!is_array($learnings) || empty($learnings)) {
            return;
        }

        foreach (array_slice($learnings, 0, 2) as $learning) {
            $this->storeLearning($learning, $userName);
        }
    }

    private function buildConversationSummary(array $messages, string $lastResponse, string $userName): string
    {
        $summary = '';
        foreach (array_slice($messages, -6) as $message) {
            $role = ($message['role'] ?? 'assistant') === 'user' ? $userName : 'Logiri';
            $content = substr((string) ($message['content'] ?? ''), 0, 500);
            $summary .= "{$role}: {$content}\n\n";
        }

        return $summary . 'Logiri: ' . substr($lastResponse, 0, 500);
    }

    private function buildExistingLearningsContext(): string
    {
        try {
            $existing = $this->db->fetchAllAssociative(
                "SELECT learning, category FROM chat_learnings WHERE is_active = TRUE ORDER BY confidence DESC LIMIT 30"
            );
        } catch (\Exception $e) {
            return '';
        }

        if (empty($existing)) {
            return '';
        }

        $context = "\n\nALREADY STORED LEARNINGS (do NOT extract anything semantically similar to these):\n";
        foreach ($existing as $row) {
            $context .= '- [' . ($row['category'] ?? 'general') . '] ' . $row['learning'] . "\n";
        }

        return $context;
    }

    private function storeLearning(array $learning, string $userName): void
    {
        $text = trim((string) ($learning['learning'] ?? ''));
        if ($text === '' || strlen($text) < 10) {
            return;
        }

        $newWords = $this->extractSignificantWords($text);
        if (count($newWords) < 2 || $this->isDuplicate($newWords)) {
            return;
        }

        $this->db->insert('chat_learnings', [
            'learning' => substr($text, 0, 500),
            'category' => $learning['category'] ?? 'general',
            'confidence' => min(10, max(1, (int) ($learning['confidence'] ?? 5))),
            'learned_from' => substr($userName . ' conversation', 0, 255),
            'is_active' => true,
            'created_at' => date('Y-m-d H:i:s'),
        ]);
    }

    private function extractSignificantWords(string $text): array
    {
        $stopWords = ['user', 'wants', 'needs', 'should', 'requires', 'prefers', 'never', 'always', 'their', 'about', 'these', 'those', 'which', 'would', 'could'];

        return array_values(array_filter(
            explode(' ', strtolower((string) preg_replace('/[^a-z0-9 ]/i', '', $text))),
            fn(string $word): bool => strlen($word) > 4 && !in_array($word, $stopWords, true)
        ));
    }

    private function isDuplicate(array $newWords): bool
    {
        try {
            $existing = $this->db->fetchAllAssociative("SELECT learning FROM chat_learnings WHERE is_active = TRUE");
        } catch (\Exception $e) {
            return false;
        }

        foreach ($existing as $row) {
            $existingWords = array_filter(
                explode(' ', strtolower((string) preg_replace('/[^a-z0-9 ]/i', '', $row['learning']))),
                fn(string $word): bool => strlen($word) > 4
            );
            $overlap = count(array_intersect($newWords, $existingWords));
            $ratio = count($newWords) > 0 ? $overlap / count($newWords) : 0;
            if ($ratio > 0.4) {
                return true;
            }
        }

        return false;
    }
}
