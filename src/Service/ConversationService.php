<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

class ConversationService
{
    public function __construct(private Connection $db)
    {
    }

    public function ensureConversation(?int $conversationId, ?int $userId, array $messages, ?array $activePersona = null): int
    {
        if ($conversationId) {
            $ownedConversation = $this->findOwnedConversation($conversationId, $userId);
            if (!$ownedConversation) {
                throw new \RuntimeException('Conversation not found');
            }

            $this->db->executeStatement(
                'UPDATE conversations SET updated_at = ? WHERE id = ?',
                [date('Y-m-d H:i:s'), $conversationId]
            );

            return $conversationId;
        }

        $firstUserMsg = '';
        foreach ($messages as $msg) {
            if (($msg['role'] ?? null) === 'user') {
                $firstUserMsg = (string) ($msg['content'] ?? '');
                break;
            }
        }

        $this->db->insert('conversations', [
            'user_id' => $userId,
            'title' => $this->generateTitle($firstUserMsg),
            'persona_name' => $activePersona['name'] ?? null,
            'created_at' => date('Y-m-d H:i:s'),
            'updated_at' => date('Y-m-d H:i:s'),
        ]);

        return (int) $this->db->lastInsertId();
    }

    public function saveUserMessage(int $conversationId, ?array $message): void
    {
        if (!$message || ($message['role'] ?? null) !== 'user') {
            return;
        }

        $this->db->insert('messages', [
            'conversation_id' => $conversationId,
            'role' => 'user',
            'content' => (string) ($message['content'] ?? ''),
            'created_at' => date('Y-m-d H:i:s'),
        ]);
    }

    public function saveAssistantMessage(int $conversationId, string $content): void
    {
        $this->db->insert('messages', [
            'conversation_id' => $conversationId,
            'role' => 'assistant',
            'content' => $content,
            'created_at' => date('Y-m-d H:i:s'),
        ]);
    }

    public function findOwnedConversation(int $conversationId, ?int $userId): ?array
    {
        $conversation = $this->db->fetchAssociative(
            'SELECT id, user_id, title, persona_name FROM conversations WHERE id = ? AND user_id = ? LIMIT 1',
            [$conversationId, $userId]
        );

        return $conversation ?: null;
    }

    private function generateTitle(string $firstMessage): string
    {
        if (!$firstMessage) {
            return 'New conversation';
        }

        $lower = strtolower($firstMessage);
        if (str_contains($lower, 'briefing') || str_contains($lower, 'seo brief') || str_contains($lower, 'what should i start')) {
            return 'Daily Briefing — ' . date('M j');
        }

        $clean = preg_replace('/\s+/', ' ', trim($firstMessage));
        return strlen($clean) > 60 ? substr($clean, 0, 57) . '...' : $clean;
    }
}
