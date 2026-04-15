<?php

namespace App\Service;

class ClaudeChatService
{
    public function sendPrompt(string $prompt, ?string $claudeKey = null, int $maxTokens = 500): ?string
    {
        $apiKey = $claudeKey ?? ($_ENV['ANTHROPIC_API_KEY'] ?? '');
        if ($apiKey === '') {
            return null;
        }

        try {
            $data = $this->callAnthropic($apiKey, [
                'model' => $_ENV['ANTHROPIC_MODEL'] ?? $_ENV['CLAUDE_MODEL'] ?? 'claude-sonnet-4-6',
                'max_tokens' => $maxTokens,
                'messages' => [['role' => 'user', 'content' => $prompt]],
            ], 30, 10);

            return $data['content'][0]['text'] ?? null;
        } catch (\Throwable $e) {
            return null;
        }
    }

    public function sendChat(string $systemPrompt, array $messages): array
    {
        $claudeMessages = [];
        foreach ($messages as $msg) {
            $claudeMessages[] = [
                'role' => $msg['role'] ?? 'user',
                'content' => $msg['content'] ?? '',
            ];
        }

        $model = $_ENV['ANTHROPIC_MODEL'] ?? $_ENV['CLAUDE_MODEL'] ?? 'claude-sonnet-4-6';
        $apiKey = $_ENV['ANTHROPIC_API_KEY'] ?? '';
        if ($apiKey === '') {
            throw new \RuntimeException('ANTHROPIC_API_KEY is not configured.');
        }

        $data = $this->callAnthropic($apiKey, [
            'model' => $model,
            'max_tokens' => 8192,
            'system' => $systemPrompt,
            'messages' => $claudeMessages,
        ], 90, 10);

        if (!isset($data['content'][0]['text'])) {
            $raw = json_encode($data);
            throw new \RuntimeException('Empty API response [prompt: ' . strlen($systemPrompt) . ' chars, raw: ' . substr((string) $raw, 0, 500) . ']');
        }

        return [
            'text' => $data['content'][0]['text'],
            'model' => $model,
            'api_key' => $apiKey,
        ];
    }

    public function validateEntityAlignment(string $rewriteText, string $userContext, string $claudeKey): ?array
    {
        if ($claudeKey === '') {
            return null;
        }

        $h1 = '';
        if (preg_match('/H1:\s*(.+?)(?:\n|$)/', $userContext, $m)) {
            $h1 = trim($m[1]);
        }

        $lines = explode("\n", $rewriteText);
        $firstSentences = '';
        $sentenceCount = 0;
        foreach ($lines as $line) {
            $line = trim($line);
            if (!$line || str_starts_with($line, '#') || str_starts_with($line, '**') || str_starts_with($line, '---') || str_starts_with($line, '```')) {
                continue;
            }
            if (str_starts_with($line, 'Current') || str_starts_with($line, 'Done when') || str_starts_with($line, 'Word count') || str_starts_with($line, 'URL:')) {
                continue;
            }
            $firstSentences .= $line . ' ';
            $sentenceCount++;
            if ($sentenceCount >= 3) {
                break;
            }
        }

        if (strlen($firstSentences) < 30) {
            return null;
        }

        $prompt = <<<PROMPT
You are an NLP entity validator for SEO content. Analyze the following text and determine if the first sentence has correct entity-predicate alignment for search engines.

H1 OF THE PAGE: {$h1}

FIRST SENTENCES OF THE REWRITE:
{$firstSentences}

ANALYSIS RULES:
1. The primary entity of the first sentence should match the H1's central topic (NOT always the brand name)
2. If H1 is about "Gooseneck Horse Trailers" -> the first sentence's primary subject should be about gooseneck horse trailers
3. If H1 is about "SafeTack Reverse Load" -> the first sentence's primary subject should be SafeTack
4. Brand name "Double D Trailers" should appear in the first 100 words but does NOT need to be the grammatical subject
5. The grammatical subject of the first sentence determines what Google NLP identifies as the primary entity
6. Passive voice buries the intended entity - "Horse trailers are built by Double D" -> entity = horse trailers; "Double D Trailers builds horse trailers" -> entity = Double D Trailers

Respond with EXACTLY this JSON (no markdown, no backticks):
{
  "detected_entity": "The primary entity/subject of the first sentence",
  "expected_entity": "What the primary entity SHOULD be based on the H1",
  "matches_h1": true or false,
  "subject_position": "correct" or "buried" or "missing",
  "brand_present_in_100_words": true or false,
  "issues": ["List of specific issues found, or empty array if none"],
  "revised_first_sentence": "If issues exist, provide a corrected first sentence. If no issues, null"
}
PROMPT;

        try {
            $text = $this->sendPrompt($prompt, $claudeKey, 500) ?? '';
            $text = preg_replace('/^```json\s*/', '', $text);
            $text = preg_replace('/\s*```$/', '', $text);
            $parsed = json_decode((string) $text, true);

            return is_array($parsed) && isset($parsed['detected_entity']) ? $parsed : null;
        } catch (\Throwable $e) {
            return null;
        }
    }

    private function callAnthropic(string $apiKey, array $payload, int $timeoutSeconds, int $connectTimeoutSeconds): array
    {
        $ch = curl_init('https://api.anthropic.com/v1/messages');
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => json_encode($payload),
            CURLOPT_HTTPHEADER => [
                'Content-Type: application/json',
                'x-api-key: ' . $apiKey,
                'anthropic-version: 2023-06-01',
            ],
            CURLOPT_TIMEOUT => $timeoutSeconds,
            CURLOPT_CONNECTTIMEOUT => $connectTimeoutSeconds,
        ]);

        $response = curl_exec($ch);
        $curlError = curl_error($ch);
        curl_close($ch);

        if ($curlError) {
            throw new \RuntimeException('API connection failed: ' . $curlError);
        }

        $data = json_decode((string) $response, true);
        if (isset($data['error'])) {
            $errMsg = $data['error']['message'] ?? 'Unknown API error';
            $errType = $data['error']['type'] ?? 'unknown';
            throw new \RuntimeException("API error ({$errType}): {$errMsg}");
        }

        return is_array($data) ? $data : [];
    }
}
