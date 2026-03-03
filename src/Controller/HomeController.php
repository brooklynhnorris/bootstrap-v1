<?php

namespace App\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\Routing\Annotation\Route;
use Doctrine\DBAL\Connection;

class HomeController extends AbstractController
{
    public function __construct(private Connection $db)
    {
        $this->ensureSchema();
    }

    private function ensureSchema(): void
    {
        try {
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS conversations (id SERIAL PRIMARY KEY, user_id INT DEFAULT NULL, title VARCHAR(255) DEFAULT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, is_archived BOOLEAN DEFAULT FALSE)');
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, conversation_id INT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE, role VARCHAR(20) NOT NULL, content TEXT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)');
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS rule_reviews (id SERIAL PRIMARY KEY, conversation_id INT DEFAULT NULL REFERENCES conversations(id) ON DELETE SET NULL, rule_id VARCHAR(20) NOT NULL, verdict VARCHAR(30) NOT NULL, feedback TEXT DEFAULT NULL, reviewed_by VARCHAR(100) DEFAULT NULL, reviewed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)');
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS user_overrides (id SERIAL PRIMARY KEY, url TEXT NOT NULL, field VARCHAR(50) NOT NULL, original_value TEXT DEFAULT NULL, override_value TEXT NOT NULL, reason TEXT DEFAULT NULL, overridden_by VARCHAR(100) DEFAULT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(url, field))');
            $this->db->executeStatement('CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages (conversation_id)');
            $this->db->executeStatement('CREATE INDEX IF NOT EXISTS idx_overrides_url ON user_overrides (url)');
        } catch (\Exception $e) {
            // Tables already exist or DB not ready — fail silently
        }
    }

    // ─────────────────────────────────────────────
    //  MAIN PAGE
    // ─────────────────────────────────────────────

    #[Route('/', name: 'home')]
    public function index(): Response
    {
        $user = $this->getUser();
        $tasks = $this->db->fetchAllAssociative(
            "SELECT * FROM tasks WHERE status != 'done' ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END, created_at DESC LIMIT 20"
        );
        $rechecks = $this->db->fetchAllAssociative(
            "SELECT * FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false ORDER BY recheck_date ASC LIMIT 10"
        );
        $taskCounts = $this->db->fetchAssociative(
            "SELECT
                COUNT(*) FILTER (WHERE status != 'done' AND priority IN ('urgent','high')) as urgent,
                COUNT(*) FILTER (WHERE status = 'in_progress') as active,
                COUNT(*) FILTER (WHERE status = 'done') as done
            FROM tasks"
        );

        // Load recent conversations for sidebar history
        $userId = $user ? $user->getId() : null;
        $conversations = $this->db->fetchAllAssociative(
            "SELECT id, title, created_at, updated_at FROM conversations
             WHERE user_id = ? AND is_archived = FALSE
             ORDER BY updated_at DESC LIMIT 20",
            [$userId]
        );

        return $this->render('home/index.html.twig', [
            'userName'      => $user ? ($user->getName() ?? explode('@', $user->getEmail())[0]) : 'User',
            'userRole'      => $user ? ($user->getTeamRole() ?? 'Owner') : 'Owner',
            'userEmail'     => $user ? $user->getEmail() : '',
            'tasks'         => $tasks,
            'rechecks'      => $rechecks,
            'taskCounts'    => $taskCounts ?: ['urgent' => 0, 'active' => 0, 'done' => 0],
            'conversations' => $conversations,
        ]);
    }

    // ─────────────────────────────────────────────
    //  CHAT
    // ─────────────────────────────────────────────

    #[Route('/chat', name: 'chat', methods: ['POST'])]
    public function chat(Request $request): JsonResponse
    {
        $body           = json_decode($request->getContent(), true);
        $messages       = $body['messages'] ?? [];
        $conversationId = $body['conversation_id'] ?? null;

        $user     = $this->getUser();
        $userName = $user ? ($user->getName() ?? explode('@', $user->getEmail())[0]) : 'User';
        $userRole = $user ? ($user->getTeamRole() ?? 'Owner') : 'Owner';
        $userId   = $user ? $user->getId() : null;

        // ── Fetch data for system prompt ──
        $semrush = $this->db->fetchAssociative(
            'SELECT organic_keywords, organic_traffic, fetched_at FROM semrush_snapshots ORDER BY fetched_at DESC LIMIT 1'
        );
        $topQueries28d = $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE date_range = '28d' ORDER BY impressions DESC LIMIT 20"
        );
        $topQueries90d = $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE date_range = '90d' ORDER BY impressions DESC LIMIT 15"
        );
        $pageAggregates = $this->db->fetchAllAssociative(
            "SELECT page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE query = '__PAGE_AGGREGATE__' ORDER BY impressions DESC LIMIT 15"
        );
        $brandedQueries = $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, position FROM gsc_snapshots WHERE date_range = '28d_branded' ORDER BY impressions DESC LIMIT 10"
        );
        $cannibalizationCandidates = $this->db->fetchAllAssociative(
            "SELECT query, COUNT(DISTINCT page) as page_count, SUM(impressions) as total_impressions
             FROM gsc_snapshots WHERE date_range = '28d' AND query != '__PAGE_AGGREGATE__'
             GROUP BY query HAVING COUNT(DISTINCT page) > 1
             ORDER BY total_impressions DESC LIMIT 15"
        );
        $topPages = $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, pageviews, bounce_rate, avg_engagement_time, engaged_sessions, conversions
             FROM ga4_snapshots WHERE date_range = '28d' ORDER BY sessions DESC LIMIT 15"
        );
        $previousPages = $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, pageviews, bounce_rate, avg_engagement_time, conversions
             FROM ga4_snapshots WHERE date_range = '28d_previous' ORDER BY sessions DESC LIMIT 15"
        );
        $landingPages = $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, bounce_rate, avg_engagement_time, conversions
             FROM ga4_snapshots WHERE date_range = '28d_landing' ORDER BY sessions DESC LIMIT 10"
        );
        $adsCampaigns = $this->db->fetchAllAssociative(
            "SELECT campaign_name, impressions, clicks, cost_micros, conversions, ctr, average_cpc, status
             FROM google_ads_snapshots WHERE data_type = 'campaign' ORDER BY cost_micros DESC LIMIT 8"
        );
        $adsKeywords = $this->db->fetchAllAssociative(
            "SELECT keyword, match_type, campaign_name, impressions, clicks, cost_micros, conversions, ctr, average_cpc
             FROM google_ads_snapshots WHERE data_type = 'keyword' ORDER BY cost_micros DESC LIMIT 8"
        );
        $adsSearchTerms = $this->db->fetchAllAssociative(
            "SELECT keyword as search_term, campaign_name, impressions, clicks, cost_micros, conversions, ctr
             FROM google_ads_snapshots WHERE data_type = 'search_term' ORDER BY clicks DESC LIMIT 8"
        );
        $adsDailySpend = $this->db->fetchAllAssociative(
            "SELECT date_range as date, cost_micros, clicks, impressions, conversions
             FROM google_ads_snapshots WHERE data_type = 'daily_spend' ORDER BY date_range DESC LIMIT 7"
        );
        $activeTasks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, assigned_role, status, priority, estimated_hours, logged_hours, created_at FROM tasks WHERE status != 'done' ORDER BY created_at DESC LIMIT 10"
        );
        $pendingRechecks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, recheck_date, recheck_type FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false AND recheck_date <= CURRENT_DATE + INTERVAL '3 days' ORDER BY recheck_date ASC LIMIT 5"
        );

        // ── Load crawl data for rules engine ──
        $crawlData = $this->loadCrawlData();

        // ── Load recent rule reviews and overrides for context ──
        $recentReviews = $this->loadRecentReviews();
        $overrideCount = $this->loadOverrideCount();

        // ── Persist conversation ──
        if (!$conversationId) {
            // New conversation — create it
            $firstUserMsg = '';
            foreach ($messages as $msg) {
                if ($msg['role'] === 'user') { $firstUserMsg = $msg['content']; break; }
            }
            $title = $this->generateTitle($firstUserMsg);
            $this->db->insert('conversations', [
                'user_id'    => $userId,
                'title'      => $title,
                'created_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
            $conversationId = $this->db->lastInsertId();
        } else {
            // Touch updated_at
            $this->db->executeStatement(
                'UPDATE conversations SET updated_at = ? WHERE id = ?',
                [date('Y-m-d H:i:s'), $conversationId]
            );
        }

        // Save the latest user message
        $lastMsg = end($messages);
        if ($lastMsg && $lastMsg['role'] === 'user') {
            $this->db->insert('messages', [
                'conversation_id' => $conversationId,
                'role'            => 'user',
                'content'         => $lastMsg['content'],
                'created_at'      => date('Y-m-d H:i:s'),
            ]);
        }

        // ── Build system prompt ──
        $systemPrompt = $this->buildSystemPrompt(
            $semrush ?: [], $topQueries28d, $topPages, $userName, $userRole,
            $activeTasks, $pendingRechecks, $topQueries90d, $pageAggregates,
            $brandedQueries, $cannibalizationCandidates, $previousPages, $landingPages,
            $adsCampaigns, $adsKeywords, $adsSearchTerms, $adsDailySpend,
            $recentReviews, $overrideCount, $crawlData
        );

        // ── Call Claude API ──
        $claudeMessages = [];
        foreach ($messages as $msg) {
            $claudeMessages[] = ['role' => $msg['role'], 'content' => $msg['content']];
        }

        $claudeKey   = $_ENV['ANTHROPIC_API_KEY'] ?? '';
        $claudeModel = $_ENV['ANTHROPIC_MODEL'] ?? 'claude-sonnet-4-5';

        $response = file_get_contents(
            'https://api.anthropic.com/v1/messages',
            false,
            stream_context_create([
                'http' => [
                    'method'        => 'POST',
                    'header'        => implode("\r\n", [
                        'Content-Type: application/json',
                        'x-api-key: ' . $claudeKey,
                        'anthropic-version: 2023-06-01',
                    ]),
                    'content'       => json_encode([
                        'model'      => $claudeModel,
                        'max_tokens' => 8192,
                        'system'     => $systemPrompt,
                        'messages'   => $claudeMessages,
                    ]),
                    'ignore_errors' => true,
                ],
            ])
        );

        $data = json_decode($response, true);

        if (isset($data['error'])) {
            return new JsonResponse(['error' => $data['error']['message']], 500);
        }

        $text = $data['content'][0]['text'] ?? 'No response from Claude.';

        // ── Auto-create tasks ──
        $tasksCreated = [];
        if (preg_match('/<!-- TASKS_JSON -->\s*(.*?)\s*<!-- \/TASKS_JSON -->/s', $text, $matches)) {
            $aiTasks = json_decode(trim($matches[1]), true);
            if (is_array($aiTasks)) {
                foreach ($aiTasks as $aiTask) {
                    $title = $aiTask['title'] ?? '';
                    if (!$title) continue;
                    $existing = $this->db->fetchAssociative(
                        "SELECT id FROM tasks WHERE title = ? AND status != 'done' LIMIT 1", [$title]
                    );
                    if ($existing) continue;
                    $priority = in_array($aiTask['priority'] ?? '', ['critical','high','medium','low']) ? $aiTask['priority'] : 'medium';
                    $this->db->insert('tasks', [
                        'title'           => $title,
                        'description'     => $aiTask['description'] ?? null,
                        'assigned_to'     => $aiTask['assigned_to'] ?? null,
                        'assigned_role'   => $aiTask['role'] ?? null,
                        'status'          => 'pending',
                        'priority'        => $priority,
                        'estimated_hours' => floatval($aiTask['estimated_hours'] ?? 1),
                        'logged_hours'    => 0,
                        'recheck_type'    => $aiTask['recheck_type'] ?? null,
                        'created_at'      => date('Y-m-d H:i:s'),
                    ]);
                    $tasksCreated[] = $title;
                }
            }
            $text = preg_replace('/<!-- TASKS_JSON -->.*?<!-- \/TASKS_JSON -->/s', '', $text);
            $text = rtrim($text);
        }

        // ── Save assistant response ──
        $this->db->insert('messages', [
            'conversation_id' => $conversationId,
            'role'            => 'assistant',
            'content'         => $text,
            'created_at'      => date('Y-m-d H:i:s'),
        ]);

        return new JsonResponse([
            'response'        => $text,
            'tasks_created'   => $tasksCreated,
            'conversation_id' => $conversationId,
        ]);
    }

    // ─────────────────────────────────────────────
    //  CONVERSATION API
    // ─────────────────────────────────────────────

    #[Route('/api/conversations', name: 'api_conversations_list', methods: ['GET'])]
    public function listConversations(): JsonResponse
    {
        $user   = $this->getUser();
        $userId = $user ? $user->getId() : null;
        $convos = $this->db->fetchAllAssociative(
            "SELECT id, title, created_at, updated_at FROM conversations
             WHERE user_id = ? AND is_archived = FALSE
             ORDER BY updated_at DESC LIMIT 30",
            [$userId]
        );
        return new JsonResponse($convos);
    }

    #[Route('/api/conversations/{id}/messages', name: 'api_conversation_messages', methods: ['GET'])]
    public function getConversationMessages(int $id): JsonResponse
    {
        $msgs = $this->db->fetchAllAssociative(
            'SELECT role, content, created_at FROM messages WHERE conversation_id = ? ORDER BY created_at ASC',
            [$id]
        );
        return new JsonResponse($msgs);
    }

    #[Route('/api/conversations/{id}/archive', name: 'api_conversation_archive', methods: ['POST'])]
    public function archiveConversation(int $id): JsonResponse
    {
        $this->db->executeStatement('UPDATE conversations SET is_archived = TRUE WHERE id = ?', [$id]);
        return new JsonResponse(['ok' => true]);
    }

    // ─────────────────────────────────────────────
    //  RULE REVIEW API
    // ─────────────────────────────────────────────

    #[Route('/api/rule-review', name: 'api_rule_review', methods: ['POST'])]
    public function saveRuleReview(Request $request): JsonResponse
    {
        $body           = json_decode($request->getContent(), true);
        $user           = $this->getUser();
        $userName       = $user ? ($user->getName() ?? explode('@', $user->getEmail())[0]) : 'User';
        $conversationId = $body['conversation_id'] ?? null;
        $ruleId         = $body['rule_id'] ?? '';
        $verdict        = $body['verdict'] ?? '';
        $feedback       = $body['feedback'] ?? null;
        $corrections    = $body['corrections'] ?? [];   // array of {url, field, original, override, reason}

        if (!$ruleId || !$verdict) {
            return new JsonResponse(['error' => 'rule_id and verdict required'], 400);
        }

        // Save the review
        $this->db->insert('rule_reviews', [
            'conversation_id' => $conversationId,
            'rule_id'         => $ruleId,
            'verdict'         => $verdict,
            'feedback'        => $feedback,
            'reviewed_by'     => $userName,
            'reviewed_at'     => date('Y-m-d H:i:s'),
        ]);

        // Save URL corrections as overrides
        $overridesApplied = 0;
        foreach ($corrections as $c) {
            if (empty($c['url']) || empty($c['field']) || empty($c['override'])) continue;

            // Upsert: update if exists, insert if not
            $existing = $this->db->fetchAssociative(
                'SELECT id FROM user_overrides WHERE url = ? AND field = ?',
                [$c['url'], $c['field']]
            );
            if ($existing) {
                $this->db->update('user_overrides', [
                    'original_value' => $c['original'] ?? null,
                    'override_value' => $c['override'],
                    'reason'         => $c['reason'] ?? null,
                    'overridden_by'  => $userName,
                    'created_at'     => date('Y-m-d H:i:s'),
                ], ['id' => $existing['id']]);
            } else {
                $this->db->insert('user_overrides', [
                    'url'            => $c['url'],
                    'field'          => $c['field'],
                    'original_value' => $c['original'] ?? null,
                    'override_value' => $c['override'],
                    'reason'         => $c['reason'] ?? null,
                    'overridden_by'  => $userName,
                    'created_at'     => date('Y-m-d H:i:s'),
                ]);
            }

            // Also update the live page_crawl_snapshots row immediately
            $tables = $this->db->fetchFirstColumn(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'page_crawl_snapshots'"
            );
            if (!empty($tables)) {
                $allowedFields = ['page_type', 'has_central_entity', 'has_core_link', 'word_count'];
                if (in_array($c['field'], $allowedFields)) {
                    $this->db->executeStatement(
                        "UPDATE page_crawl_snapshots SET {$c['field']} = ? WHERE url = ?",
                        [$c['override'], $c['url']]
                    );
                }
            }

            $overridesApplied++;
        }

        return new JsonResponse([
            'ok'               => true,
            'overrides_applied' => $overridesApplied,
            'message'          => $overridesApplied > 0
                ? "Review saved. {$overridesApplied} override(s) applied — corrections will persist on next crawl."
                : 'Review saved.',
        ]);
    }

    #[Route('/api/rule-reviews', name: 'api_rule_reviews_list', methods: ['GET'])]
    public function listRuleReviews(): JsonResponse
    {
        $reviews = $this->db->fetchAllAssociative(
            'SELECT * FROM rule_reviews ORDER BY reviewed_at DESC LIMIT 50'
        );
        return new JsonResponse($reviews);
    }

    // ─────────────────────────────────────────────
    //  OVERRIDES API
    // ─────────────────────────────────────────────

    #[Route('/api/overrides', name: 'api_overrides_list', methods: ['GET'])]
    public function listOverrides(): JsonResponse
    {
        $overrides = $this->db->fetchAllAssociative(
            'SELECT * FROM user_overrides ORDER BY created_at DESC'
        );
        return new JsonResponse($overrides);
    }

    #[Route('/api/overrides/{id}', name: 'api_overrides_delete', methods: ['DELETE'])]
    public function deleteOverride(int $id): JsonResponse
    {
        $this->db->delete('user_overrides', ['id' => $id]);
        return new JsonResponse(['ok' => true]);
    }

    // ─────────────────────────────────────────────
    //  TASK API
    // ─────────────────────────────────────────────

    #[Route('/api/tasks', name: 'api_tasks_list', methods: ['GET'])]
    public function listTasks(Request $request): JsonResponse
    {
        $status   = $request->query->get('status');
        $assignee = $request->query->get('assignee');
        $sql      = "SELECT * FROM tasks WHERE 1=1";
        $params   = [];
        if ($status) { $sql .= " AND status = ?"; $params[] = $status; }
        if ($assignee) { $sql .= " AND assigned_to = ?"; $params[] = $assignee; }
        $sql .= " ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END, created_at DESC";
        return new JsonResponse($this->db->fetchAllAssociative($sql, $params));
    }

    #[Route('/api/tasks', name: 'api_tasks_create', methods: ['POST'])]
    public function createTask(Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true);
        $this->db->insert('tasks', [
            'title'           => $body['title'] ?? 'Untitled Task',
            'description'     => $body['description'] ?? null,
            'rule_id'         => $body['rule_id'] ?? null,
            'assigned_to'     => $body['assigned_to'] ?? null,
            'assigned_role'   => $body['assigned_role'] ?? null,
            'status'          => 'pending',
            'priority'        => $body['priority'] ?? 'medium',
            'estimated_hours' => $body['estimated_hours'] ?? 1,
            'logged_hours'    => 0,
            'due_date'        => $body['due_date'] ?? null,
            'recheck_type'    => $body['recheck_type'] ?? null,
            'created_at'      => date('Y-m-d H:i:s'),
        ]);
        $id   = $this->db->lastInsertId();
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
        return new JsonResponse($task, 201);
    }

    #[Route('/api/tasks/{id}/complete', name: 'api_tasks_complete', methods: ['POST'])]
    public function completeTask(int $id): JsonResponse
    {
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
        if (!$task) return new JsonResponse(['error' => 'Task not found'], 404);
        $recheckDays = match($task['recheck_type']) {
            '404_fix', 'sitemap_fix'                     => 7,
            'cannibalization_fix', 'homepage_cannibalization',
            'intent_mismatch', 'weak_page', 'zero_click' => 14,
            'ranking_drop'                               => 28,
            default                                      => 14,
        };
        $recheckDate = date('Y-m-d', strtotime("+{$recheckDays} days"));
        $this->db->update('tasks', ['status' => 'done', 'completed_at' => date('Y-m-d H:i:s'), 'recheck_date' => $recheckDate], ['id' => $id]);
        return new JsonResponse(['task' => $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]), 'recheck_date' => $recheckDate, 'recheck_days' => $recheckDays]);
    }

    #[Route('/api/tasks/{id}/status', name: 'api_tasks_status', methods: ['POST'])]
    public function updateTaskStatus(int $id, Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true);
        $this->db->update('tasks', ['status' => $body['status'] ?? 'pending'], ['id' => $id]);
        return new JsonResponse($this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]));
    }

    #[Route('/api/tasks/{id}/log-time', name: 'api_tasks_log_time', methods: ['POST'])]
    public function logTime(int $id, Request $request): JsonResponse
    {
        $body  = json_decode($request->getContent(), true);
        $hours = floatval($body['hours'] ?? 0);
        if ($hours <= 0) return new JsonResponse(['error' => 'Hours must be positive'], 400);
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
        if (!$task) return new JsonResponse(['error' => 'Task not found'], 404);
        $this->db->update('tasks', ['logged_hours' => floatval($task['logged_hours'] ?? 0) + $hours], ['id' => $id]);
        return new JsonResponse($this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]));
    }

    #[Route('/api/tasks/{id}/verify', name: 'api_tasks_verify', methods: ['POST'])]
    public function verifyTask(int $id, Request $request): JsonResponse
    {
        $body   = json_decode($request->getContent(), true);
        $passed = $body['passed'] ?? false;
        $this->db->update('tasks', ['recheck_verified' => true, 'recheck_result' => $passed ? 'pass' : 'fail'], ['id' => $id]);
        return new JsonResponse($this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]));
    }

    #[Route('/api/rechecks', name: 'api_rechecks', methods: ['GET'])]
    public function listRechecks(): JsonResponse
    {
        return new JsonResponse($this->db->fetchAllAssociative(
            "SELECT * FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false ORDER BY recheck_date ASC"
        ));
    }

    // ─────────────────────────────────────────────
    //  HELPERS
    // ─────────────────────────────────────────────

    private function generateTitle(string $firstMessage): string
    {
        if (!$firstMessage) return 'New conversation';
        $clean = preg_replace('/\s+/', ' ', trim($firstMessage));
        return strlen($clean) > 60 ? substr($clean, 0, 57) . '...' : $clean;
    }

    private function loadCrawlData(): array
    {
        try {
            $tables = $this->db->fetchFirstColumn(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'page_crawl_snapshots'"
            );
            if (empty($tables)) return [];

            return $this->db->fetchAllAssociative(
                "SELECT url, page_type, has_central_entity, central_entity_count, has_core_link,
                        core_links_found, word_count, h1, title_tag, h1_matches_title, h2s,
                        schema_types, is_noindex, crawled_at
                 FROM page_crawl_snapshots
                 WHERE crawled_at >= (SELECT MAX(crawled_at) - INTERVAL '1 hour' FROM page_crawl_snapshots)
                 ORDER BY page_type, word_count DESC
                 LIMIT 150"
            );
        } catch (\Exception $e) { return []; }
    }

    private function loadRecentReviews(): array
    {
        try {
            $tables = $this->db->fetchFirstColumn(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'rule_reviews'"
            );
            if (empty($tables)) return [];
            return $this->db->fetchAllAssociative(
                "SELECT rule_id, verdict, feedback, reviewed_by, reviewed_at FROM rule_reviews ORDER BY reviewed_at DESC LIMIT 10"
            );
        } catch (\Exception $e) { return []; }
    }

    private function loadOverrideCount(): int
    {
        try {
            $tables = $this->db->fetchFirstColumn(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'user_overrides'"
            );
            if (empty($tables)) return 0;
            return (int) $this->db->fetchOne('SELECT COUNT(*) FROM user_overrides');
        } catch (\Exception $e) { return 0; }
    }

    // ─────────────────────────────────────────────
    //  SYSTEM PROMPT BUILDER
    // ─────────────────────────────────────────────

    private function buildSystemPrompt(
        array $semrush, array $topQueries, array $topPages,
        string $userName, string $userRole,
        array $activeTasks, array $pendingRechecks,
        array $topQueries90d = [], array $pageAggregates = [],
        array $brandedQueries = [], array $cannibalizationCandidates = [],
        array $previousPages = [], array $landingPages = [],
        array $adsCampaigns = [], array $adsKeywords = [],
        array $adsSearchTerms = [], array $adsDailySpend = [],
        array $recentReviews = [], int $overrideCount = 0, array $crawlData = []
    ): string {
        $date = date('l, F j, Y');

        $querySummary = '';
        foreach (array_slice($topQueries, 0, 10) as $row) {
            $querySummary .= '- "' . $row['query'] . '" | Page: ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Position: ' . round($row['position'], 1) . "\n";
        }

        $pageSummary = '';
        foreach (array_slice($topPages, 0, 10) as $row) {
            $engTime = isset($row['avg_engagement_time']) ? ' | Engagement: ' . round($row['avg_engagement_time'], 0) . 's' : '';
            $pageSummary .= '- ' . $row['page_path'] . ' | Sessions: ' . $row['sessions'] . ' | Pageviews: ' . $row['pageviews'] . ' | Conversions: ' . $row['conversions'] . $engTime . "\n";
        }

        $taskContext = '';
        if (!empty($activeTasks)) {
            $taskContext .= "\n\nACTIVE TASKS IN SYSTEM:\n";
            $workload = [];
            $overdueNudges = [];
            foreach ($activeTasks as $t) {
                $logged   = floatval($t['logged_hours'] ?? 0);
                $est      = floatval($t['estimated_hours'] ?? 0);
                $assignee = $t['assigned_to'] ?? 'Unassigned';
                if ($assignee !== 'Unassigned' && $t['status'] !== 'done') {
                    $workload[$assignee] = ($workload[$assignee] ?? 0) + $est;
                }
                if ($logged > $est && $est > 0 && $t['status'] !== 'done') {
                    $overdueNudges[] = "⚠️ OVER-ESTIMATE: \"{$t['title']}\" — {$logged}h logged vs {$est}h estimated. Assigned: {$assignee}";
                }
                $taskContext .= '- [' . strtoupper($t['priority']) . '] ' . $t['title'] . ' | Assigned: ' . $assignee . ' | Status: ' . $t['status'] . ' | ' . $est . "h est\n";
            }
            $taskContext .= "\nTEAM WORKLOAD:\n";
            foreach (['Brook', 'Kalib', 'Brad'] as $name) {
                $load   = $workload[$name] ?? 0;
                $status = $load > 40 ? '🔴 OVERLOADED' : ($load > 30 ? '🟡 HIGH' : '🟢 OK');
                $taskContext .= "- {$name}: {$load}h | {$status}\n";
            }
            if (!empty($overdueNudges)) {
                $taskContext .= "\nTASKS EXCEEDING ESTIMATES:\n" . implode("\n", $overdueNudges) . "\n";
            }
        }

        $recheckContext = '';
        if (!empty($pendingRechecks)) {
            $recheckContext .= "\n\nPENDING VERIFICATION RECHECKS:\n";
            foreach ($pendingRechecks as $r) {
                $recheckContext .= '- ' . $r['title'] . ' | Due: ' . $r['recheck_date'] . ' | Type: ' . ($r['recheck_type'] ?? 'general') . ' | Assigned: ' . ($r['assigned_to'] ?? 'Unassigned') . "\n";
            }
        }

        // Rule review context — so Logiri knows what's been approved/disputed
        $reviewContext = '';
        if (!empty($recentReviews)) {
            $reviewContext .= "\n\nRECENT RULE REVIEWS (last 30 days):\n";
            foreach ($recentReviews as $r) {
                $reviewContext .= '- ' . $r['rule_id'] . ' | Verdict: ' . $r['verdict'] . ' | By: ' . $r['reviewed_by'] . ' on ' . substr($r['reviewed_at'], 0, 10);
                if ($r['feedback']) $reviewContext .= ' | Note: ' . $r['feedback'];
                $reviewContext .= "\n";
            }
        }
        if ($overrideCount > 0) {
            $reviewContext .= "\nACTIVE USER OVERRIDES: {$overrideCount} manual classification corrections are in effect. These persist across crawls.\n";
        }

        $promptFile  = dirname(__DIR__, 2) . '/system-prompt.txt';
        $staticRules = file_exists($promptFile) ? file_get_contents($promptFile) : '';

        $intro  = "You are Logiri, the AI Chief of Staff for Double D Trailers (doubledtrailers.com).";
        $intro .= "\n\nYOUR PERSONA & BEHAVIOR:";
        $intro .= "\n- Authoritative, direct, action-oriented Project Manager and Chief of Staff.";
        $intro .= "\n- Lead with the MOST URGENT items first. Address the user by name.";
        $intro .= "\n- Always connect SEO findings to ACTIONABLE TASKS with specific owners.";
        $intro .= "\n- When referencing previous rule reviews, acknowledge what was approved or corrected.";
        $intro .= "\n\nCRITICAL TECHNICAL NOTE:";
        $intro .= "\n- This is a Symfony application. ALWAYS use `php bin/console` for commands. NEVER say `php artisan` — that is Laravel, not this app.";
        $intro .= "\n- Correct: `php bin/console app:crawl-pages`";
        $intro .= "\n- Wrong: `php artisan app:crawl-pages`";
        $intro .= "\n\nH1 NOTE: Some Core pages (e.g. /bumper-pull-horse-trailers/, /gooseneck-horse-trailers/) have no H1 tag. This is a confirmed on-page issue, not a crawl data error. Flag these as FC-R7 violations and assign fixes to Brook.";
        $intro .= "\n\nTASK GENERATION RULES:";
        $intro .= "\n- Generate tasks for every SEO issue you identify.";
        $intro .= "\n- Each task: title, assigned_to, priority (critical/high/medium/low), estimated_hours, recheck_type, description.";
        $intro .= "\n- Do NOT duplicate tasks already in ACTIVE TASKS.";
        $intro .= "\n- At the END of every response include:";
        $intro .= "\n<!-- TASKS_JSON -->";
        $intro .= "\n[{\"title\":\"Example\",\"assigned_to\":\"Brook\",\"priority\":\"high\",\"estimated_hours\":2,\"recheck_type\":null,\"description\":\"Example\"}]";
        $intro .= "\n<!-- /TASKS_JSON -->";
        $intro .= "\n\nCRITICAL: Include <!-- TASKS_JSON --> in EVERY response. Use [] if no tasks needed.";
        $intro .= "\n\nFOUNDATIONAL CONTENT RULES — RUN AUTOMATICALLY ON EVERY BRIEFING:";
        $intro .= "\nYou MUST evaluate ALL of the following rules on every briefing if crawl data is available. Do not wait for the user to ask. For each rule that has violations, output the findings AND a review card so the user can verify your classification logic.";
        $intro .= "\n\nFC-R1: Every indexed page must contain the central entity 'horse trailer' in the body text. Flag pages where has_central_entity = FALSE.";
        $intro .= "\nFC-R2: Every page must be classified as Core or Outer. Flag unclassified pages.";
        $intro .= "\nFC-R3: Core pages must have at least 500 words. Flag Core pages where word_count < 500.";
        $intro .= "\nFC-R5: Every Outer page must link to at least one Core page. Flag Outer pages where has_core_link = FALSE.";
        $intro .= "\nFC-R6: Core pages must have at least 800 words. Flag Core pages where word_count < 800.";
        $intro .= "\nFC-R7: Every indexed page must have an H1 tag that matches or closely reflects the title tag. Flag pages where h1_matches_title = FALSE or h1 is empty.";
        $intro .= "\nFC-R8: Core pages must have at least one H2 tag. Flag Core pages where h2s is empty.";
        $intro .= "\nFC-R9: Core pages must have schema markup. Flag Core pages where schema_types is empty.";
        $intro .= "\nFC-R10: High-traffic Outer pages (100+ GSC impressions) must link to a Core page. Cross-reference GSC impressions with has_core_link = FALSE.";
        $intro .= "\n\nREVIEW CARD FORMAT:";
        $intro .= "\nAfter presenting findings for each rule, ALWAYS append a review card in this exact format:";
        $intro .= "\n<!-- REVIEW_CARD rule_id=\"FC-RX\" -->";
        $intro .= "\nClassification criteria used: [explain how you identified violations for this rule]";
        $intro .= "\n<!-- /REVIEW_CARD -->";
        $intro .= "\nThe UI renders this as an interactive form where Brook can approve, dispute, or correct your findings. This is how you learn. Always include it.";
        $intro .= "\n\nTEAM ROSTER:";
        $intro .= "\n- Brook | SEO + Content | 40h/week";
        $intro .= "\n- Kalib | Sales | 40h/week";
        $intro .= "\n- Brad | Marketing | 40h/week";
        $intro .= "\n\nToday: " . $date;
        $intro .= "\nCurrent user: " . $userName . " | Role: " . $userRole;
        $intro .= "\n\nSEMrush: Keywords=" . ($semrush['organic_keywords'] ?? 'N/A') . " | Traffic=" . ($semrush['organic_traffic'] ?? 'N/A') . " | Updated=" . ($semrush['fetched_at'] ?? 'N/A');
        $intro .= "\n\nTop GSC Queries (28d):\n" . $querySummary;
        $intro .= "\nTop GA4 Pages (28d):\n" . $pageSummary;

        if (!empty($topQueries90d)) {
            $intro .= "\n\n90-DAY GSC TRENDS:\n";
            foreach (array_slice($topQueries90d, 0, 10) as $row) {
                $intro .= '- "' . $row['query'] . '" | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Position: ' . round($row['position'], 1) . "\n";
            }
        }
        if (!empty($pageAggregates)) {
            $intro .= "\n\nGSC PAGE AGGREGATES:\n";
            foreach (array_slice($pageAggregates, 0, 10) as $row) {
                $intro .= '- ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | CTR: ' . round($row['ctr'] * 100, 1) . '% | Position: ' . round($row['position'], 1) . "\n";
            }
        }
        if (!empty($brandedQueries)) {
            $intro .= "\n\nBRANDED QUERIES:\n";
            foreach (array_slice($brandedQueries, 0, 8) as $row) {
                $intro .= '- "' . $row['query'] . '" | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . "\n";
            }
        }
        if (!empty($cannibalizationCandidates)) {
            $intro .= "\n\nCANNIBALIZATION CANDIDATES:\n";
            foreach (array_slice($cannibalizationCandidates, 0, 10) as $row) {
                $intro .= '- "' . $row['query'] . '" → ' . $row['page_count'] . ' pages | Impressions: ' . $row['total_impressions'] . "\n";
            }
        }
        if (!empty($previousPages)) {
            $intro .= "\n\nGA4 PERIOD COMPARISON (28d vs previous):\n";
            $prevLookup = [];
            foreach ($previousPages as $p) { $prevLookup[$p['page_path']] = $p; }
            foreach (array_slice($topPages, 0, 10) as $current) {
                $prev         = $prevLookup[$current['page_path']] ?? null;
                $sessionDelta = $prev ? ($current['sessions'] - $prev['sessions']) : 'N/A';
                $convDelta    = $prev ? ($current['conversions'] - ($prev['conversions'] ?? 0)) : 'N/A';
                $intro .= '- ' . $current['page_path'] . ' | Sessions: ' . $current['sessions'] . ' (Δ' . $sessionDelta . ') | Conversions: ' . $current['conversions'] . ' (Δ' . $convDelta . ")\n";
            }
        }
        if (!empty($landingPages)) {
            $intro .= "\n\nTOP LANDING PAGES:\n";
            foreach (array_slice($landingPages, 0, 8) as $row) {
                $intro .= '- ' . $row['page_path'] . ' | Sessions: ' . $row['sessions'] . ' | Bounce: ' . round($row['bounce_rate'] * 100, 1) . '% | Engagement: ' . round($row['avg_engagement_time'], 0) . 's | Conversions: ' . $row['conversions'] . "\n";
            }
        }
        if (!empty($adsCampaigns)) {
            $intro .= "\n\nGOOGLE ADS CAMPAIGNS (30d):\n";
            foreach ($adsCampaigns as $row) {
                $intro .= '- ' . $row['campaign_name'] . ' | Spend: $' . number_format($row['cost_micros'] / 1000000, 2) . ' | Clicks: ' . $row['clicks'] . ' | CPC: $' . number_format($row['average_cpc'] / 1000000, 2) . ' | Conv: ' . $row['conversions'] . "\n";
            }
        }
        if (!empty($adsKeywords)) {
            $intro .= "\n\nTOP ADS KEYWORDS (30d):\n";
            foreach (array_slice($adsKeywords, 0, 8) as $row) {
                $intro .= '- "' . $row['keyword'] . '" [' . $row['match_type'] . '] | Spend: $' . number_format($row['cost_micros'] / 1000000, 2) . ' | CPC: $' . number_format($row['average_cpc'] / 1000000, 2) . ' | Conv: ' . $row['conversions'] . "\n";
            }
        }
        if (!empty($adsSearchTerms)) {
            $intro .= "\n\nTOP SEARCH TERMS TRIGGERING ADS:\n";
            foreach (array_slice($adsSearchTerms, 0, 8) as $row) {
                $intro .= '- "' . $row['search_term'] . '" | Clicks: ' . $row['clicks'] . ' | Spend: $' . number_format($row['cost_micros'] / 1000000, 2) . "\n";
            }
        }
        if (!empty($adsDailySpend)) {
            $totalSpend = array_sum(array_column($adsDailySpend, 'cost_micros')) / 1000000;
            $intro .= "\n\nGOOGLE ADS TOTAL SPEND (last 14d): $" . number_format($totalSpend, 2) . "\n";
        }

        $intro .= $taskContext;
        $intro .= $recheckContext;
        $intro .= $reviewContext;

        // ── Crawl data summary ──
        if (!empty($crawlData)) {
            $crawledAt = $crawlData[0]['crawled_at'] ?? 'unknown';
            $intro .= "\n\nPAGE CRAWL DATA (last crawl: {$crawledAt}):\n";
            $intro .= "Format: URL | Type | Words | Entity(count) | CoreLink | H1Match | Schema | Noindex | H1\n";
            foreach ($crawlData as $row) {
                $entity   = $row['has_central_entity'] ? "YES({$row['central_entity_count']})" : 'NO';
                $coreLink = $row['has_core_link'] ? 'YES' : 'NO';
                $h1match  = $row['h1_matches_title'] ? 'YES' : 'NO';
                $schema   = $row['schema_types'] && $row['schema_types'] !== '[]' ? implode(',', json_decode($row['schema_types'], true)) : 'none';
                $noindex  = $row['is_noindex'] ? 'NOINDEX' : 'indexed';
                $h1       = substr($row['h1'] ?? '(no h1)', 0, 60);
                $intro .= "- {$row['url']} | {$row['page_type']} | {$row['word_count']}w | entity:{$entity} | corelink:{$coreLink} | h1match:{$h1match} | schema:{$schema} | {$noindex} | \"{$h1}\"\n";
            }

            // Rule violation summaries for quick Logiri parsing
            $noEntity   = array_filter($crawlData, fn($r) => !$r['has_central_entity'] && !$r['is_noindex']);
            $noCoreLink = array_filter($crawlData, fn($r) => $r['page_type'] === 'outer' && !$r['has_core_link'] && !$r['is_noindex']);
            $thinCore   = array_filter($crawlData, fn($r) => $r['page_type'] === 'core' && $r['word_count'] < 500 && !$r['is_noindex']);
            $noH2Core   = array_filter($crawlData, fn($r) => $r['page_type'] === 'core' && ($r['h2s'] === '[]' || !$r['h2s']) && !$r['is_noindex']);
            $h1Mismatch = array_filter($crawlData, fn($r) => !$r['h1_matches_title'] && !$r['is_noindex']);
            $noSchema   = array_filter($crawlData, fn($r) => $r['page_type'] === 'core' && ($r['schema_types'] === '[]' || !$r['schema_types']) && !$r['is_noindex']);

            $intro .= "\nCRAWL RULE VIOLATION SUMMARY:\n";
            $intro .= "FC-R1 (no central entity): " . count($noEntity) . " pages — " . implode(', ', array_column(array_slice($noEntity, 0, 5), 'url')) . "\n";
            $intro .= "FC-R5 (outer missing core link): " . count($noCoreLink) . " pages — " . implode(', ', array_column(array_slice($noCoreLink, 0, 5), 'url')) . "\n";
            $intro .= "FC-R3/R6 (thin core <500w): " . count($thinCore) . " pages — " . implode(', ', array_column(array_slice($thinCore, 0, 5), 'url')) . "\n";
            $intro .= "FC-R8 (core missing H2s): " . count($noH2Core) . " pages — " . implode(', ', array_column(array_slice($noH2Core, 0, 5), 'url')) . "\n";
            $intro .= "FC-R7 (H1/title mismatch): " . count($h1Mismatch) . " pages — " . implode(', ', array_column(array_slice($h1Mismatch, 0, 5), 'url')) . "\n";
            $intro .= "FC-R9 (core missing schema): " . count($noSchema) . " pages — " . implode(', ', array_column(array_slice($noSchema, 0, 5), 'url')) . "\n";
        } else {
            $intro .= "\n\nPAGE CRAWL DATA: No crawl data available. Run php bin/console app:crawl-pages to populate.\n";
        }

        $intro .= "\n\n" . $staticRules;

        return $intro;
    }
}