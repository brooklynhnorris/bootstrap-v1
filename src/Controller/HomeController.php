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
            $this->db->executeStatement('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recheck_days INT DEFAULT NULL');
            $this->db->executeStatement('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recheck_criteria TEXT DEFAULT NULL');
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
            "SELECT * FROM tasks WHERE status != 'done' ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END, created_at DESC LIMIT 10"
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
             ORDER BY updated_at DESC LIMIT 10",
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
            "SELECT query, page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE date_range = '28d' ORDER BY impressions DESC LIMIT 10"
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

        $payload = json_encode([
            'model'      => $claudeModel,
            'max_tokens' => 4096,
            'system'     => $systemPrompt,
            'messages'   => $claudeMessages,
        ]);

        $ch = curl_init('https://api.anthropic.com/v1/messages');
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $payload,
            CURLOPT_HTTPHEADER     => [
                'Content-Type: application/json',
                'x-api-key: ' . $claudeKey,
                'anthropic-version: 2023-06-01',
            ],
            CURLOPT_TIMEOUT        => 90,
            CURLOPT_CONNECTTIMEOUT => 10,
        ]);
        $response = curl_exec($ch);
        $curlError = curl_error($ch);
        curl_close($ch);

        if ($curlError) {
            return new JsonResponse(['error' => 'API connection failed: ' . $curlError], 500);
        }

        $data = json_decode($response, true);

        if (isset($data['error'])) {
            return new JsonResponse(['error' => $data['error']['message']], 500);
        }

        $text = $data['content'][0]['text'] ?? 'No response from Claude.';

        // ── Auto-create tasks ──
        $tasksCreated = [];
        if (preg_match('/<!-- TASKS_JSON -->\s*(.*?)\s*<!-- \/TASKS_JSON -->/s', $text, $matches)) {
            $aiTasks = json_decode(trim($matches[1]), true);
            // Hard cap: never exceed 30 active tasks total
            $activeCount = $this->db->fetchOne("SELECT COUNT(*) FROM tasks WHERE status != 'done'");
            if (is_array($aiTasks) && $activeCount < 30) {
                foreach ($aiTasks as $aiTask) {
                    $title = $aiTask['title'] ?? '';
                    if (!$title) continue;
                    // Dedup: skip if same title exists, OR if same URL+rule combo exists
                    $existing = $this->db->fetchAssociative(
                        "SELECT id FROM tasks WHERE title = ? AND status != 'done' LIMIT 1", [$title]
                    );
                    if ($existing) continue;
                    // Also skip near-duplicates by checking if title contains same URL and rule
                    $urlMatch = preg_match('|(/[^/]+/)|u', $title, $urlParts) ? $urlParts[1] : null;
                    if ($urlMatch) {
                        $nearDup = $this->db->fetchAssociative(
                            "SELECT id FROM tasks WHERE title LIKE ? AND title LIKE ? AND status != 'done' LIMIT 1",
                            ['%' . $urlMatch . '%', '%' . substr($title, 0, 15) . '%']
                        );
                        if ($nearDup) continue;
                    }
                    $priority = in_array($aiTask['priority'] ?? '', ['critical','high','medium','low']) ? $aiTask['priority'] : 'medium';
                    $this->db->insert('tasks', [
                        'title'            => $title,
                        'description'      => $aiTask['description'] ?? null,
                        'assigned_to'      => $aiTask['assigned_to'] ?? null,
                        'assigned_role'    => $aiTask['role'] ?? null,
                        'status'           => 'pending',
                        'priority'         => $priority,
                        'estimated_hours'  => floatval($aiTask['estimated_hours'] ?? 1),
                        'logged_hours'     => 0,
                        'recheck_type'     => $aiTask['recheck_type'] ?? null,
                        'recheck_days'     => isset($aiTask['recheck_days']) ? intval($aiTask['recheck_days']) : null,
                        'recheck_criteria' => $aiTask['recheck_criteria'] ?? null,
                        'created_at'       => date('Y-m-d H:i:s'),
                    ]);
                    $newTask = $this->db->fetchAssociative('SELECT id, title, priority, assigned_to, estimated_hours, recheck_type FROM tasks WHERE title = ? AND status != ? LIMIT 1', [$title, 'done']);
                    $tasksCreated[] = $newTask ?: ['title' => $title];
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

    #[Route('/api/conversations/{id}/delete', name: 'api_conversation_delete', methods: ['POST'])]
    public function deleteConversation(int $id): JsonResponse
    {
        $this->db->executeStatement('DELETE FROM messages WHERE conversation_id = ?', [$id]);
        $this->db->executeStatement('DELETE FROM conversations WHERE id = ?', [$id]);
        return new JsonResponse(['ok' => true]);
    }

    #[Route('/api/conversations/{id}/rename', name: 'api_conversation_rename', methods: ['POST'])]
    public function renameConversation(int $id, Request $request): JsonResponse
    {
        $body  = json_decode($request->getContent(), true);
        $title = trim($body['title'] ?? '');
        if (!$title) {
            return new JsonResponse(['error' => 'title required'], 400);
        }
        $this->db->executeStatement(
            'UPDATE conversations SET title = ?, updated_at = ? WHERE id = ?',
            [$title, date('Y-m-d H:i:s'), $id]
        );
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

    #[Route('/api/tasks/{id}', name: 'api_task_single', methods: ['GET'])]
    public function getTask(int $id): JsonResponse
    {
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
        if (!$task) return new JsonResponse(['error' => 'Not found'], 404);
        return new JsonResponse($task);
    }

    #[Route('/api/tasks/{id}/complete', name: 'api_tasks_complete', methods: ['POST'])]
    public function completeTask(int $id, Request $request): JsonResponse
    {
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
        if (!$task) return new JsonResponse(['error' => 'Task not found'], 404);

        $body = json_decode($request->getContent(), true) ?: [];

        // Allow caller to override recheck days; otherwise derive from type
        if (isset($body['recheck_days']) && intval($body['recheck_days']) > 0) {
            $recheckDays = intval($body['recheck_days']);
        } else {
            $recheckDays = match($task['recheck_type']) {
                '404_fix', 'sitemap_fix'                  => 7,
                'h1_fix', 'h2_fix', 'schema_fix',
                'core_link_fix', 'on_page_fix'            => 14,
                'ranking_drop'                            => 28,
                default                                   => 14,
            };
        }

        $recheckCriteria = $body['recheck_criteria'] ?? $task['recheck_criteria'] ?? null;
        $recheckDate     = date('Y-m-d', strtotime("+{$recheckDays} days"));

        $this->db->update('tasks', [
            'status'           => 'done',
            'completed_at'     => date('Y-m-d H:i:s'),
            'recheck_date'     => $recheckDate,
            'recheck_days'     => $recheckDays,
            'recheck_criteria' => $recheckCriteria,
        ], ['id' => $id]);

        return new JsonResponse([
            'task'             => $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]),
            'recheck_date'     => $recheckDate,
            'recheck_days'     => $recheckDays,
            'recheck_criteria' => $recheckCriteria,
        ]);
    }

    #[Route('/api/tasks/{id}/recheck-date', name: 'api_tasks_recheck_date', methods: ['POST'])]
    public function updateRecheckDate(int $id, Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true) ?: [];
        $days = intval($body['days'] ?? 0);
        if ($days <= 0) return new JsonResponse(['error' => 'days must be positive'], 400);
        $recheckDate = date('Y-m-d', strtotime("+{$days} days"));
        $this->db->update('tasks', [
            'recheck_date'     => $recheckDate,
            'recheck_days'     => $days,
            'recheck_criteria' => $body['criteria'] ?? null,
        ], ['id' => $id]);
        return new JsonResponse(['recheck_date' => $recheckDate, 'recheck_days' => $days]);
    }

    #[Route('/api/tasks/clear-done', name: 'api_tasks_clear_done', methods: ['POST'])]
    public function clearDoneTasks(): JsonResponse
    {
        $this->db->executeStatement("DELETE FROM tasks WHERE status = 'done'");
        return new JsonResponse(['ok' => true]);
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

            // Only send pages that have at least one FC rule violation — keeps prompt small
            return $this->db->fetchAllAssociative(
                "SELECT url, page_type, has_central_entity, has_core_link,
                        word_count, h1, title_tag, h1_matches_title, h2s,
                        schema_types, is_noindex
                 FROM page_crawl_snapshots
                 WHERE crawled_at >= (SELECT MAX(crawled_at) - INTERVAL '1 hour' FROM page_crawl_snapshots)
                   AND (
                     has_central_entity = FALSE
                     OR (page_type = 'Core' AND word_count < 800)
                     OR h1_matches_title = FALSE
                     OR (page_type = 'Core' AND (h2s IS NULL OR h2s = '' OR h2s = '[]'))
                     OR (page_type = 'Core' AND (schema_types IS NULL OR schema_types = '' OR schema_types = '[]'))
                     OR (page_type = 'Outer' AND has_core_link = FALSE)
                   )
                 ORDER BY page_type, url
                 LIMIT 50"
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
        $intro .= "\n\nLOGIRI VOCABULARY — NEVER USE GENERIC SEO TOOL LANGUAGE:";
        $intro .= "\nLogiri has its own terminology. Always use the Logiri term, never the generic SEO tool equivalent.";
        $intro .= "\n| Generic Term              | Logiri Term                  |";
        $intro .= "\n|---------------------------|------------------------------|";
        $intro .= "\n| SEO Tool / Analyzer       | Signal Engine / Detection Engine |";
        $intro .= "\n| Issue / Problem           | Signal                       |";
        $intro .= "\n| Action / Task             | Play                         |";
        $intro .= "\n| Recommendation            | Remediation / Playbook Step  |";
        $intro .= "\n| Alert                     | Incident                     |";
        $intro .= "\n| Dashboard                 | Command Center               |";
        $intro .= "\n| Audit                     | Sweep                        |";
        $intro .= "\n| Monitoring                | Pulse                        |";
        $intro .= "\n| Automation                | Runbook                      |";
        $intro .= "\nExamples: Say \"Logiri detected a Signal on /page/\" not \"Logiri found an issue\". Say \"here is your Play\" not \"here is your action item\". Say \"running a Sweep\" not \"running an audit\". Say \"Pulse data shows\" not \"monitoring shows\". Never say \"SEO tool\", \"audit\", \"issues\", \"recommendations\", or \"alerts\" — always substitute the Logiri term.";

        $intro .= "\n\nBRIEFING FORMAT RULES:";
        $intro .= "\n- Structure every briefing with H2 (##) section headings so sections can collapse.";
        $intro .= "\n- Each section must start with a 1-sentence summary line BEFORE any detail.";
        $intro .= "\n- Keep top-level bullets to one line each. Put detail under sub-bullets (indented).";
        $intro .= "\n- Plays (tasks) are called PLAYS not tasks, actions, or recommendations.";
        $intro .= "\n- Never use the words: issue, problem, audit, recommendation, alert, monitoring, dashboard.";
        $intro .= "\n- Use instead: Signal, Play, Sweep, Incident, Pulse, Command Center.";
        $intro .= "\n- Start Task button label: always say 'Run this Play' not 'Start Task'.";

        $intro .= "\n\nCRITICAL TECHNICAL NOTE:";
        $intro .= "\n- This is a Symfony application. ALWAYS use `php bin/console` for commands. NEVER say `php artisan` — that is Laravel, not this app.";
        $intro .= "\n- Correct: `php bin/console app:crawl-pages`";
        $intro .= "\n- Wrong: `php artisan app:crawl-pages`";
        $intro .= "\n\nH1 NOTE: Some Core pages (e.g. /bumper-pull-horse-trailers/, /gooseneck-horse-trailers/) have no H1 tag. This is a confirmed on-page issue, not a crawl data error. Flag these as FC-R7 violations and assign fixes to Brook.";
        $intro .= "\n\nTASK GENERATION RULES — READ CAREFULLY:";
        $intro .= "\n- ONE TASK = ONE URL. Never batch multiple URLs into one task. If 10 pages need H1 fixes, create 10 separate tasks.";
        $intro .= "\n- Task title format: \"Fix [issue] on [url]\" — e.g. \"Add H1 tag to /bumper-pull-horse-trailers/\"";
        $intro .= "\n- Task description: be direct and surgical. Tell the person exactly: (1) what to change, (2) what value to use. No fluff.";
        $intro .= "\n  Example good description: \"H1 is missing. Add: <h1>Bumper Pull Horse Trailers</h1> in the page hero section.\"";
        $intro .= "\n  Example bad description: \"This page has an H1/title mismatch that needs to be resolved to improve SEO signals.\"";
        $intro .= "\n- DEDUPLICATION: Before generating a task, check ACTIVE TASKS below. If a task for that exact URL already exists with the same rule, skip it. Do not recreate it.";
        $intro .= "\n- Only generate tasks for FC rules FC-R1 through FC-R10.";
        $intro .= "\n- Each task must have: title, assigned_to, priority (critical/high/medium/low), estimated_hours (max 0.5h per URL fix), recheck_type, recheck_days, recheck_criteria, description.";
        $intro .= "\n- TASK ASSIGNMENT: On-page fixes → Brook. Rule review tasks → Jeanne.";
        $intro .= "\n- RECHECK DAYS: H1/H2 fixes = 7 days. Internal links = 7 days. Schema = 14 days. Default = 14 days.";
        $intro .= "\n- RECHECK CRITERIA: Plain English. Example: 'H1 tag present and matches title on /url/'";
        $intro .= "\n- At the END of every response include:";
        $intro .= "\n<!-- TASKS_JSON -->";
        $intro .= "\n[{\"title\":\"Add H1 tag to /example/\",\"assigned_to\":\"Brook\",\"priority\":\"critical\",\"estimated_hours\":0.25,\"recheck_type\":\"h1_fix\",\"recheck_days\":7,\"recheck_criteria\":\"H1 tag present and matches title on /example/\",\"description\":\"H1 is missing. Add: <h1>Example Page Title</h1> in the hero section.\"}]";
        $intro .= "\n<!-- /TASKS_JSON -->";
        $intro .= "\n- CRITICAL: Include <!-- TASKS_JSON --> in EVERY response.