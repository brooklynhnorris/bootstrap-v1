<?php

namespace App\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\RequestStack;
use Symfony\Component\Routing\Annotation\Route;
use Doctrine\DBAL\Connection;

class HomeController extends AbstractController
{
    public function __construct(private Connection $db, private RequestStack $requestStack)
    {
        $this->ensureSchema();
    }

    private function ensureSchema(): void
    {
        try {
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS conversations (id SERIAL PRIMARY KEY, user_id INT DEFAULT NULL, title VARCHAR(255) DEFAULT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, is_archived BOOLEAN DEFAULT FALSE, persona_name VARCHAR(50) DEFAULT NULL)');
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, conversation_id INT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE, role VARCHAR(20) NOT NULL, content TEXT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)');
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS rule_reviews (id SERIAL PRIMARY KEY, conversation_id INT DEFAULT NULL REFERENCES conversations(id) ON DELETE SET NULL, rule_id VARCHAR(20) NOT NULL, verdict VARCHAR(30) NOT NULL, feedback TEXT DEFAULT NULL, reviewed_by VARCHAR(100) DEFAULT NULL, reviewed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)');
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS user_overrides (id SERIAL PRIMARY KEY, url TEXT NOT NULL, field VARCHAR(50) NOT NULL, original_value TEXT DEFAULT NULL, override_value TEXT NOT NULL, reason TEXT DEFAULT NULL, overridden_by VARCHAR(100) DEFAULT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(url, field))');
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS activity_log (id SERIAL PRIMARY KEY, actor VARCHAR(100) NOT NULL, action VARCHAR(50) NOT NULL, target_type VARCHAR(50), target_id INT, target_title TEXT, details TEXT, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)');
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS custom_rules (id SERIAL PRIMARY KEY, rule_id VARCHAR(20) NOT NULL UNIQUE, rule_name TEXT NOT NULL, category VARCHAR(100), trigger_condition TEXT, threshold TEXT, diagnosis TEXT, action_output TEXT, priority VARCHAR(20) DEFAULT \'medium\', assigned_to VARCHAR(100), created_by VARCHAR(100), status VARCHAR(20) DEFAULT \'active\', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)');
            $this->db->executeStatement('CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages (conversation_id)');
            $this->db->executeStatement('CREATE INDEX IF NOT EXISTS idx_overrides_url ON user_overrides (url)');
            $this->db->executeStatement('CREATE INDEX IF NOT EXISTS idx_activity_log_created ON activity_log (created_at DESC)');
            $this->db->executeStatement('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recheck_days INT DEFAULT NULL');
            $this->db->executeStatement('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recheck_criteria TEXT DEFAULT NULL');
            $this->db->executeStatement('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS attempt_number INT DEFAULT 1');
            $this->db->executeStatement("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS persona_name VARCHAR(50) DEFAULT NULL");
        } catch (\Exception $e) {
            // Tables already exist or DB not ready — fail silently
        }
    }

    #[Route('/api/admin/clear-slate', name: 'clear_slate', methods: ['POST'])]
    public function clearSlate(): JsonResponse
    {
        try {
            $this->db->executeStatement('DELETE FROM messages');
            $this->db->executeStatement('DELETE FROM conversations');
            $this->db->executeStatement('DELETE FROM tasks');
            return new JsonResponse(['ok' => true, 'message' => 'Slate cleared']);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    #[Route('/api/set-persona', name: 'set_persona', methods: ['POST'])]
    public function setPersona(Request $request): JsonResponse
    {
        $body    = json_decode($request->getContent(), true);
        $name    = $body['name'] ?? null;
        $role    = $body['role'] ?? null;
        $session = $this->requestStack->getSession();

        $allowed = ['Brook', 'Brad', 'Jeanne', 'Kalib'];
        if (!$name || !in_array($name, $allowed)) {
            return new JsonResponse(['error' => 'Invalid persona'], 400);
        }

        $session->remove('active_persona'); // clear old first
        $session->set('active_persona', ['name' => $name, 'role' => $role]);
        return new JsonResponse(['ok' => true, 'name' => $name, 'role' => $role]);
    }

    #[Route('/api/clear-persona', name: 'clear_persona', methods: ['POST'])]
    public function clearPersona(): JsonResponse
    {
        $session = $this->requestStack->getSession();
        $session->remove('active_persona');
        return new JsonResponse(['ok' => true]);
    }

    // ─────────────────────────────────────────────
    //  MAIN PAGE
    // ─────────────────────────────────────────────

    #[Route('/', name: 'home')]
    public function index(): Response
    {
        $user    = $this->getUser();
        $session = $this->requestStack->getSession();

        // Active persona — set via /api/set-persona, stored in session
        // Defaults to the logged-in user's name
        $defaultName = $user ? ($user->getName() ?? explode('@', $user->getEmail())[0]) : 'User';
        $defaultRole = $user ? ($user->getTeamRole() ?? 'Owner') : 'Owner';
        $activePersona = $session->get('active_persona', null);
        $showPersonaPicker = ($activePersona === null);

        $teamMembers = [
            ['name' => 'Brook',  'role' => 'SEO + Content', 'avatar' => 'BR', 'color' => '#3b82f6'],
            ['name' => 'Brad',   'role' => 'Developer',     'avatar' => 'BD', 'color' => '#8b5cf6'],
            ['name' => 'Jeanne', 'role' => 'Owner',         'avatar' => 'JE', 'color' => '#10b981'],
            ['name' => 'Kalib',  'role' => 'Design',        'avatar' => 'KA', 'color' => '#f59e0b'],
        ];

        if ($activePersona) {
            // Find persona data
            $personaData = array_filter($teamMembers, fn($m) => $m['name'] === $activePersona['name']);
            $personaData = reset($personaData);
            $userName = $personaData ? $personaData['name'] : $defaultName;
            $userRole = $personaData ? $personaData['role'] : $defaultRole;
        } else {
            $userName = $defaultName;
            $userRole = $defaultRole;
            // No persona chosen yet — show the picker overlay (handled client-side)
            $showPersonaPicker = true;
        }
        $tasks = $this->db->fetchAllAssociative(
            "SELECT * FROM tasks WHERE status != 'done' AND assigned_to = ? ORDER BY CASE priority WHEN 'critical' THEN 0 WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END, created_at DESC LIMIT 20",
            [$userName]
        );
        $rechecks = $this->db->fetchAllAssociative(
            "SELECT * FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false ORDER BY recheck_date ASC LIMIT 10"
        );
        $taskCounts = $this->db->fetchAssociative(
            "SELECT
                COUNT(*) FILTER (WHERE status = 'pending') as urgent,
                COUNT(*) FILTER (WHERE status = 'in_progress') as active,
                COUNT(*) FILTER (WHERE status = 'done') as done
            FROM tasks WHERE assigned_to = ?",
            [$userName]
        );

        // Load recent conversations for sidebar history
        $userId = $user ? $user->getId() : null;
        $conversations = $this->db->fetchAllAssociative(
            "SELECT id, title, created_at, updated_at FROM conversations
             WHERE user_id = ? AND is_archived = FALSE AND (persona_name = ? OR persona_name IS NULL)
             ORDER BY updated_at DESC LIMIT 10",
            [$userId, $userName]
        );

        return $this->render('home/index.html.twig', [
            'userName'          => $userName,
            'userRole'          => $userRole,
            'userEmail'         => $user ? $user->getEmail() : '',
            'tasks'             => $tasks,
            'rechecks'          => $rechecks,
            'taskCounts'        => $taskCounts ?: ['urgent' => 0, 'active' => 0, 'done' => 0],
            'conversations'     => $conversations,
            'showPersonaPicker' => $showPersonaPicker,
            'teamMembers'       => $teamMembers,
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

        // ── Load verification outcomes for learning context ──
        $verificationResults = $this->loadVerificationResults();
        $ruleFeedback = $this->loadRuleFeedback();
        $ruleProposals = $this->loadRuleProposals();

        // ── Persist conversation ──
        if (!$conversationId) {
            // New conversation — create it
            $firstUserMsg = '';
            foreach ($messages as $msg) {
                if ($msg['role'] === 'user') { $firstUserMsg = $msg['content']; break; }
            }
            $title = $this->generateTitle($firstUserMsg);
            $session      = $this->requestStack->getSession();
            $activePersona = $session->get('active_persona', null);
            $this->db->insert('conversations', [
                'user_id'      => $userId,
                'title'        => $title,
                'persona_name' => $activePersona ? $activePersona['name'] : null,
                'created_at'   => date('Y-m-d H:i:s'),
                'updated_at'   => date('Y-m-d H:i:s'),
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
            $recentReviews, $overrideCount, $crawlData,
            $verificationResults, $ruleFeedback, $ruleProposals
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

        // ── NLP ENTITY VALIDATION ──
        // If the response contains a rewrite (detected by common patterns),
        // run a second Claude call to validate entity-predicate alignment
        $lastUserMsg = '';
        foreach (array_reverse($messages) as $m) {
            if ($m['role'] === 'user') { $lastUserMsg = $m['content']; break; }
        }
        $isRewriteContext = str_contains($lastUserMsg, 'FULL PAGE BODY TEXT') || str_contains($lastUserMsg, 'content rewrite') || str_contains($lastUserMsg, 'WRITE THE COMPLETE REWRITE');

        if ($isRewriteContext && strlen($text) > 200) {
            $nlpResult = $this->validateEntityAlignment($text, $lastUserMsg, $claudeKey);
            if ($nlpResult && !empty($nlpResult['issues'])) {
                // Append NLP validation results to the response
                $text .= "\n\n---\n\n**🔬 NLP Entity Validation:**\n";
                foreach ($nlpResult['issues'] as $issue) {
                    $text .= "- {$issue}\n";
                }
                if (!empty($nlpResult['revised_first_sentence'])) {
                    $text .= "\n**Suggested first sentence revision:**\n> " . $nlpResult['revised_first_sentence'] . "\n";
                }
                $text .= "\n*Entity: " . ($nlpResult['detected_entity'] ?? 'unknown') . " | Matches H1: " . ($nlpResult['matches_h1'] ? 'Yes' : 'No') . " | Subject position: " . ($nlpResult['subject_position'] ?? 'unknown') . "*";
            } elseif ($nlpResult && empty($nlpResult['issues'])) {
                $text .= "\n\n---\n✅ **NLP Validated** — Primary entity: *" . ($nlpResult['detected_entity'] ?? 'unknown') . "* | H1 match: Yes | Subject position: correct";
            }
        }

        // ── Auto-create tasks ──
        $tasksCreated = [];
        if (preg_match('/<!-- TASKS_JSON -->\s*(.*?)\s*<!-- \/TASKS_JSON -->/s', $text, $matches)) {
            $aiTasks = json_decode(trim($matches[1]), true);
            $activeCount = (int)$this->db->fetchOne("SELECT COUNT(*) FROM tasks WHERE status != 'done'");
            if (is_array($aiTasks) && $activeCount < 30) {
                foreach ($aiTasks as $aiTask) {
                    $title = $aiTask['title'] ?? '';
                    if (!$title) continue;
                    $existing = $this->db->fetchAssociative(
                        "SELECT id FROM tasks WHERE title = ? AND status != 'done' LIMIT 1", [$title]
                    );
                    if ($existing) continue;
                    // Also skip if same URL appears in an active task with same rule prefix
                    if (preg_match('|(/[^/ ]+/)|u', $title, $urlParts)) {
                        $urlFrag = $urlParts[1];
                        $rulePrefix = substr($title, 0, 10);
                        $nearDup = $this->db->fetchAssociative(
                            "SELECT id FROM tasks WHERE title LIKE ? AND title LIKE ? AND status != 'done' LIMIT 1",
                            ['%' . $urlFrag . '%', $rulePrefix . '%']
                        );
                        if ($nearDup) continue;
                    }
                    $priority = in_array($aiTask['priority'] ?? '', ['critical','high','medium','low']) ? $aiTask['priority'] : 'medium';
                    $this->db->insert('tasks', [
                        'title'            => $title,
                        'description'      => isset($aiTask['description']) ? strip_tags($aiTask['description']) : null,
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
            // Strip any bare JSON arrays that leaked outside tags (e.g. [{...},...])
            $text = preg_replace('/^\s*\[\s*\{[^\[\]]*"title"[^\[\]]*\}[\s\S]*?\]\s*$/m', '', $text);
            $text = rtrim($text);
        }

        // ── Strip any raw HTML tags the AI accidentally included ──
        $text = preg_replace('/<(h[1-6]|p|br|div|span|a|ul|li|ol|strong|em|b|i)[^>]*>/i', '', $text);
        $text = preg_replace('</(h[1-6]|p|div|span|a|ul|li|ol|strong|em|b|i)>', '', $text);

        // ── Fix truncated/mangled URLs in LLM output ──
        // Common patterns: /umper-pull → /bumper-pull, /ving-quarters → /living-quarters
        // Also fix: doubledtrailers.comumper → doubledtrailers.com/bumper
        $urlFixes = [
            '/umper-pull'       => '/bumper-pull',
            '/iving-quarters'   => '/living-quarters',
            '/ooseneck'         => '/gooseneck',
            '/afetack'          => '/safetack',
            '/rail-blazer'      => '/trail-blazer',
            '/orse-trailer'     => '/horse-trailer',
            '/orse-trailers'    => '/horse-trailers',
            '/traight-load'     => '/straight-load',
            '/lant-load'        => '/slant-load',
            '/ownsmand'         => '/townsmand',
            '/orse-jockeys'     => '/horse-jockeys',
            '/orse-racing'      => '/horse-racing',
            '/ength-in'         => '/length-in',
            '/bout/'            => '/about/',
            '/esources/'        => '/resources/',
        ];
        foreach ($urlFixes as $broken => $fixed) {
            $text = str_ireplace($broken, $fixed, $text);
        }

        // Fix doubledtrailers.com + any letter without slash (catches ALL truncated domain+path joins)
        $text = preg_replace('/doubledtrailers\.com([a-z])/', 'doubledtrailers.com/$1', $text);

        // Also fix inside JSON strings: "url": "https://www.doubledtrailers.com/..." patterns
        // Re-run the same fix to catch URLs inside code blocks and JSON
        $text = preg_replace('/doubledtrailers\.com([a-z])/', 'doubledtrailers.com/$1', $text);

        $text = rtrim($text);

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

        // Log activity
        $this->logActivity($userName, 'reviewed_rule', 'rule', null, $ruleId, "{$verdict}" . ($feedback ? " — {$feedback}" : ''));

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
        try {
        $status   = $request->query->get('status');
        $assignee = $request->query->get('assignee');
        $sql      = "SELECT * FROM tasks WHERE 1=1";
        $params   = [];
        if ($status) { $sql .= " AND status = ?"; $params[] = $status; }
        if ($assignee) { $sql .= " AND assigned_to = ?"; $params[] = $assignee; }
        $sql .= " ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END, created_at DESC";
        return new JsonResponse($this->db->fetchAllAssociative($sql, $params));
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
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

        // Track attempt number
        $attemptNumber = 1;
        try {
            $this->db->executeStatement('CREATE TABLE IF NOT EXISTS task_attempts (
                id SERIAL PRIMARY KEY,
                task_id INT NOT NULL,
                attempt_number INT NOT NULL DEFAULT 1,
                completed_at TIMESTAMP NOT NULL,
                recheck_date DATE,
                recheck_result VARCHAR(20) DEFAULT NULL,
                outcome_summary TEXT DEFAULT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )');
            $this->db->executeStatement('CREATE INDEX IF NOT EXISTS idx_task_attempts_task ON task_attempts (task_id)');

            // If this task was previously done and verified, save that as a past attempt
            if ($task['status'] === 'done' || $task['recheck_verified']) {
                $lastAttempt = $this->db->fetchOne('SELECT MAX(attempt_number) FROM task_attempts WHERE task_id = ?', [$id]);
                $attemptNumber = $lastAttempt ? ((int) $lastAttempt) + 1 : 1;

                // If first completion but there's already a verified result, store attempt 1
                if ($attemptNumber === 1 && $task['recheck_result']) {
                    $this->db->insert('task_attempts', [
                        'task_id'         => $id,
                        'attempt_number'  => 1,
                        'completed_at'    => $task['completed_at'] ?? date('Y-m-d H:i:s'),
                        'recheck_date'    => $task['recheck_date'],
                        'recheck_result'  => $task['recheck_result'],
                        'outcome_summary' => 'Original attempt',
                        'created_at'      => date('Y-m-d H:i:s'),
                    ]);
                    $attemptNumber = 2;
                }
            } else {
                $lastAttempt = $this->db->fetchOne('SELECT MAX(attempt_number) FROM task_attempts WHERE task_id = ?', [$id]);
                $attemptNumber = $lastAttempt ? ((int) $lastAttempt) + 1 : 1;
            }

            // Record this attempt
            $this->db->insert('task_attempts', [
                'task_id'        => $id,
                'attempt_number' => $attemptNumber,
                'completed_at'   => date('Y-m-d H:i:s'),
                'recheck_date'   => $recheckDate,
                'created_at'     => date('Y-m-d H:i:s'),
            ]);
        } catch (\Exception $e) {
            // Non-fatal — attempt tracking is supplementary
        }

        $this->db->update('tasks', [
            'status'            => 'done',
            'completed_at'      => date('Y-m-d H:i:s'),
            'recheck_date'      => $recheckDate,
            'recheck_days'      => $recheckDays,
            'recheck_criteria'  => $recheckCriteria,
            'recheck_verified'  => false,
            'recheck_result'    => null,
            'attempt_number'    => $attemptNumber,
        ], ['id' => $id]);

        // Log activity
        $session = $this->requestStack->getSession();
        $actor   = $session->get('persona_name', 'Unknown');
        $this->logActivity($actor, 'completed_task', 'task', $id, $task['title'] ?? '', "Recheck in {$recheckDays} days");

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
        $body   = json_decode($request->getContent(), true);
        $status = $body['status'] ?? 'pending';
        $task   = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
        $this->db->update('tasks', ['status' => $status], ['id' => $id]);

        // Log activity
        $session = $this->requestStack->getSession();
        $actor   = $session->get('persona_name', 'Unknown');
        $action  = match($status) {
            'in_progress' => 'started_task',
            'done'        => 'completed_task',
            'pending'     => 'reset_task',
            default       => 'updated_task',
        };
        $this->logActivity($actor, $action, 'task', $id, $task['title'] ?? '');

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

    #[Route('/api/tasks/{id}/feedback', name: 'api_task_feedback', methods: ['GET'])]
    public function getTaskFeedback(int $id): JsonResponse
    {
        try {
            $feedback = $this->db->fetchAllAssociative(
                "SELECT * FROM rule_feedback WHERE task_id = :id ORDER BY created_at DESC LIMIT 5",
                ['id' => $id]
            );
            $outcomes = $this->db->fetchAllAssociative(
                "SELECT * FROM rule_outcomes WHERE review_id = :id ORDER BY verified_at DESC LIMIT 5",
                ['id' => $id]
            );
            $attempts = [];
            try {
                $attempts = $this->db->fetchAllAssociative(
                    "SELECT * FROM task_attempts WHERE task_id = :id ORDER BY attempt_number ASC",
                    ['id' => $id]
                );
            } catch (\Exception $e) {}
            return new JsonResponse(['feedback' => $feedback, 'outcomes' => $outcomes, 'attempts' => $attempts]);
        } catch (\Exception $e) {
            return new JsonResponse(['feedback' => [], 'outcomes' => [], 'attempts' => []]);
        }
    }

    #[Route('/api/rule-proposals', name: 'api_rule_proposals', methods: ['GET'])]
    public function listRuleProposals(): JsonResponse
    {
        try {
            $proposals = $this->db->fetchAllAssociative(
                "SELECT * FROM rule_change_proposals WHERE status = 'pending' ORDER BY created_at DESC LIMIT 20"
            );
            return new JsonResponse($proposals);
        } catch (\Exception $e) {
            return new JsonResponse([]);
        }
    }

    #[Route('/api/rule-proposals/{id}/approve', name: 'api_rule_proposal_approve', methods: ['POST'])]
    public function approveRuleProposal(int $id, Request $request): JsonResponse
    {
        try {
            $body = json_decode($request->getContent(), true);
            $approvedBy = $body['approved_by'] ?? 'Unknown';
            $this->db->update('rule_change_proposals', [
                'status'      => 'approved',
                'approved_by' => $approvedBy,
                'approved_at' => date('Y-m-d H:i:s'),
            ], ['id' => $id]);

            // Also mark related rule_feedback entries as approved
            $proposal = $this->db->fetchAssociative('SELECT rule_id FROM rule_change_proposals WHERE id = ?', [$id]);
            if ($proposal) {
                $this->db->executeStatement(
                    "UPDATE rule_feedback SET change_approved = TRUE, approved_by = :by, approved_at = NOW() WHERE rule_id = :rule AND change_approved IS NULL",
                    ['by' => $approvedBy, 'rule' => $proposal['rule_id']]
                );
            }

            return new JsonResponse(['status' => 'approved']);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    #[Route('/api/rule-proposals/{id}/reject', name: 'api_rule_proposal_reject', methods: ['POST'])]
    public function rejectRuleProposal(int $id, Request $request): JsonResponse
    {
        try {
            $body = json_decode($request->getContent(), true);
            $feedback = $body['feedback'] ?? null;
            $this->db->update('rule_change_proposals', [
                'status'      => 'rejected',
                'approved_by' => $body['approved_by'] ?? 'Unknown',
                'approved_at' => date('Y-m-d H:i:s'),
            ], ['id' => $id]);

            // Store rejection feedback for the learning loop
            if ($feedback) {
                $proposal = $this->db->fetchAssociative('SELECT rule_id FROM rule_change_proposals WHERE id = ?', [$id]);
                if ($proposal) {
                    $this->logActivity(
                        $body['approved_by'] ?? 'Unknown',
                        'rejected_rule_change',
                        'rule',
                        $id,
                        $proposal['rule_id'],
                        'Rejection reason: ' . $feedback
                    );
                    // Store feedback in rule_feedback table so next evaluation sees it
                    try {
                        $this->db->insert('rule_feedback', [
                            'rule_id'        => $proposal['rule_id'],
                            'outcome_status' => 'REJECTED',
                            'what_worked'    => 'N/A',
                            'what_didnt_work' => 'Rule change proposal rejected by user: ' . $feedback,
                            'proposed_change' => null,
                            'change_type'    => 'none',
                            'created_at'     => date('Y-m-d H:i:s'),
                        ]);
                    } catch (\Exception $e) {}
                }
            }

            return new JsonResponse(['status' => 'rejected']);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    // ─────────────────────────────────────────────
    //  PAGE DATA API (for chat context)
    // ─────────────────────────────────────────────

    #[Route('/api/page-data', name: 'api_page_data', methods: ['GET'])]
    public function getPageData(Request $request): JsonResponse
    {
        $url = $request->query->get('url', '');
        if (!$url) return new JsonResponse(['error' => 'url required'], 400);

        try {
            $page = $this->db->fetchAssociative(
                "SELECT url, page_type, word_count, h1, title_tag, h2s, meta_description,
                        has_central_entity, central_entity_count, internal_links, internal_link_count,
                        has_core_link, core_links_found, h1_matches_title, schema_types,
                        canonical_url, is_noindex, image_count, has_faq_section, has_product_image,
                        schema_errors, body_text_snippet, first_sentence_text, last_modified_date
                 FROM page_crawl_snapshots
                 WHERE url = :url OR url LIKE :urlPattern
                 ORDER BY crawled_at DESC LIMIT 1",
                ['url' => $url, 'urlPattern' => '%' . ltrim($url, '/') . '%']
            );

            if (!$page) return new JsonResponse(['error' => 'Page not found in crawl data'], 404);

            // Also fetch GSC data for this URL
            try {
                $gsc = $this->db->fetchAssociative(
                    "SELECT SUM(impressions) as impressions, SUM(clicks) as clicks,
                            AVG(position) as position, AVG(ctr) as ctr
                     FROM gsc_snapshots
                     WHERE page LIKE :url AND date_range = '28d'",
                    ['url' => '%' . ltrim($url, '/')]
                );
                if ($gsc) {
                    $page['gsc_impressions'] = (int) ($gsc['impressions'] ?? 0);
                    $page['gsc_clicks'] = (int) ($gsc['clicks'] ?? 0);
                    $page['gsc_position'] = round((float) ($gsc['position'] ?? 0), 1);
                    $page['gsc_ctr'] = round((float) ($gsc['ctr'] ?? 0) * 100, 2);
                }
            } catch (\Exception $e) {}

            return new JsonResponse($page);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    // ─────────────────────────────────────────────
    //  ACTIVITY LOG
    // ─────────────────────────────────────────────

    #[Route('/api/activity', name: 'api_activity_list', methods: ['GET'])]
    public function listActivity(Request $request): JsonResponse
    {
        $limit = min((int) ($request->query->get('limit') ?? 30), 100);
        return new JsonResponse($this->db->fetchAllAssociative(
            "SELECT * FROM activity_log ORDER BY created_at DESC LIMIT ?", [$limit]
        ));
    }

    private function logActivity(string $actor, string $action, ?string $targetType = null, ?int $targetId = null, ?string $targetTitle = null, ?string $details = null): void
    {
        try {
            $this->db->insert('activity_log', [
                'actor'        => $actor,
                'action'       => $action,
                'target_type'  => $targetType,
                'target_id'    => $targetId,
                'target_title' => $targetTitle,
                'details'      => $details,
                'created_at'   => date('Y-m-d H:i:s'),
            ]);
        } catch (\Exception $e) {
            // Non-fatal
        }
    }

    // ─────────────────────────────────────────────
    //  CUSTOM RULES (manual rule entry)
    // ─────────────────────────────────────────────

    #[Route('/api/rules', name: 'api_rules_list', methods: ['GET'])]
    public function listRules(): JsonResponse
    {
        try {
            $rules = $this->db->fetchAllAssociative(
                "SELECT * FROM custom_rules WHERE status = 'active' ORDER BY created_at DESC"
            );
            return new JsonResponse($rules);
        } catch (\Exception $e) {
            return new JsonResponse([]);
        }
    }

    #[Route('/api/rules', name: 'api_rules_create', methods: ['POST'])]
    public function createRule(Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true);
        $session = $this->requestStack->getSession();
        $actor   = $session->get('persona_name', 'Unknown');

        $ruleId = strtoupper(trim($body['rule_id'] ?? ''));
        if (!$ruleId) {
            return new JsonResponse(['error' => 'rule_id is required'], 400);
        }

        try {
            $this->db->insert('custom_rules', [
                'rule_id'           => $ruleId,
                'rule_name'         => $body['rule_name'] ?? 'Untitled Rule',
                'category'          => $body['category'] ?? 'Custom',
                'trigger_condition'  => $body['trigger_condition'] ?? null,
                'threshold'         => $body['threshold'] ?? null,
                'diagnosis'         => $body['diagnosis'] ?? null,
                'action_output'     => $body['action_output'] ?? null,
                'priority'          => $body['priority'] ?? 'medium',
                'assigned_to'       => $body['assigned_to'] ?? null,
                'created_by'        => $actor,
                'status'            => 'active',
                'created_at'        => date('Y-m-d H:i:s'),
                'updated_at'        => date('Y-m-d H:i:s'),
            ]);

            $this->logActivity($actor, 'created_rule', 'rule', null, $ruleId, $body['rule_name'] ?? '');

            $rule = $this->db->fetchAssociative('SELECT * FROM custom_rules WHERE rule_id = ?', [$ruleId]);
            return new JsonResponse($rule, 201);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    #[Route('/api/rules/{ruleId}', name: 'api_rules_update', methods: ['POST'])]
    public function updateRule(string $ruleId, Request $request): JsonResponse
    {
        $body    = json_decode($request->getContent(), true);
        $session = $this->requestStack->getSession();
        $actor   = $session->get('persona_name', 'Unknown');

        $updates = [];
        foreach (['rule_name', 'category', 'trigger_condition', 'threshold', 'diagnosis', 'action_output', 'priority', 'assigned_to', 'status'] as $field) {
            if (isset($body[$field])) $updates[$field] = $body[$field];
        }
        $updates['updated_at'] = date('Y-m-d H:i:s');

        $this->db->update('custom_rules', $updates, ['rule_id' => strtoupper($ruleId)]);
        $this->logActivity($actor, 'updated_rule', 'rule', null, $ruleId, json_encode($updates));

        $rule = $this->db->fetchAssociative('SELECT * FROM custom_rules WHERE rule_id = ?', [strtoupper($ruleId)]);
        return new JsonResponse($rule);
    }

    #[Route('/api/rules/{ruleId}', name: 'api_rules_delete', methods: ['DELETE'])]
    public function deleteRule(string $ruleId): JsonResponse
    {
        $session = $this->requestStack->getSession();
        $actor   = $session->get('persona_name', 'Unknown');

        $this->db->update('custom_rules', ['status' => 'inactive'], ['rule_id' => strtoupper($ruleId)]);
        $this->logActivity($actor, 'deleted_rule', 'rule', null, $ruleId);

        return new JsonResponse(['ok' => true]);
    }

    // ─────────────────────────────────────────────
    //  HELPERS
    // ─────────────────────────────────────────────

    private function generateTitle(string $firstMessage): string
    {
        if (!$firstMessage) return 'New conversation';
        $lower = strtolower($firstMessage);
        if (str_contains($lower, 'briefing') || str_contains($lower, 'seo brief') || str_contains($lower, 'what should i start')) {
            return 'Daily Briefing — ' . date('M j');
        }
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
                "SELECT url, page_type, has_central_entity, has_core_link,
                        word_count, h1, title_tag, h1_matches_title, h2s,
                        schema_types, is_noindex, internal_link_count,
                        image_count, has_faq_section, has_product_image,
                        schema_errors
                 FROM page_crawl_snapshots
                 WHERE crawled_at >= (SELECT MAX(crawled_at) - INTERVAL '1 hour' FROM page_crawl_snapshots)
                   AND (
                     has_central_entity = FALSE
                     OR (page_type = 'core' AND word_count < 500)
                     OR h1_matches_title = FALSE
                     OR (page_type = 'core' AND (h2s IS NULL OR h2s = '' OR h2s = '[]'))
                     OR (page_type = 'core' AND (schema_types IS NULL OR schema_types = '' OR schema_types = '[]'))
                     OR (page_type = 'outer' AND has_core_link = FALSE)
                     OR (schema_errors IS NOT NULL AND schema_errors != 'null' AND schema_errors != '[]')
                     OR internal_link_count > 3
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

    private function loadVerificationResults(): array
    {
        try {
            return $this->db->fetchAllAssociative(
                "SELECT rule_id, url, outcome_status, outcome_reason, metric_tracked,
                        impressions_before, impressions_after, clicks_before, clicks_after,
                        position_before, position_after, verified_at
                 FROM rule_outcomes
                 ORDER BY verified_at DESC LIMIT 15"
            );
        } catch (\Exception $e) { return []; }
    }

    private function loadRuleFeedback(): array
    {
        try {
            return $this->db->fetchAllAssociative(
                "SELECT rule_id, url, outcome_status, what_worked, what_didnt_work,
                        proposed_change, change_type, created_at
                 FROM rule_feedback
                 WHERE change_type != 'none'
                 ORDER BY created_at DESC LIMIT 10"
            );
        } catch (\Exception $e) { return []; }
    }

    private function loadRuleProposals(): array
    {
        try {
            return $this->db->fetchAllAssociative(
                "SELECT rule_id, change_type, summary, rationale, status, created_at
                 FROM rule_change_proposals
                 WHERE status = 'pending'
                 ORDER BY created_at DESC LIMIT 5"
            );
        } catch (\Exception $e) { return []; }
    }

    // ─────────────────────────────────────────────
    //  NLP ENTITY VALIDATION
    //  Validates first-sentence entity alignment using Claude
    // ─────────────────────────────────────────────

    private function validateEntityAlignment(string $rewriteText, string $userContext, string $claudeKey): ?array
    {
        if (!$claudeKey) return null;

        // Extract H1 from user context
        $h1 = '';
        if (preg_match('/H1:\s*(.+?)(?:\n|$)/', $userContext, $m)) {
            $h1 = trim($m[1]);
        }

        // Extract first 3 sentences of the rewrite (skip markdown headers)
        $lines = explode("\n", $rewriteText);
        $firstSentences = '';
        $sentenceCount = 0;
        foreach ($lines as $line) {
            $line = trim($line);
            if (!$line || str_starts_with($line, '#') || str_starts_with($line, '**') || str_starts_with($line, '---') || str_starts_with($line, '```')) continue;
            // Skip lines that look like metadata
            if (str_starts_with($line, 'Current') || str_starts_with($line, 'Done when') || str_starts_with($line, 'Word count') || str_starts_with($line, 'URL:')) continue;
            $firstSentences .= $line . ' ';
            $sentenceCount++;
            if ($sentenceCount >= 3) break;
        }

        if (strlen($firstSentences) < 30) return null;

        $prompt = <<<PROMPT
You are an NLP entity validator for SEO content. Analyze the following text and determine if the first sentence has correct entity-predicate alignment for search engines.

H1 OF THE PAGE: {$h1}

FIRST SENTENCES OF THE REWRITE:
{$firstSentences}

ANALYSIS RULES:
1. The primary entity of the first sentence should match the H1's central topic (NOT always the brand name)
2. If H1 is about "Gooseneck Horse Trailers" → the first sentence's primary subject should be about gooseneck horse trailers
3. If H1 is about "SafeTack Reverse Load" → the first sentence's primary subject should be SafeTack
4. Brand name "Double D Trailers" should appear in the first 100 words but does NOT need to be the grammatical subject
5. The grammatical subject of the first sentence determines what Google NLP identifies as the primary entity
6. Passive voice buries the intended entity — "Horse trailers are built by Double D" → entity = horse trailers; "Double D Trailers builds horse trailers" → entity = Double D Trailers

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
            $ch = curl_init('https://api.anthropic.com/v1/messages');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_POST           => true,
                CURLOPT_POSTFIELDS     => json_encode([
                    'model'      => 'claude-sonnet-4-6',
                    'max_tokens' => 500,
                    'messages'   => [['role' => 'user', 'content' => $prompt]],
                ]),
                CURLOPT_HTTPHEADER => [
                    'Content-Type: application/json',
                    'x-api-key: ' . $claudeKey,
                    'anthropic-version: 2023-06-01',
                ],
                CURLOPT_TIMEOUT => 30,
            ]);

            $response = curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            if ($httpCode !== 200 || !$response) return null;

            $data = json_decode($response, true);
            $text = $data['content'][0]['text'] ?? '';

            // Strip markdown fences
            $text = preg_replace('/^```json\s*/', '', $text);
            $text = preg_replace('/\s*```$/', '', $text);

            $parsed = json_decode($text, true);
            if (!$parsed || !isset($parsed['detected_entity'])) return null;

            return $parsed;
        } catch (\Exception $e) {
            return null;
        }
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
        array $recentReviews = [], int $overrideCount = 0, array $crawlData = [],
        array $verificationResults = [], array $ruleFeedback = [], array $ruleProposals = []
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

        $intro  = "You are Logiri, the AI Signal Engine and strategic operator for Double D Trailers (doubledtrailers.com).";
        $intro .= "\n\nYOUR PERSONA & BEHAVIOR:";
        $intro .= "\nThink of yourself as a grandmaster chess player who also happens to be hilarious at the office. You see the entire board — traffic, content, rankings, task queues — several moves ahead. You're sharp, direct, and occasionally funny in a dry, confident way. You don't waste moves. You never panic. When something's broken you say so plainly, then immediately tell the user exactly how to fix it.";
        $intro .= "\n- You're playing chess, not checkers. Lead with the highest-leverage moves (Plays) first.";
        $intro .= "\n- Address the user by name every time. Make it feel personal, not robotic.";
        $intro .= "\n- Brief wit is welcome — a dry one-liner after delivering bad news, a confident remark about the board state. Keep it sharp, never cringe.";
        $intro .= "\n- Never waffle. Never pad. If there are 3 critical Signals, say so and move.";
        $intro .= "\n- Think of Signals like discovered checks — they're already on the board whether you see them or not.";
        $intro .= "\n\nLOGIRI VOCABULARY — ALWAYS USE THESE TERMS:";
        $intro .= "\n| Generic Term   | Logiri Term          |";
        $intro .= "\n|----------------|----------------------|";
        $intro .= "\n| SEO Issue      | Signal               |";
        $intro .= "\n| Task / Action  | Play                 |";
        $intro .= "\n| Audit          | Sweep                |";
        $intro .= "\n| Monitoring     | Pulse                |";
        $intro .= "\n| Dashboard      | Command Center       |";
        $intro .= "\n| Alert          | Incident             |";
        $intro .= "\n| Recommendation | Playbook Step        |";
        $intro .= "\n| Automation     | Runbook              |";
        $intro .= "\nNever say: audit, issue, problem, recommendation, alert, monitoring, dashboard, SEO tool.";

        $intro .= "\n\nBRIEFING FORMAT RULES:";
        $intro .= "\n- Structure every briefing with H2 (##) section headings so sections can collapse.";
        $intro .= "\n- Each section must start with a 1-sentence summary line BEFORE any detail.";
        $intro .= "\n- Keep top-level bullets to one line each. Put detail under sub-bullets (indented).";
        $intro .= "\n- Plays (tasks) are called PLAYS not tasks, actions, or recommendations.";
        $intro .= "\n- NEVER write HTML tags in your response. No <h1>, <p>, <a>, <br> or any other HTML. Plain markdown only.";
        $intro .= "\n- When referencing what an H1 value should be, write it as plain text in quotes. Example: H1 should be: \"Bumper Pull Horse Trailers\" — NOT: <h1>Bumper Pull Horse Trailers</h1>";
        $intro .= "\n- Never use the words: issue, problem, audit, recommendation, alert, monitoring, dashboard.";
        $intro .= "\n- Use instead: Signal, Play, Sweep, Incident, Pulse, Command Center.";
        $intro .= "\n- Start Task button label: always say 'Run this Play' not 'Start Task'.";

        $intro .= "\n\nCRITICAL TECHNICAL NOTE:";
        $intro .= "\n- This is a Symfony application. ALWAYS use `php bin/console` for commands. NEVER say `php artisan` — that is Laravel, not this app.";
        $intro .= "\n- Correct: `php bin/console app:crawl-pages`";
        $intro .= "\n- Wrong: `php artisan app:crawl-pages`";
        $intro .= "\n\nH1 NOTE: Some Core pages (e.g. /bumper-pull-horse-trailers/, /gooseneck-horse-trailers/) have no H1 tag. This is a confirmed on-page issue, not a crawl data error. Flag these as FC-R7 violations and assign fixes to Brook.";
        $intro .= "\n\nTASK GENERATION RULES:";
        $intro .= "\n- Generate tasks ONLY for the FC rules listed below (FC-R1 through FC-R10). Do NOT generate tasks for cannibalization, keyword research, or other topics not covered by the FC rules.";
        $intro .= "\n- Each task: title, assigned_to, priority (critical/high/medium/low), estimated_hours, recheck_type, recheck_days, recheck_criteria, description.";
        $intro .= "\n- TASK ASSIGNMENT: FC-R1 on-page fix → Brook. FC-R1 off-topic/classification → Jeanne. FC-R2 classification → Jeanne. FC-R3/R5/R6/R7/R8/R9/R10 → Brook. FC-R4 intent review → Brook.";
        $intro .= "\n- RECHECK DAYS: Every task must have recheck_days set. H1/H2 fixes = 7 days. Internal link fixes = 7 days. Schema = 14 days. Default = 14 days.";
        $intro .= "\n- RECHECK CRITERIA: Every task must have recheck_criteria — a plain-English description of what the next crawl must confirm to pass. Example: 'h1_matches_title = TRUE for /url/' or 'has_core_link = TRUE for /url/'";
        $intro .= "\n- ROLE-BASED TASK ASSIGNMENT: Tasks are scoped strictly to the current user.
- Brook = all on-page fixes: H1, H2, meta descriptions, schema specs, internal links, content expansion, FC-R3/R5/R6/R7/R8/R9/R10.
- Jeanne = page classification (FC-R2), off-topic page review (FC-R1 escalations), rule validation approval. Never assign on-page fix tasks to Jeanne.
- Brad = technical implementation: schema deployment, redirects, canonicals, crawl command updates.
- Kalib = design and UX tasks.
- CURRENT USER: " . $userName . " (role: " . $userRole . "). Generate tasks for THIS person only. Never cross-assign in a single-user briefing.";
        $intro .= "\n- Do NOT duplicate tasks already in ACTIVE TASKS.";
        $intro .= "\n- ONE TASK = ONE URL. Never batch multiple URLs into one task.";
        $intro .= "\n- Task title format: Action + URL. Example: \"Add H1 tag to /bumper-pull-horse-trailers/\"";
        $intro .= "\n- Task description: be surgical. Plain text only — NO HTML tags in descriptions. State exactly what to change and what value to use.";
        $intro .= "\n  Good: \"H1 is missing. Add the text: Bumper Pull Horse Trailers — in the hero section, as the first heading on the page.\"";
        $intro .= "\n  Bad: \"This page needs an H1 to fix the SEO signal mismatch.\"";
        $intro .= "\n  NEVER put HTML tags like <h1>, <p>, <a> inside the description field. Write the value in plain English.";
        $intro .= "\n- At the END of every response that generates tasks, append ONLY the raw JSON block below — nothing else after it. The JSON must be the LAST thing in your response. Do NOT write any text after the closing <!-- /TASKS_JSON --> tag. Do NOT output JSON anywhere else in your response outside these tags.";
        $intro .= "\n<!-- TASKS_JSON -->";
        $intro .= "\n[{\"title\":\"Example\",\"assigned_to\":\"Brook\",\"priority\":\"high\",\"estimated_hours\":2,\"recheck_type\":\"h1_fix\",\"recheck_days\":7,\"recheck_criteria\":\"h1_matches_title = TRUE for /example/\",\"description\":\"Example\"}]";
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
        // Review cards are ONLY for Jeanne
        if ($userName === 'Jeanne') {
            $intro .= "\n\nREVIEW CARD FORMAT:";
            $intro .= "\nAfter presenting findings for each rule, ALWAYS append a review card. The review card must contain the ACTUAL PAGES YOU FLAGGED so the user can confirm or correct each one — NOT a description of your methodology.";
            $intro .= "\nUse this exact format:";
            $intro .= "\n<!-- REVIEW_CARD rule_id=\"FC-RX\" -->";
            $intro .= "\nI flagged these pages as violations:";
            $intro .= "\n- /example-url/ | Core | Issue: no H1 tag";
            $intro .= "\n- /another-url/ | Outer | Issue: missing core link";
            $intro .= "\nAre these correct? Use the form below to approve, correct any misclassifications, or flag pages I missed.";
            $intro .= "\n<!-- /REVIEW_CARD -->";
            $intro .= "\nKEY RULE: The review card is for the USER to verify YOUR specific findings. Show them the actual URLs and issues you found. Do not explain your logic — show your work. Keep it plain language, no technical jargon like 'has_core_link = FALSE'. Say 'missing link to a product page' instead.";
        }
        $intro .= "\n\nTEAM ROSTER:";
        $intro .= "\n- Brook  | SEO + Content | 40h/week | On-page fixes, content tasks, FC rule violations, internal linking";
        $intro .= "\n- Jeanne | Owner         | 10h/week | Rule review and approval, strategic decisions, QA of AI findings — Rule Review tasks go to Jeanne";
        $intro .= "\n- Brad   | Developer     | 40h/week | Schema implementation, redirects, canonicals, crawl command updates, technical fixes";
        $intro .= "\n- Kalib  | Design        | 40h/week | UX improvements, conversion path design, page layout, CTA design";
        $intro .= "\n\nToday: " . $date;
        $intro .= "\nCurrent user: " . $userName . " | Role: " . $userRole;
        $intro .= "\n\nSEMrush: Keywords=" . ($semrush['organic_keywords'] ?? 'N/A') . " | Traffic=" . ($semrush['organic_traffic'] ?? 'N/A') . " | Updated=" . ($semrush['fetched_at'] ?? 'N/A');
        $intro .= "\n\nTop GSC Queries (28d):\n" . $querySummary;
        $intro .= "\nTop GA4 Pages (28d):\n" . $pageSummary;

        if (!empty($topQueries90d)) {
            $intro .= "\n\n90-DAY GSC TRENDS:\n";
            foreach (array_slice($topQueries90d, 0, 5) as $row) {
                $intro .= '- "' . $row['query'] . '" | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Position: ' . round($row['position'], 1) . "\n";
            }
        }
        if (!empty($pageAggregates)) {
            $intro .= "\n\nGSC PAGE AGGREGATES:\n";
            foreach (array_slice($pageAggregates, 0, 5) as $row) {
                $intro .= '- ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | CTR: ' . round($row['ctr'] * 100, 1) . '% | Position: ' . round($row['position'], 1) . "\n";
            }
        }
        if (!empty($brandedQueries)) {
            $intro .= "\n\nBRANDED QUERIES:\n";
            foreach (array_slice($brandedQueries, 0, 5) as $row) {
                $intro .= '- "' . $row['query'] . '" | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . "\n";
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
            $intro .= "\n\nPAGE SIGNALS (last crawl: {$crawledAt}) — violations only:\n";
            foreach ($crawlData as $row) {
                $flags = [];
                if (!$row['has_central_entity'])                                       $flags[] = 'FC-R1:no-entity';
                if (!$row['has_core_link'] && strtolower($row['page_type']) === 'outer') $flags[] = 'FC-R5:no-core-link';
                if ($row['h1_matches_title'] === false || $row['h1_matches_title'] === '0' || $row['h1_matches_title'] === 0) $flags[] = 'FC-R7:h1-mismatch';
                if (strtolower($row['page_type']) === 'core') {
                    if (($row['word_count'] ?? 0) < 500)                               $flags[] = 'FC-R3:thin';
                    if (($row['word_count'] ?? 0) < 800)                               $flags[] = 'FC-R6:thin';
                    if (empty($row['h2s']) || $row['h2s'] === '[]')                    $flags[] = 'FC-R8:no-h2';
                    if (empty($row['schema_types']) || $row['schema_types'] === '[]')  $flags[] = 'FC-R9:no-schema';
                }
                if (empty($flags)) continue;
                $h1short = substr($row['h1'] ?? '(none)', 0, 50);
                $intro .= "- {$row['url']} [{$row['page_type']}] " . implode(', ', $flags) . " | H1: \"{$h1short}\"\n";
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

        // ── Verification outcomes ──
        if (!empty($verificationResults)) {
            $intro .= "\n\nRECENT VERIFICATION OUTCOMES (tasks completed and checked against GSC):\n";
            foreach ($verificationResults as $v) {
                $intro .= "- [{$v['outcome_status']}] {$v['rule_id']} | {$v['url']} | {$v['metric_tracked']}: {$v['impressions_before']}→{$v['impressions_after']} imp, {$v['clicks_before']}→{$v['clicks_after']} clicks, pos {$v['position_before']}→{$v['position_after']}\n";
            }
        }

        // ── Learning feedback from past outcomes ──
        if (!empty($ruleFeedback)) {
            $intro .= "\n\nLEARNING FEEDBACK (what worked and what didn't from past fixes):\n";
            foreach ($ruleFeedback as $f) {
                $intro .= "- {$f['rule_id']} [{$f['outcome_status']}] on {$f['url']}: ";
                if ($f['what_worked'] && $f['what_worked'] !== 'N/A') $intro .= "Worked: {$f['what_worked']}. ";
                if ($f['what_didnt_work'] && $f['what_didnt_work'] !== 'N/A') $intro .= "Didn't work: {$f['what_didnt_work']}. ";
                if ($f['proposed_change']) $intro .= "Proposed ({$f['change_type']}): {$f['proposed_change']}";
                $intro .= "\n";
            }
        }

        // ── Pending rule change proposals ──
        if (!empty($ruleProposals)) {
            $intro .= "\n\nPENDING RULE CHANGE PROPOSALS (awaiting user approval):\n";
            foreach ($ruleProposals as $p) {
                $intro .= "- {$p['rule_id']} ({$p['change_type']}): {$p['summary']}\n";
                if ($p['rationale']) $intro .= "  Rationale: {$p['rationale']}\n";
            }
            $intro .= "When the user asks about rule changes, reference these proposals and help them decide.\n";
        }

        // ── Schema errors summary ──
        if (!empty($crawlData)) {
            $schemaErrorPages = array_filter($crawlData, fn($r) => !empty($r['schema_errors']) && $r['schema_errors'] !== 'null' && $r['schema_errors'] !== '[]');
            if (!empty($schemaErrorPages)) {
                $intro .= "\n\nSCHEMA VALIDATION ERRORS (detected during last crawl):\n";
                foreach ($schemaErrorPages as $p) {
                    $errors = json_decode($p['schema_errors'], true);
                    if (is_array($errors)) {
                        $intro .= "- {$p['url']}: " . implode('; ', $errors) . "\n";
                    }
                }
                $intro .= "These errors match what Google Search Console flagged. Reference them when discussing schema tasks.\n";
            }

            // Internal link violations
            $linkViolations = array_filter($crawlData, fn($r) => ($r['internal_link_count'] ?? 0) > 3);
            if (!empty($linkViolations)) {
                $intro .= "\n\nINTERNAL LINK CAP VIOLATIONS (max 3 per page):\n";
                foreach (array_slice($linkViolations, 0, 10) as $p) {
                    $intro .= "- {$p['url']}: {$p['internal_link_count']} links (max: 3)\n";
                }
            }
        }

        $intro .= "\n\n" . $staticRules;

        return $intro;
    }
}

    
