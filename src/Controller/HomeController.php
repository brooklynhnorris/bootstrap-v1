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
            // Ensure rule_change_proposals has applied_at column for auto-apply tracking
            $this->db->executeStatement("ALTER TABLE rule_change_proposals ADD COLUMN IF NOT EXISTS applied_at TIMESTAMP DEFAULT NULL");
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
            "SELECT * FROM tasks WHERE status NOT IN ('done','closed') AND assigned_to = ? ORDER BY CASE priority WHEN 'critical' THEN 0 WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END, created_at DESC LIMIT 20",
            [$userName]
        );
        $rechecks = $this->db->fetchAllAssociative(
            "SELECT * FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false ORDER BY recheck_date ASC LIMIT 10"
        );
        $taskCounts = $this->db->fetchAssociative(
            "SELECT
                COUNT(*) FILTER (WHERE status = 'pending') as urgent,
                COUNT(*) FILTER (WHERE status = 'in_progress') as active,
                COUNT(*) FILTER (WHERE status = 'done') as done,
                COUNT(*) FILTER (WHERE status = 'closed') as closed
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

        // ── Classify message intent to load only relevant data ──
        $lastUserMsg = '';
        foreach (array_reverse($messages) as $m) {
            if ($m['role'] === 'user') { $lastUserMsg = strtolower($m['content']); break; }
        }

        $needsGsc     = str_contains($lastUserMsg, 'gsc') || str_contains($lastUserMsg, 'query') || str_contains($lastUserMsg, 'ranking')
                      || str_contains($lastUserMsg, 'position') || str_contains($lastUserMsg, 'impression') || str_contains($lastUserMsg, 'keyword')
                      || str_contains($lastUserMsg, 'traffic') || str_contains($lastUserMsg, 'serp') || str_contains($lastUserMsg, 'cannibali')
                      || str_contains($lastUserMsg, 'target query') || str_contains($lastUserMsg, 'briefing') || str_contains($lastUserMsg, 'play');
        $needsGa4     = str_contains($lastUserMsg, 'ga4') || str_contains($lastUserMsg, 'analytics') || str_contains($lastUserMsg, 'bounce')
                      || str_contains($lastUserMsg, 'engagement') || str_contains($lastUserMsg, 'session') || str_contains($lastUserMsg, 'conversion')
                      || str_contains($lastUserMsg, 'landing');
        $needsAds     = str_contains($lastUserMsg, 'ads') || str_contains($lastUserMsg, 'google ads') || str_contains($lastUserMsg, 'campaign')
                      || str_contains($lastUserMsg, 'ppc') || str_contains($lastUserMsg, 'spend') || str_contains($lastUserMsg, 'cpc');
        $needsCrawl   = str_contains($lastUserMsg, 'crawl') || str_contains($lastUserMsg, 'page') || str_contains($lastUserMsg, 'schema')
                      || str_contains($lastUserMsg, 'content') || str_contains($lastUserMsg, 'h1') || str_contains($lastUserMsg, 'title')
                      || str_contains($lastUserMsg, 'link') || str_contains($lastUserMsg, 'entity') || str_contains($lastUserMsg, 'word count')
                      || str_contains($lastUserMsg, 'rule') || str_contains($lastUserMsg, 'play') || str_contains($lastUserMsg, 'signal')
                      || str_contains($lastUserMsg, 'fix') || str_contains($lastUserMsg, 'url') || str_contains($lastUserMsg, '/');
        $needsRules   = str_contains($lastUserMsg, 'rule') || str_contains($lastUserMsg, 'proposal') || str_contains($lastUserMsg, 'learning')
                      || str_contains($lastUserMsg, 'approve') || str_contains($lastUserMsg, 'reject') || str_contains($lastUserMsg, 'verified')
                      || str_contains($lastUserMsg, 'fail') || str_contains($lastUserMsg, 'pass');
        $isGeneral    = str_contains($lastUserMsg, 'what should') || str_contains($lastUserMsg, 'briefing') || str_contains($lastUserMsg, 'overview')
                      || str_contains($lastUserMsg, 'status') || str_contains($lastUserMsg, 'summary') || str_contains($lastUserMsg, 'learned')
                      || strlen($lastUserMsg) < 20;

        // General/briefing questions load everything; specific questions load targeted data
        if ($isGeneral) { $needsGsc = true; $needsGa4 = true; $needsCrawl = true; $needsRules = true; }

        // ── Fetch data for system prompt (conditionally) ──
        $semrush = $this->db->fetchAssociative(
            'SELECT organic_keywords, organic_traffic, fetched_at FROM semrush_snapshots ORDER BY fetched_at DESC LIMIT 1'
        );

        $topQueries28d = $needsGsc ? $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE date_range = '28d' ORDER BY impressions DESC LIMIT 10"
        ) : [];
        $topQueries90d = $needsGsc ? $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE date_range = '90d' ORDER BY impressions DESC LIMIT 15"
        ) : [];
        $pageAggregates = $needsGsc ? $this->db->fetchAllAssociative(
            "SELECT page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE query = '__PAGE_AGGREGATE__' ORDER BY impressions DESC LIMIT 15"
        ) : [];
        $brandedQueries = $needsGsc ? $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, position FROM gsc_snapshots WHERE date_range = '28d_branded' ORDER BY impressions DESC LIMIT 10"
        ) : [];
        $cannibalizationCandidates = $needsGsc ? $this->db->fetchAllAssociative(
            "SELECT query, COUNT(DISTINCT page) as page_count, SUM(impressions) as total_impressions
             FROM gsc_snapshots WHERE date_range = '28d' AND query != '__PAGE_AGGREGATE__'
             GROUP BY query HAVING COUNT(DISTINCT page) > 1
             ORDER BY total_impressions DESC LIMIT 15"
        ) : [];

        $topPages = $needsGa4 ? $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, pageviews, bounce_rate, avg_engagement_time, engaged_sessions, conversions
             FROM ga4_snapshots WHERE date_range = '28d' ORDER BY sessions DESC LIMIT 15"
        ) : [];
        $previousPages = $needsGa4 ? $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, pageviews, bounce_rate, avg_engagement_time, conversions
             FROM ga4_snapshots WHERE date_range = '28d_previous' ORDER BY sessions DESC LIMIT 15"
        ) : [];
        $landingPages = $needsGa4 ? $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, bounce_rate, avg_engagement_time, conversions
             FROM ga4_snapshots WHERE date_range = '28d_landing' ORDER BY sessions DESC LIMIT 10"
        ) : [];

        $adsCampaigns = $needsAds ? $this->db->fetchAllAssociative(
            "SELECT campaign_name, impressions, clicks, cost_micros, conversions, ctr, average_cpc, status
             FROM google_ads_snapshots WHERE data_type = 'campaign' ORDER BY cost_micros DESC LIMIT 8"
        ) : [];
        $adsKeywords = $needsAds ? $this->db->fetchAllAssociative(
            "SELECT keyword, match_type, campaign_name, impressions, clicks, cost_micros, conversions, ctr, average_cpc
             FROM google_ads_snapshots WHERE data_type = 'keyword' ORDER BY cost_micros DESC LIMIT 8"
        ) : [];
        $adsSearchTerms = $needsAds ? $this->db->fetchAllAssociative(
            "SELECT keyword as search_term, campaign_name, impressions, clicks, cost_micros, conversions, ctr
             FROM google_ads_snapshots WHERE data_type = 'search_term' ORDER BY clicks DESC LIMIT 8"
        ) : [];
        $adsDailySpend = $needsAds ? $this->db->fetchAllAssociative(
            "SELECT date_range as date, cost_micros, clicks, impressions, conversions
             FROM google_ads_snapshots WHERE data_type = 'daily_spend' ORDER BY date_range DESC LIMIT 7"
        ) : [];

        $activeTasks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, assigned_role, status, priority, estimated_hours, logged_hours, created_at FROM tasks WHERE status NOT IN ('done','closed') ORDER BY created_at DESC LIMIT 10"
        );
        $pendingRechecks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, recheck_date, recheck_type FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false AND recheck_date <= CURRENT_DATE + INTERVAL '3 days' ORDER BY recheck_date ASC LIMIT 5"
        );

        // ── Load crawl data for rules engine (conditionally) ──
        $crawlData = $needsCrawl ? $this->loadCrawlData() : [];
        $allCrawledUrls = $needsCrawl ? $this->loadAllCrawledUrls() : [];

        // ── If user opened a specific Play, load that URL's full crawl row ──
        $playUrlData = null;
        $rawLastUserMsg = '';
        foreach (array_reverse($messages) as $m) {
            if ($m['role'] === 'user') { $rawLastUserMsg = $m['content']; break; }
        }
        if (str_contains($rawLastUserMsg, 'I just opened the Play:')) {
            // Extract URL from the play message — try multiple patterns
            $playUrl = null;
            // Pattern 1: em dash separator — /url/
            if (preg_match('#\x{2014}\s*(/[a-z0-9_-]+(?:/[a-z0-9_-]+)*/?)#u', $rawLastUserMsg, $urlMatch)) {
                $playUrl = $urlMatch[1];
            }
            // Pattern 2: regular dash separator - /url/
            elseif (preg_match('#\s-\s*(/[a-z0-9_-]+(?:/[a-z0-9_-]+)*/?)#i', $rawLastUserMsg, $urlMatch)) {
                $playUrl = $urlMatch[1];
            }
            // Pattern 3: any URL path at the end of the message
            elseif (preg_match('#(/[a-z0-9_-]+(?:/[a-z0-9_-]+)*/?)[\s]*$#i', $rawLastUserMsg, $urlMatch)) {
                $playUrl = $urlMatch[1];
            }
            // Pattern 4: any URL path anywhere in the message (last resort)
            elseif (preg_match_all('#(/[a-z0-9][a-z0-9_-]+/)#i', $rawLastUserMsg, $allUrls)) {
                // Take the last URL found (most likely the page URL, not a rule reference)
                $playUrl = end($allUrls[1]);
            }
            // Ensure trailing slash
            if ($playUrl && !str_ends_with($playUrl, '/')) {
                $playUrl .= '/';
            }
            if ($playUrl) {
                try {
                    $playUrlData = $this->db->fetchAssociative(
                        "SELECT url, page_type, has_central_entity, has_core_link,
                                word_count, h1, title_tag, h1_matches_title, h2s,
                                schema_types, is_noindex, internal_link_count,
                                image_count, has_faq_section, has_product_image,
                                schema_errors, meta_description, first_sentence_text,
                                body_text_snippet,
                                images_without_alt, images_with_generic_alt,
                                target_query, target_query_impressions, target_query_position, target_query_clicks
                         FROM page_crawl_snapshots
                         WHERE url = ?
                         ORDER BY crawled_at DESC LIMIT 1",
                        [$playUrl]
                    );
                } catch (\Exception $e) { $playUrlData = null; }

                // Try to load image_alt_data separately (column may not exist yet)
                if ($playUrlData && $playUrl) {
                    try {
                        $imgData = $this->db->fetchOne(
                            "SELECT image_alt_data FROM page_crawl_snapshots WHERE url = ? ORDER BY crawled_at DESC LIMIT 1",
                            [$playUrl]
                        );
                        $playUrlData['image_alt_data'] = $imgData ?: null;
                    } catch (\Exception $e) {
                        $playUrlData['image_alt_data'] = null;
                    }
                }
            }
        }

        // ── Load recent rule reviews and overrides for context ──
        $recentReviews = $needsRules ? $this->loadRecentReviews() : [];
        $overrideCount = $needsRules ? $this->loadOverrideCount() : 0;

        // ── Load verification outcomes for learning context ──
        $verificationResults = $needsRules ? $this->loadVerificationResults() : [];
        $ruleFeedback = $needsRules ? $this->loadRuleFeedback() : [];
        $ruleProposals = $needsRules ? $this->loadRuleProposals() : [];

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
            $recentReviews, $overrideCount, $crawlData, $allCrawledUrls,
            $verificationResults, $ruleFeedback, $ruleProposals, $playUrlData
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
            'max_tokens' => 8192,
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
        // Only run on actual page content rewrites (not task briefings or general chat).
        // The user must have pasted FULL PAGE BODY TEXT *and* the response must contain
        // actual rewritten content (not just instructions about what to change).
        $lastUserMsg = '';
        foreach (array_reverse($messages) as $m) {
            if ($m['role'] === 'user') { $lastUserMsg = $m['content']; break; }
        }
        $isRewriteContext = str_contains($lastUserMsg, 'FULL PAGE BODY TEXT')
            || str_contains($lastUserMsg, 'WRITE THE COMPLETE REWRITE');
        // Exclude task briefings — they start with "I just opened the Play:" or "Give me my SEO briefing"
        $isBriefing = str_contains($lastUserMsg, 'I just opened the Play:')
            || str_contains($lastUserMsg, 'Give me my SEO briefing')
            || str_contains($lastUserMsg, 'content rewrite'); // too broad — only match explicit rewrite requests

        if ($isRewriteContext && !$isBriefing && strlen($text) > 500) {
            $nlpResult = $this->validateEntityAlignment($text, $lastUserMsg, $claudeKey);
            if ($nlpResult && !empty($nlpResult['issues'])) {
                // Only append if the NLP found real issues (not "this is editorial text")
                $hasRealIssues = true;
                foreach ($nlpResult['issues'] as $issue) {
                    if (str_contains($issue, 'editorial') || str_contains($issue, 'meta-commentary')
                        || str_contains($issue, 'not page content') || str_contains($issue, 'prompt scaffolding')
                        || str_contains($issue, 'instructional')) {
                        $hasRealIssues = false;
                        break;
                    }
                }
                if ($hasRealIssues) {
                    $text .= "\n\n---\n\n**🔬 NLP Entity Validation:**\n";
                    foreach ($nlpResult['issues'] as $issue) {
                        $text .= "- {$issue}\n";
                    }
                    if (!empty($nlpResult['revised_first_sentence'])) {
                        $text .= "\n**Suggested first sentence revision:**\n> " . $nlpResult['revised_first_sentence'] . "\n";
                    }
                }
            } elseif ($nlpResult && empty($nlpResult['issues'])) {
                $text .= "\n\n---\n✅ **NLP Validated** — Primary entity: *" . ($nlpResult['detected_entity'] ?? 'unknown') . "* | H1 match: Yes | Subject position: correct";
            }
        }

        // ── Execute actions from LLM response ──
        $actionsExecuted = [];
        if (preg_match('/<!-- ACTIONS_JSON -->\s*(.*?)\s*<!-- \/ACTIONS_JSON -->/s', $text, $actionMatches)) {
            $actions = json_decode(trim($actionMatches[1]), true);
            if (is_array($actions)) {
                foreach ($actions as $action) {
                    $type = $action['action'] ?? '';
                    try {
                        switch ($type) {
                            case 'clear_tasks':
                                $rId = strtoupper($action['rule_id'] ?? '');
                                if ($rId) {
                                    $count = $this->db->executeStatement(
                                        "DELETE FROM tasks WHERE rule_id = ? AND status NOT IN ('done','closed')",
                                        [$rId]
                                    );
                                    $actionsExecuted[] = "Cleared {$count} pending tasks for {$rId}";
                                }
                                break;

                            case 'clear_tasks_url':
                                $url = $action['url'] ?? '';
                                if ($url) {
                                    $count = $this->db->executeStatement(
                                        "DELETE FROM tasks WHERE title LIKE ? AND status NOT IN ('done','closed')",
                                        ['%' . $url . '%']
                                    );
                                    $actionsExecuted[] = "Cleared {$count} pending tasks for {$url}";
                                }
                                break;

                            case 'disable_rule':
                                $rId = strtoupper($action['rule_id'] ?? '');
                                if ($rId) {
                                    $this->db->update('seo_rules', ['is_active' => false, 'updated_at' => date('Y-m-d H:i:s'), 'updated_by' => 'logiri_chat'], ['rule_id' => $rId]);
                                    $actionsExecuted[] = "Rule {$rId} deactivated";
                                }
                                break;

                            case 'enable_rule':
                                $rId = strtoupper($action['rule_id'] ?? '');
                                if ($rId) {
                                    $this->db->update('seo_rules', ['is_active' => true, 'updated_at' => date('Y-m-d H:i:s'), 'updated_by' => 'logiri_chat'], ['rule_id' => $rId]);
                                    $actionsExecuted[] = "Rule {$rId} activated";
                                }
                                break;

                            case 'update_rule_field':
                                $rId = strtoupper($action['rule_id'] ?? '');
                                $field = $action['field'] ?? '';
                                $value = $action['value'] ?? '';
                                $allowedFields = ['trigger_sql', 'trigger_condition', 'threshold', 'diagnosis', 'action_output', 'priority', 'assigned'];
                                if ($rId && $field && in_array($field, $allowedFields)) {
                                    $this->db->update('seo_rules', [$field => $value, 'updated_at' => date('Y-m-d H:i:s'), 'updated_by' => 'logiri_chat'], ['rule_id' => $rId]);
                                    $actionsExecuted[] = "Rule {$rId} field '{$field}' updated";
                                }
                                break;

                            case 'add_learning':
                                $learning = $action['learning'] ?? '';
                                $category = $action['category'] ?? 'general';
                                if ($learning && strlen($learning) > 5) {
                                    $existing = $this->db->fetchOne("SELECT COUNT(*) FROM chat_learnings WHERE learning ILIKE ? AND is_active = TRUE", ['%' . substr($learning, 0, 50) . '%']);
                                    if (!$existing) {
                                        $this->db->insert('chat_learnings', [
                                            'learning' => substr($learning, 0, 500),
                                            'category' => $category,
                                            'confidence' => 8,
                                            'learned_from' => 'logiri_action',
                                            'is_active' => true,
                                            'created_at' => date('Y-m-d H:i:s'),
                                        ]);
                                        $actionsExecuted[] = "Learning stored: " . substr($learning, 0, 60);
                                    }
                                }
                                break;

                            case 'dismiss_task':
                                $taskId = intval($action['task_id'] ?? 0);
                                $dismissType = $action['type'] ?? 'invalid';
                                $reason = $action['reason'] ?? 'Dismissed by Logiri';
                                if ($taskId > 0) {
                                    $this->db->update('tasks', [
                                        'status' => 'closed',
                                        'completed_at' => date('Y-m-d H:i:s'),
                                        'recheck_date' => null,
                                        'recheck_verified' => true,
                                        'recheck_result' => $dismissType,
                                        'recheck_criteria' => $reason,
                                    ], ['id' => $taskId]);
                                    $actionsExecuted[] = "Task #{$taskId} closed as {$dismissType}";
                                }
                                break;

                            case 'suppress_url':
                                $suppressUrl = $action['url'] ?? '';
                                $suppressRule = $action['rule_id'] ?? '__ALL__';
                                $suppressReason = $action['reason'] ?? 'Suppressed via chat';
                                if ($suppressUrl) {
                                    try {
                                        $this->db->executeStatement(
                                            'CREATE TABLE IF NOT EXISTS suppressed_tasks (
                                                id SERIAL PRIMARY KEY,
                                                url TEXT NOT NULL,
                                                rule_id VARCHAR(20),
                                                reason TEXT,
                                                suppressed_by VARCHAR(100),
                                                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                UNIQUE(url, rule_id)
                                            )'
                                        );
                                        $existingSuppression = $this->db->fetchOne(
                                            'SELECT COUNT(*) FROM suppressed_tasks WHERE url = ? AND rule_id = ?',
                                            [$suppressUrl, $suppressRule]
                                        );
                                        if (!$existingSuppression) {
                                            $this->db->insert('suppressed_tasks', [
                                                'url'           => $suppressUrl,
                                                'rule_id'       => $suppressRule,
                                                'reason'        => $suppressReason,
                                                'suppressed_by' => 'logiri_chat',
                                                'created_at'    => date('Y-m-d H:i:s'),
                                            ]);
                                        }
                                        // Also clear any pending tasks for this URL
                                        $cleared = $this->db->executeStatement(
                                            "DELETE FROM tasks WHERE title LIKE ? AND status NOT IN ('done','closed')",
                                            ['%' . $suppressUrl . '%']
                                        );
                                        $actionsExecuted[] = "Suppressed {$suppressUrl} (rule: {$suppressRule}). Cleared {$cleared} pending tasks.";
                                    } catch (\Exception $e) {
                                        $actionsExecuted[] = "Suppress failed: " . substr($e->getMessage(), 0, 80);
                                    }
                                }
                                break;
                        }
                    } catch (\Exception $e) {
                        $actionsExecuted[] = "Action {$type} failed: " . substr($e->getMessage(), 0, 80);
                    }
                }
            }
            // Strip the actions block from the visible response
            $text = preg_replace('/<!-- ACTIONS_JSON -->.*?<!-- \/ACTIONS_JSON -->/s', '', $text);
        }

        // If actions were executed, append a confirmation to the response
        if (!empty($actionsExecuted)) {
            $text .= "\n\n---\n**✓ Actions executed:**\n";
            foreach ($actionsExecuted as $ae) {
                $text .= "- {$ae}\n";
            }
        }

        // ── Auto-create tasks ──
        $tasksCreated = [];
        if (preg_match('/<!-- TASKS_JSON -->\s*(.*?)\s*<!-- \/TASKS_JSON -->/s', $text, $matches)) {
            $aiTasks = json_decode(trim($matches[1]), true);
            $activeCount = (int)$this->db->fetchOne("SELECT COUNT(*) FROM tasks WHERE status NOT IN ('done','closed')");
            if (is_array($aiTasks) && $activeCount < 30) {
                foreach ($aiTasks as $aiTask) {
                    $title = $aiTask['title'] ?? '';
                    if (!$title) continue;
                    $existing = $this->db->fetchAssociative(
                        "SELECT id FROM tasks WHERE title = ? AND status NOT IN ('done','closed') LIMIT 1", [$title]
                    );
                    if ($existing) continue;
                    // Also skip if same URL appears in an active task with same rule prefix
                    if (preg_match('|(/[^/ ]+/)|u', $title, $urlParts)) {
                        $urlFrag = $urlParts[1];
                        $rulePrefix = substr($title, 0, 10);
                        $nearDup = $this->db->fetchAssociative(
                            "SELECT id FROM tasks WHERE title LIKE ? AND title LIKE ? AND status NOT IN ('done','closed') LIMIT 1",
                            ['%' . $urlFrag . '%', $rulePrefix . '%']
                        );
                        if ($nearDup) continue;

                        // Check URL suppression — skip if this URL+rule was previously dismissed
                        try {
                            $ruleId = '';
                            if (preg_match('/^\[([A-Z]+-[A-Za-z0-9]+)\]/', $title, $ruleMatch)) {
                                $ruleId = $ruleMatch[1];
                            }
                            if ($ruleId) {
                                $suppressed = $this->db->fetchOne(
                                    'SELECT COUNT(*) FROM suppressed_tasks WHERE url = ? AND (rule_id = ? OR rule_id IS NULL)',
                                    [$urlFrag, $ruleId]
                                );
                                if ($suppressed) continue;
                            }
                            // Also check if URL is suppressed for ALL rules (blanket suppress)
                            $blanketSuppressed = $this->db->fetchOne(
                                "SELECT COUNT(*) FROM suppressed_tasks WHERE url = ? AND rule_id = '__ALL__'",
                                [$urlFrag]
                            );
                            if ($blanketSuppressed) continue;
                        } catch (\Exception $e) {
                            // Table might not exist yet — continue normally
                        }

                        // ── HARD TRAFFIC GATE: Skip optimization tasks for zero-traffic pages ──
                        // Pages with 0 GSC impressions should only get strategic review tasks, not optimization.
                        // This is a PHP-level gate — the LLM cannot override it.
                        try {
                            $pageImpressions = $this->db->fetchOne(
                                "SELECT COALESCE(impressions, 0) FROM gsc_snapshots WHERE page LIKE ? AND query = '__PAGE_AGGREGATE__' LIMIT 1",
                                ['%' . $urlFrag . '%']
                            );
                            $pageImpressions = (int)($pageImpressions ?: 0);
                            if ($pageImpressions === 0) {
                                // Only allow strategic review tasks (assigned to Jeanne) for zero-traffic pages
                                $assignedTo = strtolower($aiTask['assigned_to'] ?? '');
                                $isStrategicReview = str_contains(strtolower($title), 'evaluate') 
                                    || str_contains(strtolower($title), 'noindex')
                                    || str_contains(strtolower($title), 'redirect')
                                    || str_contains(strtolower($title), 'strategic')
                                    || str_contains(strtolower($title), 'consolidat')
                                    || $assignedTo === 'jeanne';
                                if (!$isStrategicReview) continue; // Skip non-strategic tasks for dead pages
                            }
                        } catch (\Exception $e) {
                            // GSC data not available — continue normally
                        }
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

        // ── Fix HTML tag artifacts in LLM output ──
        // Preserve tags inside code blocks (backtick-wrapped), strip stray ones in prose
        // First: protect code blocks
        $codeBlocks = [];
        $text = preg_replace_callback('/```[\s\S]*?```|`[^`]+`/', function($match) use (&$codeBlocks) {
            $placeholder = '%%CODEBLOCK_' . count($codeBlocks) . '%%';
            $codeBlocks[$placeholder] = $match[0];
            return $placeholder;
        }, $text);

        // Strip stray HTML tags in prose (not in code blocks)
        $text = preg_replace('/<\/?(h[1-6]|p|br|div|span|a|ul|li|ol|strong|em|b|i)[^>]*>/i', '', $text);

        // Clean up empty angle bracket artifacts: <> or < >
        $text = str_replace(['<>', '< >'], '', $text);

        // Restore code blocks
        foreach ($codeBlocks as $placeholder => $original) {
            $text = str_replace($placeholder, $original, $text);
        }

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
            '/2h-straight-bp'   => '/2h-straight-bp',
            '/1-horse-trailer'  => '/1-horse-trailer',
        ];
        foreach ($urlFixes as $broken => $fixed) {
            $text = str_ireplace($broken, $fixed, $text);
        }

        // Fix doubledtrailers.com + any letter without slash (catches ALL truncated domain+path joins)
        $text = preg_replace('/doubledtrailers\.com([a-z])/', 'doubledtrailers.com/$1', $text);

        // Fix double-slash in URLs: /path// → /path/
        $text = preg_replace('#(/[a-z0-9\-]+)//#', '$1/', $text);

        // Fix URLs that start mid-word (e.g., "on `umper-pull-2-horse`" — missing leading /b)
        // Match word boundaries where a known DDT URL segment appears truncated
        $urlPrefixFixes = [
            'umper-pull'      => 'bumper-pull',
            'ooseneck-'       => 'gooseneck-',
            'afetack-'        => 'safetack-',
            'rail-blazer'     => 'trail-blazer',
            'orse-trailer'    => 'horse-trailer',
            'iving-quarters'  => 'living-quarters',
        ];
        foreach ($urlPrefixFixes as $truncated => $full) {
            // Only fix when it looks like a URL context (after / or ` or whitespace)
            $text = preg_replace('/(?<=[\s`\/])' . preg_quote($truncated, '/') . '/', $full, $text);
        }

        $text = rtrim($text);

        // ── Save assistant response ──
        $this->db->insert('messages', [
            'conversation_id' => $conversationId,
            'role'            => 'assistant',
            'content'         => $text,
            'created_at'      => date('Y-m-d H:i:s'),
        ]);

        // ── Extract learnings from conversation (runs after every chat exchange) ──
        if (count($messages) >= 2 && $claudeKey) {
            try {
                $this->extractLearnings($messages, $text, $claudeKey, $userName);
            } catch (\Exception $e) {
                // Non-fatal — learning extraction failure doesn't block response
            }
        }

        return new JsonResponse([
            'response'          => $text,
            'tasks_created'     => $tasksCreated,
            'actions_executed'  => $actionsExecuted,
            'conversation_id'   => $conversationId,
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
        $sql .= " ORDER BY CASE priority
                    WHEN 'critical' THEN 0
                    WHEN 'urgent' THEN 0
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                    ELSE 4 END,
                  created_at DESC";
        $tasks = $this->db->fetchAllAssociative($sql, $params);

        // Lightweight GSC impressions lookup — batch query, not per-task join
        try {
            $gscPages = $this->db->fetchAllAssociative(
                "SELECT page, MAX(impressions) AS impressions FROM gsc_snapshots WHERE date_range = '28d' AND query = '__PAGE_AGGREGATE__' GROUP BY page ORDER BY impressions DESC LIMIT 100"
            );
            $gscMap = [];
            foreach ($gscPages as $g) {
                $gscMap[$g['page']] = (int) $g['impressions'];
            }
            // Match task URLs to GSC pages
            foreach ($tasks as &$task) {
                $task['gsc_impressions'] = 0;
                if (preg_match('#(/[a-z0-9\-/]+/)#', $task['title'] ?? '', $m)) {
                    $urlPath = $m[1];
                    foreach ($gscMap as $page => $imp) {
                        if (str_contains($page, $urlPath)) {
                            $task['gsc_impressions'] = $imp;
                            break;
                        }
                    }
                }
            }
            unset($task);

            // Re-sort: within same priority, highest impressions first
            usort($tasks, function($a, $b) {
                $priOrder = ['critical' => 0, 'urgent' => 0, 'high' => 1, 'medium' => 2, 'low' => 3];
                $priA = $priOrder[$a['priority'] ?? ''] ?? 4;
                $priB = $priOrder[$b['priority'] ?? ''] ?? 4;
                if ($priA !== $priB) return $priA - $priB;
                return ($b['gsc_impressions'] ?? 0) - ($a['gsc_impressions'] ?? 0);
            });
        } catch (\Exception $e) {
            // GSC lookup failed — tasks still return without impressions
            foreach ($tasks as &$task) { $task['gsc_impressions'] = 0; }
            unset($task);
        }

        return new JsonResponse($tasks);
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
        try {
            $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
            if (!$task) return new JsonResponse(['error' => 'Task not found'], 404);

            $body = json_decode($request->getContent(), true) ?: [];

            // Allow caller to override recheck days; otherwise default to 14
            $recheckDays = 14;
            if (isset($body['recheck_days']) && intval($body['recheck_days']) > 0) {
                $recheckDays = intval($body['recheck_days']);
            } elseif (!empty($task['recheck_type'])) {
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

            // Simple update - no attempt tracking complexity
            $this->db->update('tasks', [
                'status'            => 'done',
                'completed_at'      => date('Y-m-d H:i:s'),
                'recheck_date'      => $recheckDate,
                'recheck_days'      => $recheckDays,
                'recheck_criteria'  => $recheckCriteria,
                'recheck_verified'  => 0,  // Use 0/1 for PostgreSQL boolean
                'recheck_result'    => null,
            ], ['id' => $id]);

            // Log activity (non-fatal)
            try {
                $session = $this->requestStack->getSession();
                $actor   = $session->get('persona_name', 'Unknown');
                $this->logActivity($actor, 'completed_task', 'task', $id, $task['title'] ?? '', "Recheck in {$recheckDays} days");
            } catch (\Exception $e) {
                // Ignore logging errors
            }

            return new JsonResponse([
                'task'             => $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]),
                'recheck_date'     => $recheckDate,
                'recheck_days'     => $recheckDays,
                'recheck_criteria' => $recheckCriteria,
            ]);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    #[Route('/api/tasks/{id}/dismiss', name: 'api_tasks_dismiss', methods: ['POST'])]
    public function dismissTask(int $id, Request $request): JsonResponse
    {
        try {
            $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
            if (!$task) return new JsonResponse(['error' => 'Task not found'], 404);

            $body = json_decode($request->getContent(), true) ?: [];
            $reason = $body['reason'] ?? 'Marked as invalid';
            $dismissType = $body['type'] ?? 'invalid'; // invalid, not_applicable, false_positive, duplicate

            // Mark as closed — NOT done. This prevents recheck cycle.
            $this->db->update('tasks', [
                'status'            => 'closed',
                'completed_at'      => date('Y-m-d H:i:s'),
                'recheck_date'      => null,
                'recheck_days'      => null,
                'recheck_verified'  => true,
                'recheck_result'    => $dismissType,
                'recheck_criteria'  => $reason,
            ], ['id' => $id]);

            // Store feedback so the learning loop knows why this was dismissed
            try {
                $ruleId = $task['rule_id'] ?? '';
                // Extract URL from task title for suppression
                $taskUrl = '';
                if (preg_match('|(/[a-z0-9_-]+/)|i', $task['title'] ?? '', $urlMatch)) {
                    $taskUrl = $urlMatch[1];
                }

                // ── URL SUPPRESSION: Prevent regeneration of dismissed tasks ──
                // When a task is dismissed as false_positive or not_applicable,
                // suppress that URL+rule combo so it never generates a new task.
                if ($taskUrl && $ruleId && in_array($dismissType, ['false_positive', 'not_applicable', 'invalid'])) {
                    try {
                        // Create suppression table if it doesn't exist
                        $this->db->executeStatement(
                            'CREATE TABLE IF NOT EXISTS suppressed_tasks (
                                id SERIAL PRIMARY KEY,
                                url TEXT NOT NULL,
                                rule_id VARCHAR(20),
                                reason TEXT,
                                suppressed_by VARCHAR(100),
                                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                UNIQUE(url, rule_id)
                            )'
                        );
                        // Insert suppression (ignore if already exists)
                        $existing = $this->db->fetchOne(
                            'SELECT COUNT(*) FROM suppressed_tasks WHERE url = ? AND rule_id = ?',
                            [$taskUrl, $ruleId]
                        );
                        if (!$existing) {
                            $this->db->insert('suppressed_tasks', [
                                'url'            => $taskUrl,
                                'rule_id'        => $ruleId,
                                'reason'         => $reason,
                                'suppressed_by'  => 'task_dismiss',
                                'created_at'     => date('Y-m-d H:i:s'),
                            ]);
                        }
                    } catch (\Exception $e) {
                        // Non-fatal
                    }
                }

                if ($ruleId) {
                    $this->db->insert('rule_feedback', [
                        'rule_id'          => $ruleId,
                        'task_id'          => $id,
                        'url'              => '',
                        'feedback_type'    => 'dismissed',
                        'what_worked'      => null,
                        'what_didnt'       => "Task dismissed as {$dismissType}: {$reason}",
                        'proposed_change'  => $dismissType === 'false_positive' ? "Rule {$ruleId} generated a false positive. Consider tightening the trigger condition." : null,
                        'change_type'      => $dismissType === 'false_positive' ? 'refine_threshold' : 'none',
                        'created_at'       => date('Y-m-d H:i:s'),
                    ]);
                }

                // Also store as an immediate chat learning so Logiri remembers this NOW
                if ($reason && strlen($reason) > 10) {
                    $taskUrl = '';
                    if (preg_match('|(/[a-z0-9_-]+/)|i', $task['title'] ?? '', $urlMatch)) {
                        $taskUrl = " on {$urlMatch[1]}";
                    }
                    $learning = "Rule {$ruleId} task dismissed ({$dismissType}){$taskUrl}: {$reason}";
                    $existingLearning = $this->db->fetchOne(
                        "SELECT COUNT(*) FROM chat_learnings WHERE learning ILIKE ? AND is_active = TRUE",
                        ['%' . substr($reason, 0, 50) . '%']
                    );
                    if (!$existingLearning) {
                        $this->db->insert('chat_learnings', [
                            'learning'     => substr($learning, 0, 500),
                            'category'     => 'rules_feedback',
                            'confidence'   => $dismissType === 'false_positive' ? 9 : 7,
                            'learned_from' => 'task_dismiss',
                            'is_active'    => true,
                            'created_at'   => date('Y-m-d H:i:s'),
                        ]);
                    }
                }
            } catch (\Exception $e) {
                // Non-fatal — feedback storage failure doesn't block dismiss
            }

            // Log activity
            try {
                $session = $this->requestStack->getSession();
                $actor   = $session->get('persona_name', 'Unknown');
                $this->logActivity($actor, 'dismissed_task', 'task', $id, $task['title'] ?? '', "{$dismissType}: {$reason}");
            } catch (\Exception $e) {}

            return new JsonResponse([
                'ok'     => true,
                'status' => 'closed',
                'type'   => $dismissType,
                'reason' => $reason,
            ]);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
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

    #[Route('/api/tasks/{id}/recheck-date-direct', name: 'api_tasks_recheck_date_direct', methods: ['POST'])]
    public function updateRecheckDateDirect(int $id, Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true) ?: [];
        $date = $body['date'] ?? null;
        if (!$date || !preg_match('/^\d{4}-\d{2}-\d{2}$/', $date)) {
            return new JsonResponse(['error' => 'Invalid date format. Use YYYY-MM-DD'], 400);
        }
        $this->db->update('tasks', [
            'recheck_date' => $date,
        ], ['id' => $id]);
        return new JsonResponse(['recheck_date' => $date, 'ok' => true]);
    }

    #[Route('/api/tasks/clear-done', name: 'api_tasks_clear_done', methods: ['POST'])]
    public function clearDoneTasks(): JsonResponse
    {
        $this->db->executeStatement("DELETE FROM tasks WHERE status = 'done'");
        return new JsonResponse(['ok' => true]);
    }

    #[Route('/api/tasks/clear-pending', name: 'api_tasks_clear_pending', methods: ['POST'])]
    public function clearPendingTasks(): JsonResponse
    {
        $count = $this->db->executeStatement("DELETE FROM tasks WHERE status = 'pending'");
        return new JsonResponse(['ok' => true, 'deleted' => $count]);
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

    #[Route('/api/tasks/{id}/retry', name: 'api_tasks_retry', methods: ['POST'])]
    public function retryTask(int $id): JsonResponse
    {
        try {
            $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
            
            if (!$task) {
                return new JsonResponse(['error' => 'Task not found'], 404);
            }
            
            // Increment attempt number
            $attemptNumber = ((int)($task['attempt_number'] ?? 1)) + 1;
            
            // Archive current attempt to task_attempts table (if it exists)
            try {
                $this->db->executeStatement(
                    'INSERT INTO task_attempts (task_id, attempt_number, status, recheck_result, completed_at, recheck_date, outcome_summary)
                     VALUES (?, ?, ?, ?, ?, ?, ?)',
                    [
                        $id,
                        $task['attempt_number'] ?? 1,
                        $task['status'],
                        $task['recheck_result'],
                        $task['completed_at'],
                        $task['recheck_date'],
                        'Retried after FAIL - attempt ' . ($task['attempt_number'] ?? 1)
                    ]
                );
            } catch (\Exception $e) {
                // task_attempts table might not exist - that's OK, continue
            }
            
            // Reset task to pending with incremented attempt number
            $this->db->executeStatement(
                'UPDATE tasks SET 
                    status = ?,
                    attempt_number = ?,
                    recheck_verified = ?,
                    recheck_result = NULL,
                    recheck_date = NULL,
                    completed_at = NULL,
                    updated_at = NOW()
                 WHERE id = ?',
                ['pending', $attemptNumber, 0, $id]
            );
            
            // Update title to show it's a retry
            $currentTitle = $task['title'] ?? '';
            if (!str_contains($currentTitle, '[RETRY')) {
                $newTitle = '[RETRY-' . $attemptNumber . '] ' . preg_replace('/^\[RECHECK-FAIL\]\s*/', '', $currentTitle);
                $this->db->executeStatement(
                    'UPDATE tasks SET title = ? WHERE id = ?',
                    [$newTitle, $id]
                );
            }
            
            // Log activity
            try {
                $session = $this->requestStack->getSession();
                $actor = $session->get('persona_name', 'Unknown');
                $this->logActivity($actor, 'retried_task', 'task', $id, $task['title'] ?? '', "Attempt #{$attemptNumber}");
            } catch (\Exception $e) {
                // Ignore logging errors
            }
            
            return new JsonResponse([
                'success' => true,
                'message' => 'Task queued for retry (Attempt #' . $attemptNumber . ')',
                'task_id' => $id,
                'attempt_number' => $attemptNumber
            ]);
            
        } catch (\Exception $e) {
            return new JsonResponse(['error' => 'Failed to retry task: ' . $e->getMessage()], 500);
        }
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
    public function listRuleProposals(Request $request): JsonResponse
    {
        try {
            $includeResolved = $request->query->get('include_resolved', false);
            if ($includeResolved) {
                // Return pending + recently resolved (for Rules tab "Recently Updated" filter)
                $proposals = $this->db->fetchAllAssociative(
                    "SELECT * FROM rule_change_proposals WHERE status = 'pending' OR (status IN ('applied', 'approved', 'rejected') AND approved_at >= NOW() - INTERVAL '7 days') ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, created_at DESC LIMIT 50"
                );
            } else {
                $proposals = $this->db->fetchAllAssociative(
                    "SELECT * FROM rule_change_proposals WHERE status = 'pending' ORDER BY created_at DESC LIMIT 20"
                );
            }
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
            $session = $this->requestStack->getSession();
            $persona = $session->get('active_persona', null);
            $approvedBy = $persona ? $persona['name'] : ($body['approved_by'] ?? 'Unknown');

            // Fetch the full proposal
            $proposal = $this->db->fetchAssociative('SELECT * FROM rule_change_proposals WHERE id = ?', [$id]);
            if (!$proposal) {
                return new JsonResponse(['error' => 'Proposal not found'], 404);
            }

            $ruleId      = $proposal['rule_id'];
            $changeType  = $proposal['change_type'] ?? 'modify_action';
            $newRuleText = $proposal['new_rule_text'] ?? '';

            // ── AUTO-APPLY: Update the rule in seo_rules database table ──
            $applied = false;
            $applyError = null;

            if (!empty($newRuleText)) {
                try {
                    // Parse fields from the new rule text
                    $updates = ['full_text' => $newRuleText, 'updated_at' => date('Y-m-d H:i:s'), 'updated_by' => substr($approvedBy, 0, 100)];

                    if (preg_match('/Trigger Source:\s*([^\n]+)/', $newRuleText, $m)) $updates['trigger_source'] = trim($m[1]);
                    if (preg_match('/Trigger Condition:\s*(.*?)(?=\nThreshold:|$)/s', $newRuleText, $m)) {
                        $updates['trigger_condition'] = trim($m[1]);
                        $sql = preg_replace('/```sql\s*/', '', $updates['trigger_condition']);
                        $sql = preg_replace('/```\s*/', '', $sql);
                        $updates['trigger_sql'] = trim($sql);
                    }
                    if (preg_match('/Threshold:\s*(.*?)(?=\nDiagnosis:|$)/s', $newRuleText, $m)) $updates['threshold'] = trim($m[1]);
                    if (preg_match('/Diagnosis:\s*(.*?)(?=\nAction Output:|$)/s', $newRuleText, $m)) $updates['diagnosis'] = trim($m[1]);
                    if (preg_match('/Action Output:\s*(.*?)(?=\nPriority:|$)/s', $newRuleText, $m)) $updates['action_output'] = trim($m[1]);
                    if (preg_match('/Priority:\s*([^\n]+)/', $newRuleText, $m)) {
                        // Normalize priority to one of: Critical, High, Medium, Low
                        $rawPri = strtolower(trim($m[1]));
                        $updates['priority'] = match(true) {
                            str_contains($rawPri, 'critical'), str_contains($rawPri, 'urgent') => 'Critical',
                            str_contains($rawPri, 'high') => 'High',
                            str_contains($rawPri, 'low') => 'Low',
                            default => 'Medium',
                        };
                    }
                    if (preg_match('/Assigned:\s*([^\n]+)/', $newRuleText, $m)) $updates['assigned'] = substr(trim($m[1]), 0, 100);

                    // Check if rule exists in DB
                    $existing = $this->db->fetchAssociative('SELECT id FROM seo_rules WHERE rule_id = ?', [$ruleId]);

                    if ($existing) {
                        $this->db->update('seo_rules', $updates, ['rule_id' => $ruleId]);
                        $applied = true;
                    } else {
                        // Rule not in DB yet — insert it (handles unseeded table or new rules)
                        $newName = '';
                        if (preg_match('/^[A-Z].*?\|\s*([^\n]+)/', trim($newRuleText), $nm)) {
                            $newName = trim($nm[1]);
                        }
                        $updates['rule_id']   = $ruleId;
                        $updates['name']      = $newName ?: $ruleId;
                        $updates['is_active'] = true;
                        $updates['category']  = match(true) {
                            str_starts_with($ruleId, 'OPQ')       => 'On-Page Content Quality',
                            str_starts_with($ruleId, 'TECH')      => 'Technical SEO',
                            str_starts_with($ruleId, 'SCH'),
                            str_starts_with($ruleId, 'DDT-SD')    => 'Schema & Structured Data',
                            str_starts_with($ruleId, 'ILA')       => 'Internal Link Architecture',
                            str_starts_with($ruleId, 'KIA')       => 'Keyword & Intent Alignment',
                            str_starts_with($ruleId, 'DDT-EEAT')  => 'E-E-A-T & Trust Signals',
                            str_starts_with($ruleId, 'ETA')       => 'Entity & Topical Authority',
                            str_starts_with($ruleId, 'USE')       => 'User Signals & Engagement',
                            str_starts_with($ruleId, 'CI')        => 'Competitive Intelligence',
                            str_starts_with($ruleId, 'CFL'),
                            str_starts_with($ruleId, 'CF-')       => 'Content Freshness & Lifecycle',
                            str_starts_with($ruleId, 'DDT-LOCAL') => 'Local & Dealer SEO',
                            str_starts_with($ruleId, 'MAO')       => 'Media & Asset Optimization',
                            str_starts_with($ruleId, 'AIS')       => 'AI Search & Citation Eligibility',
                            str_starts_with($ruleId, 'CWV')       => 'Core Web Vitals & Performance',
                            str_starts_with($ruleId, 'CTA')       => 'Conversion Path & CTA',
                            default                               => 'Other',
                        };
                        try {
                            $this->db->insert('seo_rules', $updates);
                            $applied = true;
                        } catch (\Exception $insertErr) {
                            $applyError = 'Insert failed: ' . substr($insertErr->getMessage(), 0, 100);
                        }
                    }
                } catch (\Exception $e) {
                    $applyError = 'DB update failed: ' . substr($e->getMessage(), 0, 100);
                }
            } else {
                $applyError = 'No new_rule_text in proposal — nothing to apply';
            }

            // Update proposal status
            $this->db->update('rule_change_proposals', [
                'status'      => $applied ? 'applied' : 'approved',
                'approved_by' => $approvedBy,
                'approved_at' => date('Y-m-d H:i:s'),
                'applied_at'  => $applied ? date('Y-m-d H:i:s') : null,
            ], ['id' => $id]);

            // Also mark related rule_feedback entries as approved
            $this->db->executeStatement(
                "UPDATE rule_feedback SET change_approved = TRUE, approved_by = :by, approved_at = NOW() WHERE rule_id = :rule AND change_approved IS NULL",
                ['by' => $approvedBy, 'rule' => $ruleId]
            );

            // Log activity
            $this->logActivity(
                $approvedBy,
                $applied ? 'applied_rule_change' : 'approved_rule_change',
                'rule',
                $id,
                $ruleId,
                $applied
                    ? "Auto-applied {$changeType} to seo_rules table. Rule {$ruleId} updated."
                    : "Approved but not auto-applied: " . ($applyError ?? 'unknown reason')
            );

            $message = $applied
                ? "Rule {$ruleId} updated in database. Changes take effect on next cron run."
                : "Proposal approved but could not auto-apply: {$applyError}";

            return new JsonResponse([
                'status'  => $applied ? 'applied' : 'approved',
                'applied' => $applied,
                'message' => $message,
                'error'   => $applyError,
            ]);
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
                "SELECT * FROM seo_rules WHERE is_active = TRUE ORDER BY rule_id ASC"
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

    #[Route('/api/tasks/{id}', name: 'api_task_delete', methods: ['DELETE'])]
    public function deleteTask(int $id): JsonResponse
    {
        try {
            $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
            if (!$task) return new JsonResponse(['error' => 'Task not found'], 404);

            $this->db->delete('tasks', ['id' => $id]);

            $session = $this->requestStack->getSession();
            $actor = $session->get('persona_name', 'Unknown');
            $this->logActivity($actor, 'deleted_task', 'task', $id, $task['title'] ?? '');

            return new JsonResponse(['ok' => true, 'deleted' => $id]);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    #[Route('/api/rules/{ruleId}/toggle', name: 'api_rules_toggle', methods: ['POST'])]
    public function toggleRule(string $ruleId, Request $request): JsonResponse
    {
        try {
            $body = json_decode($request->getContent(), true);
            $active = $body['active'] ?? true;
            $ruleId = strtoupper($ruleId);

            // Update seo_rules table
            $existing = $this->db->fetchAssociative('SELECT id FROM seo_rules WHERE rule_id = ?', [$ruleId]);
            if (!$existing) {
                return new JsonResponse(['error' => "Rule {$ruleId} not found"], 404);
            }

            $this->db->update('seo_rules', [
                'is_active'  => $active,
                'updated_at' => date('Y-m-d H:i:s'),
                'updated_by' => 'user',
            ], ['rule_id' => $ruleId]);

            $session = $this->requestStack->getSession();
            $actor = $session->get('persona_name', 'Unknown');
            $this->logActivity($actor, $active ? 'activated_rule' : 'deactivated_rule', 'rule', null, $ruleId);

            $status = $active ? 'activated' : 'deactivated';
            return new JsonResponse(['ok' => true, 'message' => "Rule {$ruleId} {$status}. Takes effect on next cron run."]);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    // ─────────────────────────────────────────────
    //  CHAT LEARNINGS API
    // ─────────────────────────────────────────────

    #[Route('/api/learnings', name: 'api_learnings_list', methods: ['GET'])]
    public function listLearnings(): JsonResponse
    {
        try {
            $learnings = $this->db->fetchAllAssociative(
                "SELECT * FROM chat_learnings ORDER BY is_active DESC, confidence DESC, created_at DESC"
            );
            return new JsonResponse($learnings);
        } catch (\Exception $e) {
            return new JsonResponse([]);
        }
    }

    #[Route('/api/learnings', name: 'api_learnings_create', methods: ['POST'])]
    public function createLearning(Request $request): JsonResponse
    {
        try {
            $body = json_decode($request->getContent(), true);
            $this->db->insert('chat_learnings', [
                'learning'     => substr($body['learning'] ?? '', 0, 500),
                'category'     => $body['category'] ?? 'general',
                'confidence'   => min(10, max(1, intval($body['confidence'] ?? 7))),
                'learned_from' => 'manual',
                'is_active'    => true,
                'created_at'   => date('Y-m-d H:i:s'),
            ]);
            return new JsonResponse(['ok' => true, 'id' => $this->db->lastInsertId()]);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    #[Route('/api/learnings/{id}', name: 'api_learnings_update', methods: ['POST'])]
    public function updateLearning(int $id, Request $request): JsonResponse
    {
        try {
            $body = json_decode($request->getContent(), true);
            $updates = [];
            if (isset($body['learning']))   $updates['learning']   = substr($body['learning'], 0, 500);
            if (isset($body['category']))   $updates['category']   = $body['category'];
            if (isset($body['confidence'])) $updates['confidence'] = min(10, max(1, intval($body['confidence'])));
            if (isset($body['is_active']))  $updates['is_active']  = (bool) $body['is_active'];
            if (empty($updates)) return new JsonResponse(['error' => 'Nothing to update'], 400);
            $this->db->update('chat_learnings', $updates, ['id' => $id]);
            return new JsonResponse(['ok' => true]);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    #[Route('/api/learnings/{id}', name: 'api_learnings_delete', methods: ['DELETE'])]
    public function deleteLearning(int $id): JsonResponse
    {
        try {
            $this->db->delete('chat_learnings', ['id' => $id]);
            return new JsonResponse(['ok' => true]);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
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

            // Join with GSC page aggregates to get traffic data for triage
            $rows = $this->db->fetchAllAssociative(
                "SELECT p.url, p.page_type, p.has_central_entity, p.has_core_link,
                        p.word_count, p.h1, p.title_tag, p.h1_matches_title, p.h2s,
                        p.schema_types, p.is_noindex, p.internal_link_count,
                        p.image_count, p.has_faq_section, p.has_product_image,
                        p.schema_errors, p.crawled_at,
                        p.target_query, p.target_query_impressions, p.target_query_position, p.target_query_clicks
                 FROM page_crawl_snapshots p
                 WHERE p.crawled_at >= (SELECT MAX(crawled_at) - INTERVAL '1 hour' FROM page_crawl_snapshots)
                   AND (
                     p.has_central_entity = FALSE
                     OR (p.page_type = 'core' AND p.word_count < 500)
                     OR p.h1_matches_title = FALSE
                     OR (p.page_type = 'core' AND (p.h2s IS NULL OR p.h2s = '' OR p.h2s = '[]'))
                     OR (p.page_type = 'core' AND (p.schema_types IS NULL OR p.schema_types = '' OR p.schema_types = '[]'))
                     OR (p.page_type = 'outer' AND p.has_core_link = FALSE)
                     OR (p.schema_errors IS NOT NULL AND p.schema_errors != 'null' AND p.schema_errors != '[]')
                     OR p.internal_link_count > 3
                   )
                 ORDER BY p.page_type, p.url
                 LIMIT 50"
            );

            // Enrich each row with GSC traffic data for triage (separate query to avoid slow JOIN)
            foreach ($rows as &$row) {
                $row['page_impressions'] = 0;
                $row['page_clicks'] = 0;
                try {
                    $gscRow = $this->db->fetchAssociative(
                        "SELECT impressions, clicks FROM gsc_snapshots WHERE page LIKE ? AND query = '__PAGE_AGGREGATE__' LIMIT 1",
                        ['%' . $row['url']]
                    );
                    if ($gscRow) {
                        $row['page_impressions'] = (int)($gscRow['impressions'] ?? 0);
                        $row['page_clicks'] = (int)($gscRow['clicks'] ?? 0);
                    }
                } catch (\Exception $e) {
                    // GSC data not available
                }

                $impressions = (int)$row['page_impressions'];
                if ($impressions >= 500) {
                    $row['triage'] = 'high_value';
                } elseif ($impressions >= 50) {
                    $row['triage'] = 'optimize';
                } elseif ($impressions > 0) {
                    $row['triage'] = 'low_value';
                } else {
                    $row['triage'] = 'strategic_review';
                }
            }
            unset($row);

            return $rows;
        } catch (\Exception $e) { return []; }
    }

    /**
     * Load ALL crawled URLs (not just violations) so the LLM has a complete
     * list of valid pages to reference when suggesting link targets.
     */
    private function loadAllCrawledUrls(): array
    {
        try {
            $tables = $this->db->fetchFirstColumn(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'page_crawl_snapshots'"
            );
            if (empty($tables)) return [];

            return $this->db->fetchAllAssociative(
                "SELECT url, page_type, word_count, is_noindex
                 FROM page_crawl_snapshots
                 WHERE crawled_at >= (SELECT MAX(crawled_at) - INTERVAL '1 hour' FROM page_crawl_snapshots)
                   AND is_noindex = FALSE
                 ORDER BY page_type, url"
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
        array $recentReviews = [], int $overrideCount = 0, array $crawlData = [], array $allCrawledUrls = [],
        array $verificationResults = [], array $ruleFeedback = [], array $ruleProposals = [], ?array $playUrlData = null
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

        // Build rules context from seo_rules DB table (primary) or file (fallback)
        $staticRules = '';
        try {
            $dbRules = $this->db->fetchAllAssociative("SELECT * FROM seo_rules WHERE is_active = TRUE ORDER BY category, rule_id");
            if (!empty($dbRules)) {
                $parts = [];
                $parts[] = "LOGIRI RULES ENGINE -- DOUBLE D TRAILERS (from database, " . count($dbRules) . " active rules)";
                $currentCat = '';
                foreach ($dbRules as $r) {
                    if ($r['category'] !== $currentCat) {
                        $currentCat = $r['category'];
                        $parts[] = "\n--- " . strtoupper($currentCat) . " ---";
                    }
                    $parts[] = "\n{$r['rule_id']} | {$r['name']}";
                    $parts[] = "Priority: {$r['priority']} | Assigned: {$r['assigned']}";
                    if ($r['diagnosis']) $parts[] = "Diagnosis: " . substr($r['diagnosis'], 0, 300);
                    if ($r['threshold']) $parts[] = "Threshold: " . substr($r['threshold'], 0, 200);
                }
                $staticRules = implode("\n", $parts);
            }
        } catch (\Exception $e) {
            // Table doesn't exist yet — fall through to file
        }
        if (empty($staticRules)) {
            $staticRules = file_exists($promptFile) ? file_get_contents($promptFile) : '';
        }

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
        $intro .= "\n\nANTI-HALLUCINATION RULES — CRITICAL:";
        $intro .= "\n- ONLY reference URLs that appear in the PAGE SIGNALS or CRAWL DATA sections below. If a URL is not listed there, it has not been crawled.";
        $intro .= "\n- NEVER invent URLs. NEVER guess what URLs might exist. NEVER reference /horse-trailers/, /gooseneck-horse-trailers/, or any URL not explicitly shown in the crawl data.";
        $intro .= "\n- NEVER fabricate metrics (word count, internal link count, impressions, etc.). If a page is not in the crawl data, say: 'This URL has not been crawled yet. Run php bin/console app:crawl-pages to index it.'";
        $intro .= "\n- If the user asks about a URL not in crawl data, tell them it needs to be crawled first — do NOT make up data for it.";
        $intro .= "\n- When suggesting content moves or link targets, ONLY suggest URLs that appear in the crawl data below.";
        $intro .= "\n- BEFORE writing any play that includes 'link to [URL]', mentally verify the URL exists in crawl data. If it does not appear below, DO NOT SUGGEST IT. Common hallucinated URLs: /horse-trailers/, /about/, /contact/, /gooseneck-horse-trailers/. These may not exist — check first.";
        $intro .= "\n\nPAGE TRIAGE — APPLY BEFORE GENERATING ANY TASK:";
        $intro .= "\nEvery page in PAGE SIGNALS has a triage classification. Use it to determine the RIGHT action:";
        $intro .= "\n  [high_value] (500+ impressions): Full optimization plays. Fix every violation. These pages drive traffic.";
        $intro .= "\n  [optimize] (50-499 impressions): Standard optimization plays. Worth fixing.";
        $intro .= "\n  [low_value] (1-49 impressions): Minimal effort only. Fix H1/title if broken. Do NOT generate alt text, schema, or content expansion plays for these pages — not worth the time.";
        $intro .= "\n  [strategic_review] (0 impressions): DO NOT generate optimization tasks. Instead, generate ONE strategic play: 'Evaluate /url/ for noindex, redirect, or consolidation — 0 impressions, no rankings. Recommend: [noindex if thin/irrelevant] or [301 redirect to /closest-core-page/ if topically related] or [consolidate content into /relevant-page/ if useful content exists].' Assign strategic reviews to Jeanne.";
        $intro .= "\nCRITICAL: Never generate alt text, schema, internal link, or content expansion plays for [strategic_review] pages. The only valid play for a zero-traffic page is a strategic decision about whether to keep, redirect, or noindex it.";

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
        $intro .= "\n- NEVER generate tasks for URLs listed in the SUPPRESSED URLS section. These have been explicitly excluded by the user. If a suppressed URL has violations, skip it silently.";
        $intro .= "\n- Task title format: Action + URL. Example: \"Add H1 tag to /bumper-pull-horse-trailers/\"";
        $intro .= "\n- Task description: be surgical. Plain text only — NO HTML tags in descriptions. State exactly what to change and what value to use.";
        $intro .= "\n  Good: \"H1 is missing. Add the text: Bumper Pull Horse Trailers — in the hero section, as the first heading on the page.\"";
        $intro .= "\n  Bad: \"This page needs an H1 to fix the SEO signal mismatch.\"";
        $intro .= "\n  NEVER put HTML tags like <h1>, <p>, <a> inside the description field. Write the value in plain English.";
        $intro .= "\n\nPLAY CARD FORMAT — MANDATORY WHEN USER OPENS A PLAY:";
        $intro .= "\nWhen the user message starts with 'I just opened the Play:', respond with a structured play card. The play card must follow these rules:";
        $intro .= "\n1. CURRENT STATE: List only the rule violations that apply to this URL, with actual data from crawl. Do not list violations you cannot fix in the play.";
        $intro .= "\n2. YOUR PLAY: Must prescribe a specific action for EVERY violation listed in CURRENT STATE. If you diagnosed it, you fix it. No orphan diagnoses.";
        $intro .= "\n3. If a violation requires a different team member (Brad for schema, Kalib for design), say so explicitly: 'Brad: deploy ProductPage schema with these fields.' Do not just note the violation and move on.";
        $intro .= "\n4. SUCCESS: Recheck criteria must map 1:1 to the actions in YOUR PLAY. Every action gets a pass/fail condition.";
        $intro .= "\n5. Keep the play to ONE role's actions when possible. If multiple roles are needed, separate with headers per person.";
        $intro .= "\n6. NEVER diagnose a problem without prescribing the fix. If you can't prescribe it, don't mention it.";
        $intro .= "\n7. LINK TARGETS MUST EXIST: When a play says 'link to X' or 'add internal links to these pages', EVERY target URL must appear in the CRAWL DATA below. If a URL is not in the crawl data, it does not exist on the site. NEVER suggest linking to /horse-trailers/, /about/, or any URL you assume should exist. Check the crawl data first. If no suitable link target exists in crawl data, say so — do not invent one.";
        $intro .= "\n8. SINGLE RULE SCOPE: The play card title contains a rule ID (e.g. [MAO-R1], [FC-R3], [ILA-004]). Your play must ONLY address that specific rule. Do NOT bundle fixes for other rules into the same play. If you notice other violations on the page, mention them BRIEFLY at the end as 'Also flagged: [rule] — separate play needed' but do NOT include them in YOUR PLAY or SUCCESS criteria. Example: if the task is [MAO-R1] about alt text, do NOT add title tag fixes or schema deployment — those are separate plays for separate rules.";
        $intro .= "\n9. SHOW YOUR DATA: When a play involves specific page elements (images, links, headings), list the actual elements from crawl data. For image alt text plays, show each image filename and its current alt. For link plays, show the actual links found. Do not say 'apply values from the table' or 'see your task description' — the play card IS the task description. If the crawl data doesn't have per-element detail, say so and tell the user to check the page source manually.";
        $intro .= "\n- At the END of every response that generates tasks, append ONLY the raw JSON block below — nothing else after it. The JSON must be the LAST thing in your response. Do NOT write any text after the closing <!-- /TASKS_JSON --> tag. Do NOT output JSON anywhere else in your response outside these tags.";
        $intro .= "\n<!-- TASKS_JSON -->";
        $intro .= "\n[{\"title\":\"Example\",\"assigned_to\":\"Brook\",\"priority\":\"high\",\"estimated_hours\":2,\"recheck_type\":\"h1_fix\",\"recheck_days\":7,\"recheck_criteria\":\"h1_matches_title = TRUE for /example/\",\"description\":\"Example\"}]";
        $intro .= "\n<!-- /TASKS_JSON -->";
        $intro .= "\n\nCRITICAL: Include <!-- TASKS_JSON --> in EVERY response. Use [] if no tasks needed.";

        $intro .= "\n\nEXECUTABLE ACTIONS — When the user asks you to DO something (clear tasks, disable a rule, modify a rule, etc.), include an ACTIONS_JSON block. These actions are executed IMMEDIATELY by the system — they are not suggestions.";
        $intro .= "\nAvailable actions:";
        $intro .= "\n  clear_tasks: Delete pending tasks for a rule. {\"action\":\"clear_tasks\",\"rule_id\":\"ILA-005\"}";
        $intro .= "\n  clear_tasks_url: Delete pending tasks for a specific URL. {\"action\":\"clear_tasks_url\",\"url\":\"/some-page/\"}";
        $intro .= "\n  disable_rule: Deactivate a rule. {\"action\":\"disable_rule\",\"rule_id\":\"MAO-R4\"}";
        $intro .= "\n  enable_rule: Reactivate a rule. {\"action\":\"enable_rule\",\"rule_id\":\"MAO-R4\"}";
        $intro .= "\n  update_rule_field: Modify a specific rule field. {\"action\":\"update_rule_field\",\"rule_id\":\"ILA-005\",\"field\":\"trigger_sql\",\"value\":\"SELECT ...\"}";
        $intro .= "\n  add_learning: Store a learning. {\"action\":\"add_learning\",\"learning\":\"text\",\"category\":\"rules_feedback\"}";
        $intro .= "\n  dismiss_task: Close a specific task. {\"action\":\"dismiss_task\",\"task_id\":123,\"type\":\"false_positive\",\"reason\":\"text\"}";
        $intro .= "\n  suppress_url: Stop generating tasks for a URL. Use when user says a page is useless, irrelevant, or should be ignored. {\"action\":\"suppress_url\",\"url\":\"/some-page/\",\"rule_id\":\"__ALL__\",\"reason\":\"text\"}. Use rule_id=\"__ALL__\" to suppress all rules for that URL, or a specific rule_id to suppress only that rule.";
        $intro .= "\nFormat: <!-- ACTIONS_JSON -->[{\"action\":\"...\"}]<!-- /ACTIONS_JSON -->";
        $intro .= "\nPlace ACTIONS_JSON BEFORE TASKS_JSON. Actions execute first, then tasks are created.";
        $intro .= "\nOnly include actions when the user explicitly asks you to do something. Do NOT include actions in regular briefings.";
        $intro .= "\n\nFOUNDATIONAL CONTENT RULES — RUN AUTOMATICALLY ON EVERY BRIEFING:";
        $intro .= "\nYou MUST evaluate ALL of the following rules on every briefing if crawl data is available. Do not wait for the user to ask. For each rule that has violations, output the findings AND a review card so the user can verify your classification logic.";
        $intro .= "\n\nFC-R1: Every indexed page must contain the central entity 'horse trailer' in the body text. Flag pages where has_central_entity = FALSE.";
        $intro .= "\nFC-R2: Every page must be classified as Core or Outer. Flag unclassified pages.";
        $intro .= "\nFC-R3: Core pages must have at least 500 words. Flag Core pages where word_count < 500.";
        $intro .= "\nFC-R5: Outer pages with 50+ GSC impressions (28d) must link to at least one Core page. Flag Outer pages where has_core_link = FALSE AND the page has at least 50 impressions in GSC data. Do NOT generate plays for outer pages with fewer than 50 impressions — they are not worth the effort.";
        $intro .= "\nFC-R6: PRODUCT core pages (e.g. trailer model pages) have a 500-word MAX — do NOT flag them for thin content if word_count >= 300. INFORMATIONAL core pages (buying guides, comparison pages) must have at least 800 words. Flag informational core pages where word_count < 800. NEVER tell the user to expand a product page to 800 words.";
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
                // Safe boolean check helper for DBAL (can return true/false, 1/0, '1'/'0', 't'/'f', empty string)
                $isTruthy = function($val): bool {
                    return $val === true || $val === 1 || $val === '1' || $val === 't' || $val === 'true';
                };

                if (!$isTruthy($row['has_central_entity']))                              $flags[] = 'FC-R1:no-entity';
                if (!$isTruthy($row['has_core_link']) && strtolower($row['page_type']) === 'outer') $flags[] = 'FC-R5:no-core-link';
                if (!$isTruthy($row['h1_matches_title']))                                $flags[] = 'FC-R7:h1-mismatch';
                if (strtolower($row['page_type']) === 'core') {
                    if (($row['word_count'] ?? 0) < 500)                                 $flags[] = 'FC-R3:thin';
                    // FC-R6 only applies to informational core pages, not product pages — LLM determines page subtype from context
                    if (empty($row['h2s']) || $row['h2s'] === '[]')                      $flags[] = 'FC-R8:no-h2';
                    if (empty($row['schema_types']) || $row['schema_types'] === '[]')    $flags[] = 'FC-R9:no-schema';
                }
                if (empty($flags)) continue;
                $h1short = substr($row['h1'] ?? '(none)', 0, 120);
                $titleTag = substr($row['title_tag'] ?? '(none)', 0, 120);
                $wc = $row['word_count'] ?? 0;
                $imp = $row['page_impressions'] ?? 0;
                $clk = $row['page_clicks'] ?? 0;
                $triage = $row['triage'] ?? 'unknown';
                $tq = $row['target_query'] ?? null;
                $tqInfo = $tq ? " | Target: \"{$tq}\" (pos:" . ($row['target_query_position'] ?? '?') . ", imp:" . ($row['target_query_impressions'] ?? 0) . ")" : '';
                $intro .= "- {$row['url']} [{$row['page_type']}] [{$triage}] " . implode(', ', $flags) . " | {$wc}w | {$imp}imp/{$clk}clk | H1: \"{$h1short}\" | Title: \"{$titleTag}\"{$tqInfo}\n";
            }

                        // Rule violation summaries for quick Logiri parsing
            $isTruthy = function($val): bool {
                return $val === true || $val === 1 || $val === '1' || $val === 't' || $val === 'true';
            };
            $noEntity   = array_filter($crawlData, fn($r) => !$isTruthy($r['has_central_entity']) && !$isTruthy($r['is_noindex']));
            $noCoreLink = array_filter($crawlData, fn($r) => $r['page_type'] === 'outer' && !$isTruthy($r['has_core_link']) && !$isTruthy($r['is_noindex']));
            $thinCore   = array_filter($crawlData, fn($r) => $r['page_type'] === 'core' && $r['word_count'] < 500 && !$isTruthy($r['is_noindex']));
            $noH2Core   = array_filter($crawlData, fn($r) => $r['page_type'] === 'core' && ($r['h2s'] === '[]' || !$r['h2s']) && !$isTruthy($r['is_noindex']));
            $h1Mismatch = array_filter($crawlData, fn($r) => !$isTruthy($r['h1_matches_title']) && !$isTruthy($r['is_noindex']));
            $noSchema   = array_filter($crawlData, fn($r) => $r['page_type'] === 'core' && ($r['schema_types'] === '[]' || !$r['schema_types']) && !$isTruthy($r['is_noindex']));

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

        // ── Complete URL registry — so LLM never has to guess which pages exist ──
        if (!empty($allCrawledUrls)) {
            $coreUrls = array_filter($allCrawledUrls, fn($r) => strtolower($r['page_type']) === 'core');
            $outerUrls = array_filter($allCrawledUrls, fn($r) => strtolower($r['page_type']) === 'outer');
            $intro .= "\n\nVALID SITE URLS — COMPLETE LIST (use ONLY these when suggesting link targets):\n";
            $intro .= "Core pages (" . count($coreUrls) . "):\n";
            foreach ($coreUrls as $r) {
                $intro .= "  " . $r['url'] . " (" . $r['word_count'] . "w)\n";
            }
            $intro .= "Outer pages (" . count($outerUrls) . "):\n";
            foreach (array_slice(array_values($outerUrls), 0, 60) as $r) {
                $intro .= "  " . $r['url'] . "\n";
            }
            $intro .= "\nIF A URL IS NOT IN THIS LIST, IT DOES NOT EXIST ON THE SITE. DO NOT REFERENCE IT.\n";
        }

        // ── Suppressed URLs — never generate tasks for these ──
        try {
            $suppressedUrls = $this->db->fetchAllAssociative(
                'SELECT url, rule_id, reason FROM suppressed_tasks ORDER BY url'
            );
            if (!empty($suppressedUrls)) {
                $intro .= "\n\nSUPPRESSED URLS — STRATEGIC DECISIONS ALREADY MADE:\n";
                foreach ($suppressedUrls as $s) {
                    $scope = $s['rule_id'] === '__ALL__' ? 'all rules' : $s['rule_id'];
                    $intro .= "- {$s['url']} ({$scope})" . ($s['reason'] ? " — {$s['reason']}" : "") . "\n";
                }
                $intro .= "These URLs have been reviewed and a strategic decision was made. Do NOT create optimization tasks for them. If asked about them, reference the decision above.\n";
            }
        } catch (\Exception $e) {
            // Table doesn't exist yet — fine
        }

        // ── Play-specific crawl data — full row for the URL being worked on ──
        if ($playUrlData) {
            // Helper to safely convert DBAL boolean fields (can be true/false, 1/0, '1'/'0', 't'/'f', or empty string)
            $toBoolStr = function($val): string {
                if ($val === true || $val === 1 || $val === '1' || $val === 't' || $val === 'true') return 'TRUE';
                return 'FALSE';
            };

            $intro .= "\n\nPLAY TARGET URL — FULL CRAWL DATA (use ONLY these values, do NOT invent or override):\n";
            $intro .= "URL: " . $playUrlData['url'] . "\n";
            $intro .= "Page type: " . $playUrlData['page_type'] . "\n";
            $intro .= "Word count: " . $playUrlData['word_count'] . "\n";
            $intro .= "H1: \"" . ($playUrlData['h1'] ?? '(none)') . "\"\n";
            $intro .= "Title tag: \"" . ($playUrlData['title_tag'] ?? '(none)') . "\"\n";
            $intro .= "H1 matches title: " . $toBoolStr($playUrlData['h1_matches_title']) . "\n";
            $intro .= "H2s: " . ($playUrlData['h2s'] ?: '(none)') . "\n";
            $intro .= "Schema types: " . ($playUrlData['schema_types'] ?: '(none)') . "\n";
            $intro .= "Schema errors: " . ($playUrlData['schema_errors'] ?: '(none)') . "\n";
            $intro .= "Has central entity: " . $toBoolStr($playUrlData['has_central_entity']) . "\n";
            $intro .= "Has core link: " . $toBoolStr($playUrlData['has_core_link']) . "\n";
            $intro .= "Internal link count: " . ($playUrlData['internal_link_count'] ?? 0) . "\n";
            $intro .= "Image count: " . ($playUrlData['image_count'] ?? 0) . "\n";
            $intro .= "Has FAQ section: " . $toBoolStr($playUrlData['has_faq_section']) . "\n";
            $intro .= "Has product image: " . $toBoolStr($playUrlData['has_product_image']) . "\n";
            $intro .= "Images without alt: " . ($playUrlData['images_without_alt'] ?? 0) . "\n";
            $intro .= "Images with generic alt: " . ($playUrlData['images_with_generic_alt'] ?? 0) . "\n";
            // Per-image data for alt text plays
            if (!empty($playUrlData['image_alt_data'])) {
                $imgData = json_decode($playUrlData['image_alt_data'], true);
                if (is_array($imgData) && !empty($imgData)) {
                    $intro .= "Image alt text inventory:\n";
                    foreach ($imgData as $imgItem) {
                        $altDisplay = $imgItem['alt'] ?? 'NULL';
                        $intro .= "  - " . $imgItem['src'] . " | alt: \"" . $altDisplay . "\"\n";
                    }
                }
            }
            $intro .= "Meta description: \"" . ($playUrlData['meta_description'] ?? '(none)') . "\"\n";
            $intro .= "First sentence: \"" . ($playUrlData['first_sentence_text'] ?? '(none)') . "\"\n";
            $tq = $playUrlData['target_query'] ?? null;
            if ($tq) {
                $intro .= "Target query: \"{$tq}\" (pos:" . ($playUrlData['target_query_position'] ?? '?') . ", imp:" . ($playUrlData['target_query_impressions'] ?? 0) . ", clicks:" . ($playUrlData['target_query_clicks'] ?? 0) . ")\n";
            }
            $intro .= "CRITICAL: The data above is the GROUND TRUTH for this URL. Do NOT contradict it. If it says h1_matches_title = TRUE, do NOT claim there is a mismatch. If it says word_count = 2100, do NOT say the page is thin.\n";

            // Include actual page content so the LLM can make surgical recommendations
            if (!empty($playUrlData['body_text_snippet'])) {
                $bodySnippet = $playUrlData['body_text_snippet'];
                // Truncate to ~6000 chars to leave room in context window
                if (strlen($bodySnippet) > 6000) {
                    $bodySnippet = substr($bodySnippet, 0, 6000) . "\n... [truncated at 6000 chars]";
                }
                $intro .= "\nACTUAL PAGE CONTENT (from crawl — use this for surgical recommendations, do NOT invent content):\n";
                $intro .= "---\n" . $bodySnippet . "\n---\n";
                $intro .= "The text above is what the crawler extracted from the page. Base ALL content recommendations on this actual text. Do NOT invent section headings, opening sentences, or content that is not shown above.\n";
            } else {
                $intro .= "\nNOTE: No body text available for this URL. Do NOT guess what the page says. Ask the user to provide the current content before making rewrite recommendations.\n";
            }
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

        // ── Chat learnings (persistent memory from past conversations) ──
        try {
            $learnings = $this->db->fetchAllAssociative(
                "SELECT learning, category, learned_from FROM chat_learnings WHERE is_active = TRUE ORDER BY confidence DESC, created_at DESC LIMIT 30"
            );
            if (!empty($learnings)) {
                $intro .= "\n\nYOUR MEMORY (learned from past conversations with this user — follow these):\n";
                $currentCat = '';
                foreach ($learnings as $l) {
                    $cat = $l['category'] ?? 'general';
                    if ($cat !== $currentCat) {
                        $currentCat = $cat;
                        $intro .= "\n[{$cat}]\n";
                    }
                    $intro .= "- " . $l['learning'] . "\n";
                }
                $intro .= "\nThese are things you've learned about how this user works. Apply them automatically without mentioning that you're doing so.\n";
            }
        } catch (\Exception $e) {
            // Table may not exist yet
        }

        $intro .= "\n\n" . $staticRules;

        return $intro;
    }

    // ─────────────────────────────────────────────
    //  CHAT LEARNING EXTRACTION
    // ─────────────────────────────────────────────

    private function extractLearnings(array $messages, string $lastResponse, string $apiKey, string $userName): void
    {
        // Build a compact conversation summary for extraction
        $convoSummary = '';
        foreach (array_slice($messages, -6) as $msg) {
            $role = $msg['role'] === 'user' ? $userName : 'Logiri';
            $content = substr($msg['content'], 0, 500);
            $convoSummary .= "{$role}: {$content}\n\n";
        }
        $convoSummary .= "Logiri: " . substr($lastResponse, 0, 500);

        // Load existing learnings so the LLM can avoid duplicates
        $existingLearnings = '';
        try {
            $existing = $this->db->fetchAllAssociative(
                "SELECT learning, category FROM chat_learnings WHERE is_active = TRUE ORDER BY confidence DESC LIMIT 30"
            );
            if (!empty($existing)) {
                $existingLearnings = "\n\nALREADY STORED LEARNINGS (do NOT extract anything semantically similar to these):\n";
                foreach ($existing as $e) {
                    $existingLearnings .= "- [{$e['category']}] {$e['learning']}\n";
                }
            }
        } catch (\Exception $e) {}

        $extractPrompt = <<<PROMPT
You are analyzing a conversation between an SEO tool (Logiri) and its user to extract learnable insights that should persist across future conversations.

CONVERSATION:
{$convoSummary}
{$existingLearnings}

Extract ONLY genuinely NEW learnings not already covered above. Categories:
- preferences: How the user likes information presented (format, detail level, tone)
- corrections: Things the user corrected about the tool's output or assumptions
- workflow: How the user prefers to work (task size, bundling, approval patterns)
- domain_knowledge: Business-specific facts the user shared that aren't in the data
- rules_feedback: Opinions on specific SEO rules or approaches

RULES:
- CRITICAL: Check the ALREADY STORED LEARNINGS above. If your proposed learning says the same thing as an existing one (even with different wording), DO NOT include it. "User wants exact copy" and "User requires verbatim instructions" are THE SAME learning.
- Only extract learnings that would change future behavior. "User said thanks" is NOT a learning.
- Each learning must be a short, actionable statement (under 30 words).
- Max 2 learnings per conversation. Often there are 0 — that's fine. Return [] if nothing NEW.
- Do NOT extract one-time task instructions as permanent preferences.

Respond ONLY with a JSON array. No other text. Empty array [] if no new learnings.
Example: [{"learning":"User wants page hierarchy shown in task briefs","category":"preferences","confidence":8}]
PROMPT;

        $payload = json_encode([
            'model'      => 'claude-sonnet-4-5',
            'max_tokens' => 500,
            'messages'   => [['role' => 'user', 'content' => $extractPrompt]],
        ]);

        $ch = curl_init('https://api.anthropic.com/v1/messages');
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $payload,
            CURLOPT_HTTPHEADER     => [
                'Content-Type: application/json',
                'x-api-key: ' . $apiKey,
                'anthropic-version: 2023-06-01',
            ],
            CURLOPT_TIMEOUT        => 30,
            CURLOPT_CONNECTTIMEOUT => 5,
        ]);
        $response = curl_exec($ch);
        curl_close($ch);

        if (!$response) return;

        $data = json_decode($response, true);
        $text = $data['content'][0]['text'] ?? '';
        if (!$text) return;

        // Parse the JSON array
        $text = preg_replace('/```json\s*/', '', $text);
        $text = preg_replace('/```\s*/', '', $text);
        $text = trim($text);

        $learnings = json_decode($text, true);
        if (!is_array($learnings) || empty($learnings)) return;

        // Ensure table exists
        try {
            $this->db->executeStatement("
                CREATE TABLE IF NOT EXISTS chat_learnings (
                    id SERIAL PRIMARY KEY,
                    learning TEXT NOT NULL,
                    category VARCHAR(50) DEFAULT 'general',
                    confidence INT DEFAULT 5,
                    learned_from VARCHAR(255) DEFAULT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ");
        } catch (\Exception $e) {
            return;
        }

        foreach (array_slice($learnings, 0, 2) as $l) {
            if (empty($l['learning']) || strlen($l['learning']) < 10) continue;

            // Improved dedup: extract significant keywords and check for overlap
            $newWords = array_filter(
                explode(' ', strtolower(preg_replace('/[^a-z0-9 ]/i', '', $l['learning']))),
                fn($w) => strlen($w) > 4 && !in_array($w, ['user', 'wants', 'needs', 'should', 'requires', 'prefers', 'never', 'always', 'their', 'about', 'these', 'those', 'which', 'would', 'could'])
            );
            if (count($newWords) < 2) continue;

            // Check each existing learning for keyword overlap
            $isDuplicate = false;
            try {
                $allExisting = $this->db->fetchAllAssociative(
                    "SELECT learning FROM chat_learnings WHERE is_active = TRUE"
                );
                foreach ($allExisting as $ex) {
                    $exWords = array_filter(
                        explode(' ', strtolower(preg_replace('/[^a-z0-9 ]/i', '', $ex['learning']))),
                        fn($w) => strlen($w) > 4
                    );
                    $overlap = count(array_intersect($newWords, $exWords));
                    $ratio = count($newWords) > 0 ? $overlap / count($newWords) : 0;
                    if ($ratio > 0.4) {
                        $isDuplicate = true;
                        break;
                    }
                }
            } catch (\Exception $e) {}

            if ($isDuplicate) continue;

            $this->db->insert('chat_learnings', [
                'learning'     => substr($l['learning'], 0, 500),
                'category'     => $l['category'] ?? 'general',
                'confidence'   => min(10, max(1, intval($l['confidence'] ?? 5))),
                'learned_from' => substr($userName . ' conversation', 0, 255),
                'is_active'    => true,
                'created_at'   => date('Y-m-d H:i:s'),
            ]);
        }
    }
}