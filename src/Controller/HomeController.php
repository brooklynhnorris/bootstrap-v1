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
    }

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

        return $this->render('home/index.html.twig', [
            'userName' => $user ? ($user->getName() ?? explode('@', $user->getEmail())[0]) : 'User',
            'userRole' => $user ? ($user->getTeamRole() ?? 'Owner') : 'Owner',
            'userEmail' => $user ? $user->getEmail() : '',
            'tasks' => $tasks,
            'rechecks' => $rechecks,
            'taskCounts' => $taskCounts ?: ['urgent' => 0, 'active' => 0, 'done' => 0],
        ]);
    }

    #[Route('/chat', name: 'chat', methods: ['POST'])]
    public function chat(Request $request): JsonResponse
    {
        $body     = json_decode($request->getContent(), true);
        $messages = $body['messages'] ?? [];

        $user = $this->getUser();
        $userName = $user ? ($user->getName() ?? explode('@', $user->getEmail())[0]) : 'User';
        $userRole = $user ? ($user->getTeamRole() ?? 'Owner') : 'Owner';

        $semrush = $this->db->fetchAssociative(
            'SELECT organic_keywords, organic_traffic, fetched_at FROM semrush_snapshots ORDER BY fetched_at DESC LIMIT 1'
        );

        // GSC data - reduced limits for prompt efficiency
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

        // Cannibalization: find queries appearing on multiple pages
        $cannibalizationCandidates = $this->db->fetchAllAssociative(
            "SELECT query, COUNT(DISTINCT page) as page_count, SUM(impressions) as total_impressions
             FROM gsc_snapshots WHERE date_range = '28d' AND query != '__PAGE_AGGREGATE__'
             GROUP BY query HAVING COUNT(DISTINCT page) > 1
             ORDER BY total_impressions DESC LIMIT 15"
        );

        // GA4 data - reduced limits
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

        $topQueries = $topQueries28d;

        // Google Ads data
        $adsCampaigns = $this->db->fetchAllAssociative(
            "SELECT campaign_name, impressions, clicks, cost_micros, conversions, ctr, average_cpc, status
             FROM google_ads_snapshots WHERE data_type = 'campaign' ORDER BY cost_micros DESC LIMIT 10"
        );
        $adsKeywords = $this->db->fetchAllAssociative(
            "SELECT keyword, match_type, campaign_name, impressions, clicks, cost_micros, conversions, ctr, average_cpc
             FROM google_ads_snapshots WHERE data_type = 'keyword' ORDER BY cost_micros DESC LIMIT 15"
        );
        $adsSearchTerms = $this->db->fetchAllAssociative(
            "SELECT keyword as search_term, campaign_name, impressions, clicks, cost_micros, conversions, ctr
             FROM google_ads_snapshots WHERE data_type = 'search_term' ORDER BY clicks DESC LIMIT 15"
        );
        $adsDailySpend = $this->db->fetchAllAssociative(
            "SELECT date_range as date, cost_micros, clicks, impressions, conversions
             FROM google_ads_snapshots WHERE data_type = 'daily_spend' ORDER BY date_range DESC LIMIT 14"
        );

        $activeTasks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, assigned_role, status, priority, estimated_hours, logged_hours, created_at FROM tasks WHERE status != 'done' ORDER BY created_at DESC LIMIT 10"
        );

        $pendingRechecks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, recheck_date, recheck_type FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false AND recheck_date <= CURRENT_DATE + INTERVAL '3 days' ORDER BY recheck_date ASC LIMIT 5"
        );

        $systemPrompt = $this->buildSystemPrompt(
            $semrush ?: [], $topQueries, $topPages, $userName, $userRole, $activeTasks, $pendingRechecks,
            $topQueries90d, $pageAggregates, $brandedQueries, $cannibalizationCandidates,
            $previousPages, $landingPages, $adsCampaigns, $adsKeywords, $adsSearchTerms, $adsDailySpend
        );

        // Gemini expects system prompt prepended to messages
        $geminiMessages = [];
        foreach ($messages as $msg) {
            $geminiMessages[] = [
                'role'  => $msg['role'] === 'assistant' ? 'model' : 'user',
                'parts' => [['text' => $msg['content']]],
            ];
        }

        // Prepend system prompt as first user message
        array_unshift($geminiMessages, [
            'role'  => 'user',
            'parts' => [['text' => $systemPrompt]],
        ]);
        array_splice($geminiMessages, 1, 0, [[
            'role'  => 'model',
            'parts' => [['text' => 'Understood. I am Logiri, ready to assist.']],
        ]]);

        $geminiModel = $_ENV['GEMINI_MODEL'] ?? 'gemini-2.0-flash-001';
        $geminiKey   = $_ENV['GEMINI_API_KEY'] ?? '';

        $response = file_get_contents(
            "https://generativelanguage.googleapis.com/v1beta/models/{$geminiModel}:generateContent?key={$geminiKey}",
            false,
            stream_context_create([
                'http' => [
                    'method'        => 'POST',
                    'header'        => 'Content-Type: application/json',
                    'content'       => json_encode([
                        'contents'         => $geminiMessages,
                        'generationConfig' => [
                            'maxOutputTokens' => 8192,
                            'temperature'     => 0.7,
                        ],
                    ]),
                    'ignore_errors' => true,
                ],
            ])
        );

        $data = json_decode($response, true);

        if (isset($data['error'])) {
            return new JsonResponse(['error' => $data['error']['message']], 500);
        }

        $text = $data['candidates'][0]['content']['parts'][0]['text'] ?? 'No response from Gemini.';

        // ── Parse and auto-create tasks from AI response ──
        $tasksCreated = [];
        if (preg_match('/<!-- TASKS_JSON -->\s*(.*?)\s*<!-- \/TASKS_JSON -->/s', $text, $matches)) {
            $tasksJson = trim($matches[1]);
            $aiTasks = json_decode($tasksJson, true);

            if (is_array($aiTasks)) {
                foreach ($aiTasks as $aiTask) {
                    $title = $aiTask['title'] ?? '';
                    if (!$title) continue;

                    $existing = $this->db->fetchAssociative(
                        "SELECT id FROM tasks WHERE title = ? AND status != 'done' LIMIT 1",
                        [$title]
                    );
                    if ($existing) continue;

                    $priority = $aiTask['priority'] ?? 'medium';
                    if (!in_array($priority, ['critical', 'high', 'medium', 'low'])) {
                        $priority = 'medium';
                    }

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

            // Strip the hidden JSON block from the visible response
            $text = preg_replace('/<!-- TASKS_JSON -->.*?<!-- \/TASKS_JSON -->/s', '', $text);
            $text = rtrim($text);
        }

        return new JsonResponse([
            'response'      => $text,
            'tasks_created' => $tasksCreated,
        ]);
    }

    // ── Task API Endpoints ──

    #[Route('/api/tasks', name: 'api_tasks_list', methods: ['GET'])]
    public function listTasks(Request $request): JsonResponse
    {
        $status = $request->query->get('status');
        $assignee = $request->query->get('assignee');

        $sql = "SELECT * FROM tasks WHERE 1=1";
        $params = [];

        if ($status) {
            $sql .= " AND status = ?";
            $params[] = $status;
        }
        if ($assignee) {
            $sql .= " AND assigned_to = ?";
            $params[] = $assignee;
        }

        $sql .= " ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END, created_at DESC";

        $tasks = $this->db->fetchAllAssociative($sql, $params);
        return new JsonResponse($tasks);
    }

    #[Route('/api/tasks', name: 'api_tasks_create', methods: ['POST'])]
    public function createTask(Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true);

        $this->db->insert('tasks', [
            'title'         => $body['title'] ?? 'Untitled Task',
            'description'   => $body['description'] ?? null,
            'rule_id'       => $body['rule_id'] ?? null,
            'assigned_to'   => $body['assigned_to'] ?? null,
            'assigned_role' => $body['assigned_role'] ?? null,
            'status'        => 'pending',
            'priority'      => $body['priority'] ?? 'medium',
            'estimated_hours' => $body['estimated_hours'] ?? 1,
            'logged_hours'  => 0,
            'due_date'      => $body['due_date'] ?? null,
            'recheck_type'  => $body['recheck_type'] ?? null,
            'created_at'    => date('Y-m-d H:i:s'),
        ]);

        $id = $this->db->lastInsertId();
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);

        return new JsonResponse($task, 201);
    }

    #[Route('/api/tasks/{id}/complete', name: 'api_tasks_complete', methods: ['POST'])]
    public function completeTask(int $id): JsonResponse
    {
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
        if (!$task) {
            return new JsonResponse(['error' => 'Task not found'], 404);
        }

        $recheckDays = match($task['recheck_type']) {
            '404_fix'                  => 7,
            'sitemap_fix'              => 7,
            'cannibalization_fix'      => 14,
            'homepage_cannibalization' => 14,
            'intent_mismatch'          => 14,
            'weak_page'                => 14,
            'zero_click'               => 14,
            'ranking_drop'             => 28,
            default                    => 14,
        };

        $recheckDate = date('Y-m-d', strtotime("+{$recheckDays} days"));

        $this->db->update('tasks', [
            'status'       => 'done',
            'completed_at' => date('Y-m-d H:i:s'),
            'recheck_date' => $recheckDate,
        ], ['id' => $id]);

        $updated = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);

        return new JsonResponse([
            'task'         => $updated,
            'recheck_date' => $recheckDate,
            'recheck_days' => $recheckDays,
        ]);
    }

    #[Route('/api/tasks/{id}/status', name: 'api_tasks_status', methods: ['POST'])]
    public function updateTaskStatus(int $id, Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true);
        $status = $body['status'] ?? 'pending';

        $this->db->update('tasks', ['status' => $status], ['id' => $id]);
        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);

        return new JsonResponse($task);
    }

    #[Route('/api/tasks/{id}/log-time', name: 'api_tasks_log_time', methods: ['POST'])]
    public function logTime(int $id, Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true);
        $hours = floatval($body['hours'] ?? 0);

        if ($hours <= 0) {
            return new JsonResponse(['error' => 'Hours must be positive'], 400);
        }

        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);
        if (!$task) {
            return new JsonResponse(['error' => 'Task not found'], 404);
        }

        $newLogged = floatval($task['logged_hours'] ?? 0) + $hours;
        $this->db->update('tasks', ['logged_hours' => $newLogged], ['id' => $id]);
        $updated = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);

        return new JsonResponse($updated);
    }

    #[Route('/api/tasks/{id}/verify', name: 'api_tasks_verify', methods: ['POST'])]
    public function verifyTask(int $id, Request $request): JsonResponse
    {
        $body = json_decode($request->getContent(), true);
        $passed = $body['passed'] ?? false;

        $this->db->update('tasks', [
            'recheck_verified' => true,
            'recheck_result'   => $passed ? 'pass' : 'fail',
        ], ['id' => $id]);

        $task = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);

        return new JsonResponse($task);
    }

    #[Route('/api/rechecks', name: 'api_rechecks', methods: ['GET'])]
    public function listRechecks(): JsonResponse
    {
        $rechecks = $this->db->fetchAllAssociative(
            "SELECT * FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false ORDER BY recheck_date ASC"
        );
        return new JsonResponse($rechecks);
    }

    // ── System Prompt Builder ──

    private function buildSystemPrompt(
        array $semrush,
        array $topQueries,
        array $topPages,
        string $userName,
        string $userRole,
        array $activeTasks,
        array $pendingRechecks,
        array $topQueries90d = [],
        array $pageAggregates = [],
        array $brandedQueries = [],
        array $cannibalizationCandidates = [],
        array $previousPages = [],
        array $landingPages = [],
        array $adsCampaigns = [],
        array $adsKeywords = [],
        array $adsSearchTerms = [],
        array $adsDailySpend = []
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

        $keywords = $semrush['organic_keywords'] ?? 'N/A';
        $traffic  = $semrush['organic_traffic'] ?? 'N/A';
        $updated  = $semrush['fetched_at'] ?? 'N/A';

        $taskContext = '';
        if (!empty($activeTasks)) {
            $taskContext .= "\n\nACTIVE TASKS IN SYSTEM:\n";
            $workload = [];
            $overdueNudges = [];

            foreach ($activeTasks as $t) {
                $logged   = floatval($t['logged_hours'] ?? 0);
                $est      = floatval($t['estimated_hours'] ?? 0);
                $assignee = $t['assigned_to'] ?? 'Unassigned';
                $status   = $t['status'];
                $timeInfo = $est > 0 ? " | Time: {$logged}/{$est}h" : "";

                if ($assignee !== 'Unassigned' && $status !== 'done') {
                    $workload[$assignee] = ($workload[$assignee] ?? 0) + $est;
                }

                if ($logged > $est && $est > 0 && $status !== 'done') {
                    $overdueNudges[] = "⚠️ OVER-ESTIMATE: \"{$t['title']}\" — {$logged}h logged vs {$est}h estimated, still {$status}. Assigned: {$assignee}";
                }

                $taskContext .= "- [" . strtoupper($t['priority']) . "] " . $t['title'] . " | Assigned: " . $assignee . " | Status: " . $status . $timeInfo . "\n";
            }

            $taskContext .= "\nTEAM WORKLOAD (open task hours / 40h capacity):\n";
            foreach (['Brook', 'Kalib', 'Brad'] as $name) {
                $load   = $workload[$name] ?? 0;
                $status = $load > 40 ? '🔴 OVERLOADED' : ($load > 30 ? '🟡 HIGH' : '🟢 OK');
                $taskContext .= "- {$name}: {$load}h assigned | {$status}\n";
            }

            if (!empty($overdueNudges)) {
                $taskContext .= "\nTASKS EXCEEDING ESTIMATES (requires nudge):\n";
                foreach ($overdueNudges as $nudge) {
                    $taskContext .= $nudge . "\n";
                }
            }
        }

        $recheckContext = '';
        if (!empty($pendingRechecks)) {
            $recheckContext .= "\n\nPENDING VERIFICATION RECHECKS:\n";
            foreach ($pendingRechecks as $r) {
                $recheckContext .= "- Task: " . $r['title'] . " | Recheck due: " . $r['recheck_date'] . " | Type: " . ($r['recheck_type'] ?? 'general') . " | Assigned: " . ($r['assigned_to'] ?? 'Unassigned') . "\n";
            }
        }

        $promptFile  = dirname(__DIR__, 2) . '/system-prompt.txt';
        $staticRules = file_exists($promptFile) ? file_get_contents($promptFile) : '';

        $intro  = "You are Logiri, the AI Chief of Staff for Double D Trailers (doubledtrailers.com).";
        $intro .= "\n\nYOUR PERSONA & BEHAVIOR:";
        $intro .= "\n- You are an authoritative yet encouraging Project Manager and Chief of Staff.";
        $intro .= "\n- Be professional, direct, and action-oriented. Address the user by name.";
        $intro .= "\n- Lead with the MOST URGENT items first.";
        $intro .= "\n- Always connect SEO findings to ACTIONABLE TASKS with specific owners.";
        $intro .= "\n\nTASK GENERATION RULES:";
        $intro .= "\n- Generate ACTIONABLE TASKS when you identify SEO issues or give a briefing.";
        $intro .= "\n- Each task: title, assigned_to, priority (critical/high/medium/low), estimated_hours (min 0.5), recheck_type (cannibalization_fix/intent_mismatch/ranking_drop/404_fix/sitemap_fix or null), description.";
        $intro .= "\n- Do NOT duplicate tasks already in ACTIVE TASKS.";
        $intro .= "\n- At the END of every response include this hidden block (even if empty):";
        $intro .= "\n<!-- TASKS_JSON -->";
        $intro .= "\n[{\"title\":\"Example\",\"assigned_to\":\"Brook\",\"priority\":\"high\",\"estimated_hours\":2,\"recheck_type\":null,\"description\":\"Example task\"}]";
        $intro .= "\n<!-- /TASKS_JSON -->";
        $intro .= "\n\nCRITICAL: You MUST include the <!-- TASKS_JSON --> block at the end of EVERY response. Use an empty array [] if no tasks needed.";
        $intro .= "\n\nTEAM ROSTER:";
        $intro .= "\n- Brook | SEO + Content | 40h/week";
        $intro .= "\n- Kalib | Sales | 40h/week";
        $intro .= "\n- Brad | Marketing | 40h/week";
        $intro .= "\n\nToday: " . $date;
        $intro .= "\nCurrent user: " . $userName . " | Role: " . $userRole;
        $intro .= "\n\nSEMrush: Keywords=" . $keywords . " | Traffic=" . $traffic . " | Updated=" . $updated;
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
            $intro .= "\n\nGA4 PERIOD COMPARISON (28d vs previous 28d):\n";
            $prevLookup = [];
            foreach ($previousPages as $p) { $prevLookup[$p['page_path']] = $p; }
            foreach (array_slice($topPages, 0, 10) as $current) {
                $path         = $current['page_path'];
                $prev         = $prevLookup[$path] ?? null;
                $sessionDelta = $prev ? ($current['sessions'] - $prev['sessions']) : 'N/A';
                $convDelta    = $prev ? ($current['conversions'] - ($prev['conversions'] ?? 0)) : 'N/A';
                $intro .= '- ' . $path . ' | Sessions: ' . $current['sessions'] . ' (Δ' . $sessionDelta . ') | Conversions: ' . $current['conversions'] . ' (Δ' . $convDelta . ")\n";
            }
        }

        if (!empty($landingPages)) {
            $intro .= "\n\nTOP LANDING PAGES:\n";
            foreach (array_slice($landingPages, 0, 8) as $row) {
                $intro .= '- ' . $row['page_path'] . ' | Sessions: ' . $row['sessions'] . ' | Bounce: ' . round($row['bounce_rate'] * 100, 1) . '% | Engagement: ' . round($row['avg_engagement_time'], 0) . "s | Conversions: " . $row['conversions'] . "\n";
            }
        }

        if (!empty($topPages) && isset($topPages[0]['avg_engagement_time'])) {
            $intro .= "\n\nENGAGEMENT METRICS:\n";
            foreach (array_slice($topPages, 0, 8) as $row) {
                $engRate = ($row['sessions'] > 0) ? round(($row['engaged_sessions'] / $row['sessions']) * 100, 1) : 0;
                $intro .= '- ' . $row['page_path'] . ' | Engagement: ' . round($row['avg_engagement_time'] ?? 0, 0) . 's | Rate: ' . $engRate . '% | Bounce: ' . round(($row['bounce_rate'] ?? 0) * 100, 1) . "%\n";
            }
        }

        // ── Google Ads data ──
        if (!empty($adsCampaigns)) {
            $intro .= "\n\nGOOGLE ADS CAMPAIGNS (30d):\n";
            foreach ($adsCampaigns as $row) {
                $spend = '$' . number_format($row['cost_micros'] / 1000000, 2);
                $cpc   = '$' . number_format($row['average_cpc'] / 1000000, 2);
                $intro .= '- ' . $row['campaign_name'] . ' | Spend: ' . $spend . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | CPC: ' . $cpc . ' | Conversions: ' . $row['conversions'] . ' | Status: ' . $row['status'] . "\n";
            }
        }

        if (!empty($adsKeywords)) {
            $intro .= "\n\nTOP GOOGLE ADS KEYWORDS (30d by spend):\n";
            foreach (array_slice($adsKeywords, 0, 10) as $row) {
                $spend = '$' . number_format($row['cost_micros'] / 1000000, 2);
                $cpc   = '$' . number_format($row['average_cpc'] / 1000000, 2);
                $intro .= '- "' . $row['keyword'] . '" [' . $row['match_type'] . '] | Spend: ' . $spend . ' | Clicks: ' . $row['clicks'] . ' | CPC: ' . $cpc . ' | Conversions: ' . $row['conversions'] . "\n";
            }
        }

        if (!empty($adsSearchTerms)) {
            $intro .= "\n\nTOP SEARCH TERMS TRIGGERING ADS (30d):\n";
            foreach (array_slice($adsSearchTerms, 0, 10) as $row) {
                $spend = '$' . number_format($row['cost_micros'] / 1000000, 2);
                $intro .= '- "' . $row['search_term'] . '" | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Spend: ' . $spend . ' | Conversions: ' . $row['conversions'] . "\n";
            }
        }

        if (!empty($adsDailySpend)) {
            $totalSpend = array_sum(array_column($adsDailySpend, 'cost_micros')) / 1000000;
            $intro .= "\n\nGOOGLE ADS TOTAL SPEND (last 14 days): $" . number_format($totalSpend, 2) . "\n";
        }

        $intro .= $taskContext;
        $intro .= $recheckContext;
        $intro .= "\n\n" . $staticRules;

        return $intro;
    }
}

    
