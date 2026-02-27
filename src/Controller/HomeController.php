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

        // Full GSC data by date range
        $topQueries28d = $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE date_range = '28d' ORDER BY impressions DESC LIMIT 50"
        );
        $topQueries90d = $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE date_range = '90d' ORDER BY impressions DESC LIMIT 30"
        );
        $pageAggregates = $this->db->fetchAllAssociative(
            "SELECT page, clicks, impressions, ctr, position FROM gsc_snapshots WHERE query = '__PAGE_AGGREGATE__' ORDER BY impressions DESC LIMIT 30"
        );
        $brandedQueries = $this->db->fetchAllAssociative(
            "SELECT query, page, clicks, impressions, position FROM gsc_snapshots WHERE date_range = '28d_branded' ORDER BY impressions DESC LIMIT 20"
        );

        // Cannibalization: find queries appearing on multiple pages
        $cannibalizationCandidates = $this->db->fetchAllAssociative(
            "SELECT query, COUNT(DISTINCT page) as page_count, SUM(impressions) as total_impressions
             FROM gsc_snapshots WHERE date_range = '28d' AND query != '__PAGE_AGGREGATE__'
             GROUP BY query HAVING COUNT(DISTINCT page) > 1
             ORDER BY total_impressions DESC LIMIT 30"
        );

        // GA4 current + comparison
        $topPages = $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, pageviews, bounce_rate, avg_engagement_time, engaged_sessions, conversions
             FROM ga4_snapshots WHERE date_range = '28d' ORDER BY sessions DESC LIMIT 30"
        );
        $previousPages = $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, pageviews, bounce_rate, avg_engagement_time, conversions
             FROM ga4_snapshots WHERE date_range = '28d_previous' ORDER BY sessions DESC LIMIT 30"
        );
        $landingPages = $this->db->fetchAllAssociative(
            "SELECT page_path, sessions, bounce_rate, avg_engagement_time, conversions
             FROM ga4_snapshots WHERE date_range = '28d_landing' ORDER BY sessions DESC LIMIT 20"
        );

        // For backward compat, pass topQueries28d as topQueries
        $topQueries = $topQueries28d;

        $activeTasks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, assigned_role, status, priority, estimated_hours, logged_hours, created_at FROM tasks WHERE status != 'done' ORDER BY created_at DESC LIMIT 10"
        );

        $pendingRechecks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, recheck_date, recheck_type FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false AND recheck_date <= CURRENT_DATE + INTERVAL '3 days' ORDER BY recheck_date ASC LIMIT 5"
        );

        $systemPrompt = $this->buildSystemPrompt(
            $semrush ?: [], $topQueries, $topPages, $userName, $userRole, $activeTasks, $pendingRechecks,
            $topQueries90d, $pageAggregates, $brandedQueries, $cannibalizationCandidates,
            $previousPages, $landingPages
        );

       // Gemini expects system prompt prepended to messages
$geminiMessages = [];
foreach ($messages as $msg) {
    $geminiMessages[] = [
        'role'  => $msg['role'] === 'assistant' ? 'model' : 'user',
        'parts' => [['text' => $msg['content']]],
    ];
}

// Prepend system prompt as first user message if no messages yet
array_unshift($geminiMessages, [
    'role'  => 'user',
    'parts' => [['text' => $systemPrompt]],
]);
array_splice($geminiMessages, 1, 0, [[
    'role'  => 'model',
    'parts' => [['text' => 'Understood. I am Logiri, ready to assist.']],
]]);

$geminiModel = $_ENV['GEMINI_MODEL'] ?? 'gemini-2.5-flash';
$geminiKey   = $_ENV['GEMINI_API_KEY'] ?? '';

$response = file_get_contents(
    "https://generativelanguage.googleapis.com/v1beta/models/{$geminiModel}:generateContent?key={$geminiKey}",
    false,
    stream_context_create([
        'http' => [
            'method'        => 'POST',
            'header'        => 'Content-Type: application/json',
            'content'       => json_encode([
                'contents'          => $geminiMessages,
                'generationConfig'  => [
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

                    // Check for duplicate: skip if a task with similar title already exists and isn't done
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

        return new JsonResponse(array(
            'response' => $text,
            'tasks_created' => $tasksCreated,
        ));
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
            'title' => $body['title'] ?? 'Untitled Task',
            'description' => $body['description'] ?? null,
            'rule_id' => $body['rule_id'] ?? null,
            'assigned_to' => $body['assigned_to'] ?? null,
            'assigned_role' => $body['assigned_role'] ?? null,
            'status' => 'pending',
            'priority' => $body['priority'] ?? 'medium',
            'estimated_hours' => $body['estimated_hours'] ?? 1,
            'logged_hours' => 0,
            'due_date' => $body['due_date'] ?? null,
            'recheck_type' => $body['recheck_type'] ?? null,
            'created_at' => date('Y-m-d H:i:s'),
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
            '404_fix' => 7,
            'sitemap_fix' => 7,
            'cannibalization_fix' => 14,
            'homepage_cannibalization' => 14,
            'intent_mismatch' => 14,
            'weak_page' => 14,
            'zero_click' => 14,
            'ranking_drop' => 28,
            default => 14,
        };

        $recheckDate = date('Y-m-d', strtotime("+{$recheckDays} days"));

        $this->db->update('tasks', [
            'status' => 'done',
            'completed_at' => date('Y-m-d H:i:s'),
            'recheck_date' => $recheckDate,
        ], ['id' => $id]);

        $updated = $this->db->fetchAssociative('SELECT * FROM tasks WHERE id = ?', [$id]);

        return new JsonResponse([
            'task' => $updated,
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
            'recheck_result' => $passed ? 'pass' : 'fail',
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
        array $landingPages = []
    ): string {
        $date = date('l, F j, Y');

        $querySummary = '';
        foreach (array_slice($topQueries, 0, 20) as $row) {
            $querySummary .= '- "' . $row['query'] . '" | Page: ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Position: ' . round($row['position'], 1) . "\n";
        }

        $pageSummary = '';
        foreach (array_slice($topPages, 0, 20) as $row) {
            $engTime = isset($row['avg_engagement_time']) ? ' | Engagement: ' . round($row['avg_engagement_time'], 0) . 's' : '';
            $pageSummary .= '- ' . $row['page_path'] . ' | Sessions: ' . $row['sessions'] . ' | Pageviews: ' . $row['pageviews'] . ' | Conversions: ' . $row['conversions'] . $engTime . "\n";
        }

        $keywords = $semrush['organic_keywords'] ?? 'N/A';
        $traffic  = $semrush['organic_traffic'] ?? 'N/A';
        $updated  = $semrush['fetched_at'] ?? 'N/A';

        $taskContext = '';
        if (!empty($activeTasks)) {
            $taskContext .= "\n\nACTIVE TASKS IN SYSTEM:\n";

            // Calculate per-person workload
            $workload = [];
            $overdueNudges = [];

            foreach ($activeTasks as $t) {
                $logged = floatval($t['logged_hours'] ?? 0);
                $est = floatval($t['estimated_hours'] ?? 0);
                $assignee = $t['assigned_to'] ?? 'Unassigned';
                $status = $t['status'];
                $timeInfo = $est > 0 ? " | Time: {$logged}/{$est}h" : "";

                // Track workload per person
                if ($assignee !== 'Unassigned' && $status !== 'done') {
                    $workload[$assignee] = ($workload[$assignee] ?? 0) + $est;
                }

                // Detect over-estimate tasks for nudging
                if ($logged > $est && $est > 0 && $status !== 'done') {
                    $overdueNudges[] = "⚠️ OVER-ESTIMATE: \"{$t['title']}\" — {$logged}h logged vs {$est}h estimated, still {$status}. Assigned: {$assignee}";
                }

                $taskContext .= "- [" . strtoupper($t['priority']) . "] " . $t['title'] . " | Assigned: " . $assignee . " | Status: " . $status . $timeInfo . "\n";
            }

            // Capacity summary
            $taskContext .= "\nTEAM WORKLOAD (open task hours / 40h capacity):\n";
            foreach (['Brook', 'Kalib', 'Brad'] as $name) {
                $load = $workload[$name] ?? 0;
                $status = $load > 40 ? '🔴 OVERLOADED' : ($load > 30 ? '🟡 HIGH' : '🟢 OK');
                $taskContext .= "- {$name}: {$load}h assigned | {$status}\n";
            }

            // Nudges for over-estimate tasks
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

        $promptFile = dirname(__DIR__, 2) . '/system-prompt.txt';
        $staticRules = file_exists($promptFile) ? file_get_contents($promptFile) : '';

        $intro  = "You are Logiri, the AI Chief of Staff for Double D Trailers (doubledtrailers.com).";
        $intro .= "\n\nYOUR PERSONA & BEHAVIOR:";
        $intro .= "\n- You are an authoritative yet encouraging Project Manager. You are the 'Chief of Staff' for this team.";
        $intro .= "\n- You don't just provide dry data. You are focused on deadlines, accountability, and moving work forward.";
        $intro .= "\n- Address the user by name. Be professional, direct, and action-oriented.";
        $intro .= "\n- When giving briefings, lead with the MOST URGENT items first (overdue tasks, capacity issues, critical incidents).";
        $intro .= "\n- When analyzing SEO data, always connect findings to ACTIONABLE TASKS with specific owners.";
        $intro .= "\n\nTASK GENERATION RULES:";
        $intro .= "\n- When you identify SEO issues or give a briefing, generate ACTIONABLE TASKS.";
        $intro .= "\n- At the END of your response, include a hidden JSON block with all tasks you recommend.";
        $intro .= "\n- Format: wrap the JSON in <!-- TASKS_JSON --> and <!-- /TASKS_JSON --> tags.";
        $intro .= "\n- The JSON must be a valid array of task objects.";
        $intro .= "\n- Each task object has: title, assigned_to, priority (critical/high/medium/low), estimated_hours (min 0.5), recheck_type (optional: cannibalization_fix, intent_mismatch, ranking_drop, 404_fix, sitemap_fix, or null), description (brief).";
        $intro .= "\n- In your VISIBLE response, present tasks in readable format with context and reasoning.";
        $intro .= "\n- Only generate tasks for NEW issues. Do NOT duplicate tasks that already exist in ACTIVE TASKS (shown below).";
        $intro .= "\n- Assign to the person with the matching role and lowest current workload.";
        $intro .= "\n- Estimates are working hours. Be realistic. Minimum 0.5h.";
        $intro .= "\n\nExample of the hidden block at end of response:";
        $intro .= "\n<!-- TASKS_JSON -->";
        $intro .= "\n[{\"title\":\"Resolve horse trailers for sale cannibalization\",\"assigned_to\":\"Brook\",\"priority\":\"critical\",\"estimated_hours\":3,\"recheck_type\":\"cannibalization_fix\",\"description\":\"De-optimize 9 competing pages, consolidate signals to homepage\"}]";
        $intro .= "\n<!-- /TASKS_JSON -->";
        $intro .= $intro .= "\n\nCRITICAL INSTRUCTION: Structure EVERY response like this:\n1. First line: <!-- TASKS_JSON --> [array of tasks] <!-- /TASKS_JSON -->\n2. Then your briefing text\n\nNever omit the TASKS_JSON block. It must always be the very first thing in your response.";
        $intro .= "\n\nCAPACITY & WORKLOAD AWARENESS:";
        $intro .= "\n- Each team member has 40 hours/week capacity.";
        $intro .= "\n- A user is OVERLOADED if sum(estimated_hours of open tasks) > 40.";
        $intro .= "\n- Flag overload in briefings. Suggest rebalancing when someone is overloaded.";
        $intro .= "\n- When a task has logged_hours > estimated_hours and status is NOT done, flag it as OVER-ESTIMATE with a nudge.";
        $intro .= "\n\nPROACTIVE NUDGE FORMAT (for tasks exceeding estimates):";
        $intro .= "\n  ⚠️ **Estimate exceeded** — Task \"[title]\" has [logged]h logged against [est]h estimated, still marked as [status].";
        $intro .= "\n  Please provide: 1) an updated ETA, 2) a brief note on what is blocking completion.";
        $intro .= "\n\nINCIDENT & RECHECK LIFECYCLE:";
        $intro .= "\n- When a rule triggers an issue, it creates an INCIDENT with evidence.";
        $intro .= "\n- Incidents generate remediation TASKS with time estimates.";
        $intro .= "\n- Completed tasks are RECHECKED after the appropriate interval (7-28 days depending on type).";
        $intro .= "\n- Do NOT suggest creating duplicate incidents for issues that already have active tasks.";
        $intro .= "\n\nTASK STATUSES: pending (backlog/todo), in_progress, blocked, done";
        $intro .= "\nTASK PRIORITIES: critical, high, medium, low";
        $intro .= "\n\nTEAM ROSTER:";
        $intro .= "\n- Brook | Role: SEO + Content | Capacity: 40h/week";
        $intro .= "\n- Kalib | Role: Sales | Capacity: 40h/week";
        $intro .= "\n- Brad | Role: Marketing | Capacity: 40h/week";
        $intro .= "\n\nToday is " . $date . '.';
        $intro .= "\n\nCURRENT USER: " . $userName . " | Role: " . $userRole;
        $intro .= "\nPersonalize your response for this user. Prioritize tasks relevant to their role.";
        $intro .= "\n\nCURRENT DATA SNAPSHOT:";
        $intro .= "\nSEMrush Overview:";
        $intro .= "\n- Organic Keywords: " . $keywords;
        $intro .= "\n- Organic Traffic: " . $traffic;
        $intro .= "\n- Last updated: " . $updated;
        $intro .= "\n\nTop GSC Queries (last 28 days):\n" . $querySummary;
        $intro .= "\nTop GA4 Pages (last 28 days):\n" . $pageSummary;

        // ── 90-day GSC trends (for algorithm update detection) ──
        if (!empty($topQueries90d)) {
            $intro .= "\n\n90-DAY GSC QUERY TRENDS (for AU1/AU2 algorithm update detection):\n";
            foreach (array_slice($topQueries90d, 0, 20) as $row) {
                $intro .= '- "' . $row['query'] . '" | Page: ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Position: ' . round($row['position'], 1) . "\n";
            }
        }

        // ── Page-level aggregates (for consolidation rules CON-R1 through CON-R6) ──
        if (!empty($pageAggregates)) {
            $intro .= "\n\nGSC PAGE AGGREGATES (for CON rules - zero-click, weak pages):\n";
            foreach (array_slice($pageAggregates, 0, 20) as $row) {
                $intro .= '- ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | CTR: ' . round($row['ctr'] * 100, 1) . '% | Position: ' . round($row['position'], 1) . "\n";
            }
        }

        // ── Branded queries (for BE1/BE2 brand/entity rules) ──
        if (!empty($brandedQueries)) {
            $intro .= "\n\nBRANDED QUERIES (for BE1/BE2 brand entity rules):\n";
            foreach (array_slice($brandedQueries, 0, 15) as $row) {
                $intro .= '- "' . $row['query'] . '" | Page: ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . "\n";
            }
        }

        // ── Cannibalization candidates (for C-R1 through C-R5) ──
        if (!empty($cannibalizationCandidates)) {
            $intro .= "\n\nCANNIBALIZATION CANDIDATES (queries ranking on multiple pages - C-R1 through C-R5):\n";
            foreach (array_slice($cannibalizationCandidates, 0, 20) as $row) {
                $intro .= '- "' . $row['query'] . '" → ' . $row['page_count'] . ' pages competing | Total impressions: ' . $row['total_impressions'] . "\n";
            }
        }

        // ── GA4 period comparison (for CP1/CP2 content performance rules) ──
        if (!empty($previousPages)) {
            $intro .= "\n\nGA4 PERIOD COMPARISON (current 28d vs previous 28d - for CP1/CP2 rules):\n";
            // Build lookup of previous period
            $prevLookup = [];
            foreach ($previousPages as $p) { $prevLookup[$p['page_path']] = $p; }
            foreach (array_slice($topPages, 0, 15) as $current) {
                $path = $current['page_path'];
                $prev = $prevLookup[$path] ?? null;
                $sessionDelta = $prev ? ($current['sessions'] - $prev['sessions']) : 'N/A';
                $convDelta = $prev ? ($current['conversions'] - ($prev['conversions'] ?? 0)) : 'N/A';
                $intro .= '- ' . $path . ' | Sessions: ' . $current['sessions'] . ' (Δ ' . $sessionDelta . ') | Conversions: ' . $current['conversions'] . ' (Δ ' . $convDelta . ")\n";
            }
        }

        // ── Landing page performance (for conversion optimization) ──
        if (!empty($landingPages)) {
            $intro .= "\n\nTOP LANDING PAGES WITH ENGAGEMENT:\n";
            foreach (array_slice($landingPages, 0, 15) as $row) {
                $intro .= '- ' . $row['page_path'] . ' | Sessions: ' . $row['sessions'] . ' | Bounce: ' . round($row['bounce_rate'] * 100, 1) . '% | Avg Engagement: ' . round($row['avg_engagement_time'], 0) . "s | Conversions: " . $row['conversions'] . "\n";
            }
        }

        // ── GA4 Engagement metrics for top pages ──
        if (!empty($topPages) && isset($topPages[0]['avg_engagement_time'])) {
            $intro .= "\n\nENGAGEMENT METRICS (for scroll depth/time on page analysis):\n";
            foreach (array_slice($topPages, 0, 10) as $row) {
                $engRate = ($row['sessions'] > 0) ? round(($row['engaged_sessions'] / $row['sessions']) * 100, 1) : 0;
                $intro .= '- ' . $row['page_path'] . ' | Avg Engagement: ' . round($row['avg_engagement_time'] ?? 0, 0) . 's | Engagement Rate: ' . $engRate . '% | Bounce: ' . round(($row['bounce_rate'] ?? 0) * 100, 1) . "%\n";
            }
        }
        $intro .= $taskContext;
        $intro .= $recheckContext;
        $intro .= "\n\n" . $staticRules;

        return $intro;
    }
}