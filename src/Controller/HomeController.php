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

        $topQueries = $this->db->fetchAllAssociative(
            'SELECT query, page, clicks, impressions, position FROM gsc_snapshots ORDER BY impressions DESC LIMIT 20'
        );

        $topPages = $this->db->fetchAllAssociative(
            'SELECT page_path, sessions, pageviews, conversions FROM ga4_snapshots ORDER BY sessions DESC LIMIT 20'
        );

        $activeTasks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, assigned_role, status, priority, estimated_hours, logged_hours, created_at FROM tasks WHERE status != 'done' ORDER BY created_at DESC LIMIT 10"
        );

        $pendingRechecks = $this->db->fetchAllAssociative(
            "SELECT id, title, assigned_to, recheck_date, recheck_type FROM tasks WHERE status = 'done' AND recheck_date IS NOT NULL AND recheck_verified = false AND recheck_date <= CURRENT_DATE + INTERVAL '3 days' ORDER BY recheck_date ASC LIMIT 5"
        );

        $systemPrompt = $this->buildSystemPrompt(
            $semrush ?: [], $topQueries, $topPages, $userName, $userRole, $activeTasks, $pendingRechecks
        );

        $response = file_get_contents('https://api.anthropic.com/v1/messages', false, stream_context_create(array(
            'http' => array(
                'method'        => 'POST',
                'header'        => implode("\r\n", array(
                    'Content-Type: application/json',
                    'x-api-key: ' . $_ENV['ANTHROPIC_API_KEY'],
                    'anthropic-version: 2023-06-01',
                )),
                'content'       => json_encode(array(
                    'model'      => $_ENV['CLAUDE_MODEL'] ?? 'claude-sonnet-4-6',
                    'max_tokens' => 2048,
                    'system'     => $systemPrompt,
                    'messages'   => $messages,
                )),
                'ignore_errors' => true,
            ),
        )));

        $data = json_decode($response, true);

        if (isset($data['error'])) {
            return new JsonResponse(array('error' => $data['error']['message']), 500);
        }

        $text = $data['content'][0]['text'] ?? 'No response from Claude.';

        return new JsonResponse(array('response' => $text));
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
        array $pendingRechecks
    ): string {
        $date = date('l, F j, Y');

        $querySummary = '';
        foreach (array_slice($topQueries, 0, 20) as $row) {
            $querySummary .= '- "' . $row['query'] . '" | Page: ' . $row['page'] . ' | Clicks: ' . $row['clicks'] . ' | Impressions: ' . $row['impressions'] . ' | Position: ' . round($row['position'], 1) . "\n";
        }

        $pageSummary = '';
        foreach (array_slice($topPages, 0, 20) as $row) {
            $pageSummary .= '- ' . $row['page_path'] . ' | Sessions: ' . $row['sessions'] . ' | Pageviews: ' . $row['pageviews'] . ' | Conversions: ' . $row['conversions'] . "\n";
        }

        $keywords = $semrush['organic_keywords'] ?? 'N/A';
        $traffic  = $semrush['organic_traffic'] ?? 'N/A';
        $updated  = $semrush['fetched_at'] ?? 'N/A';

        $taskContext = '';
        if (!empty($activeTasks)) {
            $taskContext .= "\n\nACTIVE TASKS IN SYSTEM:\n";
            foreach ($activeTasks as $t) {
                $logged = floatval($t['logged_hours'] ?? 0);
                $est = floatval($t['estimated_hours'] ?? 0);
                $timeInfo = $est > 0 ? " | Time: {$logged}/{$est}h" : "";
                $taskContext .= "- [" . strtoupper($t['priority']) . "] " . $t['title'] . " | Assigned: " . ($t['assigned_to'] ?? 'Unassigned') . " | Status: " . $t['status'] . $timeInfo . "\n";
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

        $intro  = 'You are Logiri, an SEO intelligence assistant built specifically for Double D Trailers (doubledtrailers.com).';
        $intro .= ' You help the internal team identify and act on SEO issues using real data from SEMrush, Google Search Console, and Google Analytics 4.';
        $intro .= "\n\nToday is " . $date . '.';
        $intro .= "\n\nCURRENT USER: " . $userName . " | Role: " . $userRole;
        $intro .= "\nPersonalize your response for this user. Address them by name. Prioritize tasks relevant to their role.";
        $intro .= "\n\nCURRENT DATA SNAPSHOT:";
        $intro .= "\nSEMrush Overview:";
        $intro .= "\n- Organic Keywords: " . $keywords;
        $intro .= "\n- Organic Traffic: " . $traffic;
        $intro .= "\n- Last updated: " . $updated;
        $intro .= "\n\nTop GSC Queries (last 28 days):\n" . $querySummary;
        $intro .= "\nTop GA4 Pages (last 28 days):\n" . $pageSummary;
        $intro .= $taskContext;
        $intro .= $recheckContext;
        $intro .= "\n\n" . $staticRules;

        return $intro;
    }
}

    
