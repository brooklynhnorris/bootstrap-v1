<?php

namespace App\Controller;

use App\Service\ActionRequestService;
use App\Service\ClaudeChatService;
use App\Service\ConversationService;
use App\Service\LearningExtractionService;
use App\Service\PromptBuilderService;
use App\Service\RuleContextService;
use App\Service\TaskSuggestionService;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\RequestStack;
use Symfony\Component\Routing\Annotation\Route;
use Doctrine\DBAL\Connection;

class HomeController extends AbstractController
{
    public function __construct(
        private Connection $db,
        private RequestStack $requestStack,
        private ActionRequestService $actionRequestService,
        private ClaudeChatService $claudeChatService,
        private ConversationService $conversationService,
        private LearningExtractionService $learningExtractionService,
        private PromptBuilderService $promptBuilderService,
        private RuleContextService $ruleContextService,
        private TaskSuggestionService $taskSuggestionService
    ) {
    }

    private function ensureSchema(): void
    {
    }

    #[Route('/api/admin/clear-slate', name: 'clear_slate', methods: ['POST'])]
    public function clearSlate(): JsonResponse
    {
        $this->denyAccessUnlessGranted('ROLE_ADMIN');

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

        // Detect if this is a casual/conversational message that doesn't need data
        $isCasual = strlen($lastUserMsg) < 30 && !str_contains($lastUserMsg, 'briefing') 
                    && !str_contains($lastUserMsg, 'task') && !str_contains($lastUserMsg, 'play')
                    && !str_contains($lastUserMsg, 'rule') && !str_contains($lastUserMsg, 'crawl')
                    && !str_contains($lastUserMsg, 'signal') && !str_contains($lastUserMsg, '/')
                    && !str_contains($lastUserMsg, 'what should') && !str_contains($lastUserMsg, 'status');

        $isBriefingRequest = str_contains($lastUserMsg, 'briefing') || str_contains($lastUserMsg, 'what should')
                           || str_contains($lastUserMsg, 'overview') || str_contains($lastUserMsg, 'status')
                           || str_contains($lastUserMsg, 'summary') || str_contains($lastUserMsg, 'fresh task')
                           || str_contains($lastUserMsg, 'give me') || str_contains($lastUserMsg, 'generate');

        $isPlayOpen = str_contains($lastUserMsg, 'i just opened the play');

        $needsGsc     = !$isCasual && !$isPlayOpen && (str_contains($lastUserMsg, 'gsc') || str_contains($lastUserMsg, 'ranking')
                      || str_contains($lastUserMsg, 'position') || str_contains($lastUserMsg, 'impression')
                      || str_contains($lastUserMsg, 'traffic') || str_contains($lastUserMsg, 'serp')
                      || $isBriefingRequest);
        $needsGa4     = !$isCasual && !$isPlayOpen && (str_contains($lastUserMsg, 'ga4') || str_contains($lastUserMsg, 'analytics') || str_contains($lastUserMsg, 'bounce')
                      || str_contains($lastUserMsg, 'engagement') || str_contains($lastUserMsg, 'session') || str_contains($lastUserMsg, 'conversion')
                      || $isBriefingRequest);
        $needsAds     = !$isCasual && (str_contains($lastUserMsg, 'ads') || str_contains($lastUserMsg, 'google ads') || str_contains($lastUserMsg, 'campaign')
                      || str_contains($lastUserMsg, 'ppc') || str_contains($lastUserMsg, 'spend') || str_contains($lastUserMsg, 'cpc'));
        $needsCrawl   = !$isCasual && !$isPlayOpen && (str_contains($lastUserMsg, 'crawl') || str_contains($lastUserMsg, 'page') || str_contains($lastUserMsg, 'schema')
                      || str_contains($lastUserMsg, 'h1') || str_contains($lastUserMsg, 'title')
                      || str_contains($lastUserMsg, 'link') || str_contains($lastUserMsg, 'entity') || str_contains($lastUserMsg, 'word count')
                      || str_contains($lastUserMsg, 'play') || str_contains($lastUserMsg, 'signal')
                      || str_contains($lastUserMsg, 'fix') || str_contains($lastUserMsg, '/')
                      || $isBriefingRequest);
        $needsRules   = !$isCasual && !$isPlayOpen && (str_contains($lastUserMsg, 'rule') || str_contains($lastUserMsg, 'proposal') || str_contains($lastUserMsg, 'learning')
                      || str_contains($lastUserMsg, 'approve') || str_contains($lastUserMsg, 'reject')
                      || $isBriefingRequest);
        // Only load the full URL list when we actually need link targets (briefings, not play cards)
        $needsUrlList = $isBriefingRequest && $needsCrawl;

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
        $allCrawledUrls = $needsUrlList ? $this->loadAllCrawledUrls() : [];

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
            if ($playUrl && substr($playUrl, -1) !== '/') {
                // Ensure trailing slash for DB matching
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
        $recentReviews = $needsRules ? $this->ruleContextService->loadRecentReviews() : [];
        $overrideCount = $needsRules ? $this->ruleContextService->loadOverrideCount() : 0;

        // ── Load verification outcomes for learning context ──
        $verificationResults = $needsRules ? $this->ruleContextService->loadVerificationResults() : [];
        $ruleFeedback = $needsRules ? $this->ruleContextService->loadRuleFeedback() : [];
        $ruleProposals = $needsRules ? $this->ruleContextService->loadRuleProposals() : [];

        // ── Persist conversation ──
        try {
            $conversationId = $this->conversationService->ensureConversation(
                $conversationId ? (int) $conversationId : null,
                $userId,
                $messages,
                $this->requestStack->getSession()->get('active_persona', null)
            );
        } catch (\RuntimeException $e) {
            return new JsonResponse(['error' => $e->getMessage()], 404);
        }

        // Save the latest user message
        $lastMsg = end($messages);
        $this->conversationService->saveUserMessage((int) $conversationId, is_array($lastMsg) ? $lastMsg : null);

        // ── Build system prompt ──
        $systemPrompt = $this->promptBuilderService->buildSystemPrompt(
            $semrush ?: [], $topQueries28d, $topPages, $userName, $userRole,
            $activeTasks, $pendingRechecks, $topQueries90d, $pageAggregates,
            $brandedQueries, $cannibalizationCandidates, $previousPages, $landingPages,
            $adsCampaigns, $adsKeywords, $adsSearchTerms, $adsDailySpend,
            $recentReviews, $overrideCount, $crawlData, $allCrawledUrls,
            $verificationResults, $ruleFeedback, $ruleProposals, $playUrlData
        );

        // ── Call Claude API ──
        try {
            $claudeResponse = $this->claudeChatService->sendChat($systemPrompt, $messages);
            $text = $claudeResponse['text'];
            $claudeKey = $claudeResponse['api_key'];
        } catch (\RuntimeException $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }

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
            $nlpResult = $this->claudeChatService->validateEntityAlignment($text, $lastUserMsg, $claudeKey);
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
        $llmActionsEnabled = $this->llmActionsEnabled();
        if (preg_match('/<!-- ACTIONS_JSON -->\s*(.*?)\s*<!-- \/ACTIONS_JSON -->/s', $text, $actionMatches)) {
            $actions = json_decode(trim($actionMatches[1]), true);
            if (is_array($actions)) {
                $queuedActions = $this->actionRequestService->queueMany($actions, 'llm');
                if (!empty($queuedActions)) {
                    $actionsExecuted = array_merge($actionsExecuted, $queuedActions);
                }

                if (empty($queuedActions)) {
                    $actionsExecuted[] = 'LLM actions were proposed, but action request queueing is unavailable until migrations are run.';
                } elseif ($llmActionsEnabled) {
                    $actionsExecuted[] = 'LLM actions were queued for approval instead of executing immediately.';
                } else {
                    $actionsExecuted[] = 'LLM actions were queued for review; LOGIRI_ENABLE_LLM_ACTIONS is disabled so nothing executed automatically.';
                }
            }
            if (!is_array($actions)) {
                $actionsExecuted[] = 'LLM actions were proposed but not executed because LOGIRI_ENABLE_LLM_ACTIONS is disabled.';
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
        $taskResult = $this->taskSuggestionService->createTasksFromResponse($text, $crawlData);
        $text = $taskResult['text'];
        $tasksCreated = $taskResult['tasks_created'];

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
        $this->conversationService->saveAssistantMessage((int) $conversationId, $text);

        // ── Extract learnings from conversation (runs after every chat exchange) ──
        if (count($messages) >= 2 && $claudeKey) {
            try {
                $this->learningExtractionService->extractLearnings($messages, $text, $claudeKey, $userName);
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
        if (!$this->findOwnedConversation($id, $this->getCurrentUserId())) {
            return new JsonResponse(['error' => 'Not found'], 404);
        }

        $msgs = $this->db->fetchAllAssociative(
            'SELECT role, content, created_at FROM messages WHERE conversation_id = ? ORDER BY created_at ASC',
            [$id]
        );
        return new JsonResponse($msgs);
    }

    #[Route('/api/conversations/{id}/archive', name: 'api_conversation_archive', methods: ['POST'])]
    public function archiveConversation(int $id): JsonResponse
    {
        if (!$this->findOwnedConversation($id, $this->getCurrentUserId())) {
            return new JsonResponse(['error' => 'Not found'], 404);
        }

        $this->db->executeStatement('UPDATE conversations SET is_archived = TRUE WHERE id = ?', [$id]);
        return new JsonResponse(['ok' => true]);
    }

    #[Route('/api/conversations/{id}/delete', name: 'api_conversation_delete', methods: ['POST'])]
    public function deleteConversation(int $id): JsonResponse
    {
        if (!$this->findOwnedConversation($id, $this->getCurrentUserId())) {
            return new JsonResponse(['error' => 'Not found'], 404);
        }

        $this->db->executeStatement('DELETE FROM messages WHERE conversation_id = ?', [$id]);
        $this->db->executeStatement('DELETE FROM conversations WHERE id = ?', [$id]);
        return new JsonResponse(['ok' => true]);
    }

    #[Route('/api/conversations/{id}/rename', name: 'api_conversation_rename', methods: ['POST'])]
    public function renameConversation(int $id, Request $request): JsonResponse
    {
        if (!$this->findOwnedConversation($id, $this->getCurrentUserId())) {
            return new JsonResponse(['error' => 'Not found'], 404);
        }

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
                $actor = $this->getCurrentActorName();
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
                if (preg_match('|(/[a-z0-9][a-z0-9_-]+(?:/[a-z0-9_-]+)*/)|i', $task['title'] ?? '', $urlMatch)) {
                    $taskUrl = $urlMatch[1];
                }

                // ── URL SUPPRESSION: Prevent regeneration of dismissed tasks ──
                // When a task is dismissed as false_positive or not_applicable,
                // suppress that URL+rule combo so it never generates a new task.
                if ($taskUrl && $ruleId && in_array($dismissType, ['false_positive', 'not_applicable', 'invalid'])) {
                    try {
                        // Create suppression table if it doesn't exist
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
                    if (preg_match('|(/[a-z0-9][a-z0-9_-]+(?:/[a-z0-9_-]+)*/)|i', $task['title'] ?? '', $urlMatch)) {
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

    #[Route('/api/action-requests', name: 'api_action_requests_list', methods: ['GET'])]
    public function listActionRequests(Request $request): JsonResponse
    {
        $this->denyAccessUnlessGranted('ROLE_ADMIN');

        $status = $request->query->get('status');
        $limit = min((int) ($request->query->get('limit') ?? 50), 200);

        return new JsonResponse($this->actionRequestService->listRequests($status ?: null, $limit));
    }

    #[Route('/api/action-requests/{id}/approve', name: 'api_action_requests_approve', methods: ['POST'])]
    public function approveActionRequest(int $id): JsonResponse
    {
        $this->denyAccessUnlessGranted('ROLE_ADMIN');

        try {
            $actor = $this->getCurrentActorName();
            $result = $this->actionRequestService->approve($id, $actor);
            $request = $result['request'] ?? null;
            $summary = $result['summary'] ?? 'Action request executed.';

            if ($request) {
                $this->logActivity(
                    $actor,
                    'approved_action_request',
                    $request['target_type'] ?? 'action_request',
                    null,
                    (string) ($request['target_id'] ?? $request['action_type'] ?? $id),
                    $summary
                );
            }

            return new JsonResponse([
                'ok' => true,
                'message' => $summary,
                'request' => $request,
            ]);
        } catch (\Throwable $e) {
            return new JsonResponse(['error' => $e->getMessage()], 400);
        }
    }

    #[Route('/api/action-requests/{id}/reject', name: 'api_action_requests_reject', methods: ['POST'])]
    public function rejectActionRequest(int $id, Request $request): JsonResponse
    {
        $this->denyAccessUnlessGranted('ROLE_ADMIN');

        try {
            $body = json_decode($request->getContent(), true) ?: [];
            $reason = $body['reason'] ?? null;
            $actor = $this->getCurrentActorName();
            $updated = $this->actionRequestService->reject($id, $actor, $reason);

            $this->logActivity(
                $actor,
                'rejected_action_request',
                $updated['target_type'] ?? 'action_request',
                null,
                (string) ($updated['target_id'] ?? $updated['action_type'] ?? $id),
                $reason ?: 'Rejected by reviewer'
            );

            return new JsonResponse([
                'ok' => true,
                'request' => $updated,
            ]);
        } catch (\Throwable $e) {
            return new JsonResponse(['error' => $e->getMessage()], 400);
        }
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

    private function loadCrawlData(): array
    {
        try {
            $deterministicRows = $this->loadDeterministicCrawlData();
            if (!empty($deterministicRows)) {
                return $deterministicRows;
            }

            if (!$this->tableExists('page_crawl_snapshots')) return [];

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
                 LIMIT 25"
            );

            // Add triage classification using target_query_impressions already in crawl data (no extra queries)
            foreach ($rows as &$row) {
                $impressions = (int)($row['target_query_impressions'] ?? 0);
                $row['page_impressions'] = $impressions;
                $row['page_clicks'] = (int)($row['target_query_clicks'] ?? 0);
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
            if ($this->tableExists('page_facts')) {
                $rows = $this->db->fetchAllAssociative(
                    "SELECT url, page_type, word_count, CASE WHEN is_indexable = TRUE THEN FALSE ELSE TRUE END AS is_noindex
                     FROM page_facts
                     WHERE is_indexable = TRUE
                     ORDER BY page_type, url"
                );
                if (!empty($rows)) {
                    return $rows;
                }
            }

            if (!$this->tableExists('page_crawl_snapshots')) return [];

            return $this->db->fetchAllAssociative(
                "SELECT url, page_type, word_count, is_noindex
                 FROM page_crawl_snapshots
                 WHERE crawled_at >= (SELECT MAX(crawled_at) - INTERVAL '1 hour' FROM page_crawl_snapshots)
                   AND is_noindex = FALSE
                 ORDER BY page_type, url"
            );
        } catch (\Exception $e) { return []; }
    }

    private function loadDeterministicCrawlData(): array
    {
        try {
            if (!$this->tableExists('page_facts') || !$this->tableExists('rule_violations')) {
                return [];
            }

            $snapshotVersion = (int) $this->db->fetchOne('SELECT COALESCE(MAX(snapshot_version), 0) FROM rule_violations');
            if ($snapshotVersion <= 0) {
                return [];
            }

            $rows = $this->db->fetchAllAssociative(
                "SELECT pf.url, pf.page_type, pf.has_central_entity, pf.has_core_link,
                        pf.word_count, pf.h1, pf.title_tag, pf.h1_matches_title,
                        CASE WHEN pf.h2_count > 0 THEN '[\"present\"]' ELSE '[]' END AS h2s,
                        COALESCE(CAST(pf.schema_types AS TEXT), '[]') AS schema_types,
                        CASE WHEN pf.is_indexable = TRUE THEN FALSE ELSE TRUE END AS is_noindex,
                        pf.internal_link_count,
                        0 AS image_count,
                        FALSE AS has_faq_section,
                        FALSE AS has_product_image,
                        COALESCE(CAST(pf.schema_errors AS TEXT), '[]') AS schema_errors,
                        pf.last_crawled_at AS crawled_at,
                        pf.target_query, pf.target_query_impressions, pf.target_query_position, pf.target_query_clicks,
                        MAX(rv.triage) AS triage,
                        STRING_AGG(rv.rule_id, ',' ORDER BY rv.rule_id) AS rule_ids
                 FROM page_facts pf
                 INNER JOIN rule_violations rv
                    ON rv.url = pf.url
                   AND rv.snapshot_version = :snapshot_version
                   AND rv.status IN ('fail', 'suppressed')
                 GROUP BY pf.url, pf.page_type, pf.has_central_entity, pf.has_core_link,
                        pf.word_count, pf.h1, pf.title_tag, pf.h1_matches_title, pf.h2_count,
                        pf.schema_types, pf.is_indexable, pf.internal_link_count, pf.schema_errors,
                        pf.last_crawled_at, pf.target_query, pf.target_query_impressions,
                        pf.target_query_position, pf.target_query_clicks
                 ORDER BY COALESCE(pf.target_query_impressions, 0) DESC, pf.url
                 LIMIT 25",
                ['snapshot_version' => $snapshotVersion]
            );

            foreach ($rows as &$row) {
                $impressions = (int) ($row['target_query_impressions'] ?? 0);
                $row['page_impressions'] = $impressions;
                $row['page_clicks'] = (int) ($row['target_query_clicks'] ?? 0);
                if (empty($row['triage'])) {
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
            }
            unset($row);

            return $rows;
        } catch (\Exception $e) {
            return [];
        }
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

    private function findOwnedConversation(int $conversationId, ?int $userId): ?array
    {
        return $this->conversationService->findOwnedConversation($conversationId, $userId);
    }

    private function getCurrentUserId(): ?int
    {
        $user = $this->getUser();
        return $user ? $user->getId() : null;
    }

    private function getCurrentActorName(): string
    {
        $session = $this->requestStack->getSession();
        $activePersona = $session->get('active_persona', null);
        if (is_array($activePersona) && !empty($activePersona['name'])) {
            return (string) $activePersona['name'];
        }

        $user = $this->getUser();
        if ($user && method_exists($user, 'getName') && $user->getName()) {
            return (string) $user->getName();
        }

        return 'Unknown';
    }

    private function llmActionsEnabled(): bool
    {
        $flag = strtolower((string) ($_ENV['LOGIRI_ENABLE_LLM_ACTIONS'] ?? ''));
        return in_array($flag, ['1', 'true', 'yes', 'on'], true);
    }

    private function tableExists(string $tableName): bool
    {
        $tables = $this->db->fetchFirstColumn(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
            [$tableName]
        );

        return !empty($tables);
    }
}


