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
        return $this->render('home/index.html.twig');
    }

    #[Route('/chat', name: 'chat', methods: ['POST'])]
    public function chat(Request $request): JsonResponse
    {
        $body     = json_decode($request->getContent(), true);
        $messages = $body['messages'] ?? [];

        $semrush = $this->db->fetchAssociative(
            'SELECT organic_keywords, organic_traffic, fetched_at FROM semrush_snapshots ORDER BY fetched_at DESC LIMIT 1'
        );

        $topQueries = $this->db->fetchAllAssociative(
            'SELECT query, page, clicks, impressions, position FROM gsc_snapshots ORDER BY impressions DESC LIMIT 20'
        );

        $topPages = $this->db->fetchAllAssociative(
            'SELECT page_path, sessions, pageviews, conversions FROM ga4_snapshots ORDER BY sessions DESC LIMIT 20'
        );

        $systemPrompt = $this->buildSystemPrompt($semrush ?: [], $topQueries, $topPages);

        $response = file_get_contents('https://api.anthropic.com/v1/messages', false, stream_context_create(array(
            'http' => array(
                'method'        => 'POST',
                'header'        => implode("\r\n", array(
                    'Content-Type: application/json',
                    'x-api-key: ' .  $_ENV['ANTHROPIC_API_KEY'] ,
                    'anthropic-version: 2023-06-01',
                )),
                'content'       => json_encode(array(
                    'model'      => $_ENV['CLAUDE_MODEL'] ?? 'claude-sonnet-4-6',
                    'max_tokens' => 1024,
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

    private function buildSystemPrompt(array $semrush, array $topQueries, array $topPages): string
    {
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

        $promptFile = dirname(__DIR__, 2) . '/system-prompt.txt';
        $staticRules = file_exists($promptFile) ? file_get_contents($promptFile) : '';

        $intro  = 'You are Logiri, an SEO intelligence assistant built specifically for Double D Trailers (doubledtrailers.com).';
        $intro .= ' You help the internal team identify and act on SEO issues using real data from SEMrush, Google Search Console, and Google Analytics 4.';
        $intro .= "\n\nToday is " . $date . '.';
        $intro .= "\n\nCURRENT DATA SNAPSHOT:";
        $intro .= "\nSEMrush Overview:";
        $intro .= "\n- Organic Keywords: " . $keywords;
        $intro .= "\n- Organic Traffic: " . $traffic;
        $intro .= "\n- Last updated: " . $updated;
        $intro .= "\n\nTop GSC Queries (last 28 days):\n" . $querySummary;
        $intro .= "\nTop GA4 Pages (last 28 days):\n" . $pageSummary;
        $intro .= "\n\n" . $staticRules;

        return $intro;
    }
}