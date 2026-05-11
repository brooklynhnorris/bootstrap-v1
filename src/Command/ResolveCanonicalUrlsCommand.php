<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:resolve-canonical-urls', description: 'Resolve and store canonical URLs for page_facts via HEAD redirect tracing')]
class ResolveCanonicalUrlsCommand extends Command
{
    private string $siteBaseUrl = '';

    public function __construct(private Connection $db)
    {
        parent::__construct();
        $this->siteBaseUrl = rtrim((string) ($_ENV['GSC_SITE_URL'] ?? ''), '/');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $rows = $this->db->fetchAllAssociative(
            "SELECT url FROM page_facts
             WHERE canonical_url IS NULL
               AND canonical_resolved_at IS NULL
             ORDER BY url ASC"
        );

        $total = count($rows);
        if ($total === 0) {
            $output->writeln('No unresolved canonical URLs found.');
            return Command::SUCCESS;
        }

        $processed = 0;
        $redirects = 0;
        $errors = 0;

        foreach ($rows as $row) {
            $url = (string) ($row['url'] ?? '');
            $canonical = null;
            $hadError = false;

            try {
                $finalUrl = $this->resolveFinalUrl($url);
                if ($finalUrl !== null) {
                    // Normalize trailing slashes — /foo/ and /foo are the same page
                    if (rtrim($finalUrl, '/') === rtrim($url, '/')) {
                        $finalUrl = $url;
                    }

                    $canonical = $finalUrl;
                    if ($this->normalizeUrl($finalUrl) !== $this->normalizeUrl($url)) {
                        $redirects++;
                    }
                } else {
                    $hadError = true;
                }
            } catch (\Throwable) {
                $hadError = true;
            }

            $update = [
                'canonical_resolved_at' => date('Y-m-d H:i:s'),
            ];
            if ($canonical !== null) {
                $update['canonical_url'] = $canonical;
            }

            $this->db->update('page_facts', $update, ['url' => $url]);

            if ($hadError) {
                $errors++;
            }

            $processed++;
            if ($processed % 25 === 0 || $processed === $total) {
                $output->writeln("Processed {$processed}/{$total} URLs ({$redirects} redirects found, {$errors} errors)");
            }

            usleep(250000);
        }

        $output->writeln("Done. Processed {$processed}/{$total} URLs ({$redirects} redirects found, {$errors} errors)");

        return Command::SUCCESS;
    }

    private function resolveFinalUrl(string $url): ?string
    {
        if ($url === '') {
            return null;
        }

        $requestUrl = $this->toAbsoluteUrl($url);
        if ($requestUrl === null) {
            return null;
        }

        $ch = curl_init($requestUrl);
        if ($ch === false) {
            return null;
        }

        curl_setopt_array($ch, [
            CURLOPT_NOBODY => true,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_MAXREDIRS => 5,
            CURLOPT_TIMEOUT => 10,
            CURLOPT_CONNECTTIMEOUT => 10,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_USERAGENT => 'LogiriCanonicalResolver/1.0',
        ]);

        curl_exec($ch);
        $errno = curl_errno($ch);
        $statusCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $effectiveUrl = (string) curl_getinfo($ch, CURLINFO_EFFECTIVE_URL);
        curl_close($ch);

        if ($errno !== 0 || $statusCode >= 400 || $effectiveUrl === '') {
            return null;
        }

        $path = parse_url($effectiveUrl, PHP_URL_PATH);
        if (!is_string($path) || $path === '') {
            return null;
        }

        return $this->normalizeUrl($path);
    }

    private function toAbsoluteUrl(string $url): ?string
    {
        $candidate = trim($url);
        if ($candidate === '') {
            return null;
        }

        if (str_starts_with($candidate, 'http://') || str_starts_with($candidate, 'https://')) {
            return $candidate;
        }

        if ($this->siteBaseUrl === '') {
            return null;
        }

        if (!str_starts_with($candidate, '/')) {
            $candidate = '/' . $candidate;
        }

        return $this->siteBaseUrl . $candidate;
    }

    private function normalizeUrl(string $url): string
    {
        $url = trim(strtolower($url));
        return $url === '/' ? '/' : rtrim($url, '/');
    }
}
