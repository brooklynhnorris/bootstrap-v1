<?php

namespace App\Service;

class AssetUrlClassifier
{
    /** @var string[] */
    private const BLOCKED_EXTENSIONS = [
        'jpg', 'jpeg', 'png', 'webp', 'gif', 'svg',
        'pdf', 'doc', 'docx', 'xls', 'xlsx',
        'zip',
        'mp4', 'webm',
    ];

    public function isAssetUrl(string $url): ?string
    {
        $u = $this->normalizeUrl($url);
        $parsed = parse_url($u);
        $path = isset($parsed['path']) && is_string($parsed['path']) ? $parsed['path'] : $u;

        if (preg_match('#^/wp-content/uploads/#i', $path) === 1) {
            return 'wp-content-uploads-prefix';
        }
        if (preg_match('#^/scripts/.*\.html$#i', $path) === 1) {
            return 'scripts-html-suffix';
        }

        if (preg_match('/\.([a-z0-9]+)$/i', $path, $m) === 1) {
            $ext = strtolower((string) ($m[1] ?? ''));
            if (in_array($ext, self::BLOCKED_EXTENSIONS, true)) {
                return 'extension-' . $ext;
            }
        }

        return null;
    }

    private function normalizeUrl(string $url): string
    {
        $u = trim($url);
        $u = strtolower($u);
        $u = rtrim($u, '/');

        $q = strpos($u, '?');
        if ($q !== false) {
            $u = substr($u, 0, $q);
        }

        $f = strpos($u, '#');
        if ($f !== false) {
            $u = substr($u, 0, $f);
        }

        return $u;
    }
}
