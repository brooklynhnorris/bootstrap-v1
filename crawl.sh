#!/bin/bash
set -e
echo "Starting weekly data refresh..."
php bin/console app:fetch-gsc
echo "GSC done"
php bin/console app:fetch-ga4
echo "GA4 done"
php bin/console app:fetch-semrush
echo "SEMrush done"
php bin/console app:crawl-pages --limit=1000
echo "Crawl done"
php bin/console app:evaluate-rule
echo "Rule evaluation done"
echo "All data refreshed successfully"