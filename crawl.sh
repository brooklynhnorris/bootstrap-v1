#!/bin/bash
# Don't use set -e — individual step failures are handled with || warn

echo "============================================"
echo "  LOGIRI WEEKLY PIPELINE"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

# Step 1: Fetch fresh data from all sources
echo ""
echo "[1/6] Fetching Google Search Console data..."
php bin/console app:fetch-gsc || echo "  [WARN] GSC fetch failed — token may need refresh"

echo ""
echo "[2/6] Fetching Google Analytics 4 data..."
php bin/console app:fetch-ga4 || echo "  [WARN] GA4 fetch failed — token may need refresh"

echo ""
echo "[3/6] Fetching SEMrush data..."
php bin/console app:fetch-semrush || echo "  [WARN] SEMrush fetch failed — check API key"

# Step 2: Crawl all pages with fresh content
echo ""
echo "[4/6] Crawling WordPress pages..."
php bin/console app:crawl-pages --limit=1000
echo "  Crawl complete"

# Step 3: Evaluate all rules against fresh data (skip validation — rules are pre-validated)
echo ""
echo "[5/6] Running rule evaluation (skip-validation, single-round play briefs)..."
php bin/console app:evaluate-rule --skip-validation
echo "  Evaluation complete — tasks added to Playbook Board"

# Step 4: Verify outcomes from previous fixes
echo ""
echo "[6/8] Verifying outcomes from completed tasks..."
php bin/console app:verify-outcomes || echo "  [WARN] Outcome verification failed or no outcomes to verify"

# Step 4a: Fetch GA4 Engagement metrics
echo ""
echo "[4a/8] Fetching GA4 Engagement data..."
php bin/console app:fetch-ga4-engagement --days=28 --compare || echo "  [WARN] GA4 Engagement fetch failed"

# Step 4b: Fetch Core Web Vitals
echo ""
echo "[4b/8] Fetching Core Web Vitals..."
php bin/console app:fetch-cwv --limit=50 || echo "  [WARN] CWV fetch failed — check PSI_API_KEY"

# Step 5: Propose rule changes based on outcome feedback
echo ""
echo "[7/8] Reviewing rule performance — proposing changes for underperforming rules..."
php bin/console app:propose-rule-changes || echo "  [WARN] Rule change proposal skipped or no feedback yet"

# Step 6: Generate performance report if enough pages updated
echo ""
echo "[8/8] Generating performance report..."
php bin/console app:generate-report || echo "  [WARN] Report generation skipped (fewer than 10 pages updated)"

echo ""
echo "============================================"
echo "  PIPELINE COMPLETE"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"