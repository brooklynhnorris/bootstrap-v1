#!/bin/bash
# Don't use set -e — individual step failures are handled with || warn

echo "============================================"
echo "  LOGIRI DAILY PIPELINE"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

# Step 1: Fetch fresh data from all sources
echo ""
echo "[1/9] Fetching Google Search Console data..."
php bin/console app:fetch-gsc || echo "  [WARN] GSC fetch failed — token may need refresh"

echo ""
echo "[2/9] Fetching Google Analytics 4 data..."
php bin/console app:fetch-ga4 || echo "  [WARN] GA4 fetch failed — token may need refresh"

echo ""
echo "[3/9] Fetching DataForSEO data (keywords, volumes, competitors, SERP, backlinks)..."
php bin/console app:fetch-dataforseo || echo "  [WARN] DataForSEO fetch failed — check API credentials"

echo ""
echo "[3a/9] Fetching GA4 Engagement data..."
php bin/console app:fetch-ga4-engagement --days=28 --compare || echo "  [WARN] GA4 Engagement fetch failed"

echo ""
echo "[3b/9] Fetching Core Web Vitals..."
php bin/console app:fetch-cwv --limit=50 || echo "  [WARN] CWV fetch failed — check PSI_API_KEY"

# Step 2: Crawl all pages with fresh content
echo ""
echo "[4/9] Crawling WordPress pages..."
php bin/console app:crawl-pages --limit=1000
echo "  Crawl complete"

# Step 3: Evaluate all rules against fresh data
echo ""
echo "[5/9] Running rule evaluation (skip-validation, single-round play briefs)..."
php bin/console app:evaluate-rule --skip-validation
echo "  Evaluation complete — tasks added to Playbook Board"

# Step 4: Verify outcomes from previous fixes
echo ""
echo "[6/9] Verifying outcomes from completed tasks..."
php bin/console app:verify-outcomes || echo "  [WARN] Outcome verification failed or no outcomes to verify"

# Step 5: Propose rule changes based on outcome feedback
echo ""
echo "[7/9] Reviewing rule performance — proposing changes for underperforming rules..."
php bin/console app:propose-rule-changes || echo "  [WARN] Rule change proposal skipped or no feedback yet"

# Step 6: Generate performance report
echo ""
echo "[8/9] Generating performance report..."
php bin/console app:generate-report || echo "  [WARN] Report generation skipped (fewer than 10 pages updated)"

# Step 7: Final SERP spot-check (cheap — $0.01 for 5 queries)
echo ""
echo "[9/9] Final live SERP spot-check on top queries..."
php bin/console app:fetch-dataforseo --skip-keywords --skip-volumes --skip-competitors --skip-backlinks --serp-limit=5 || echo "  [WARN] Final SERP check skipped"

echo ""
echo "============================================"
echo "  PIPELINE COMPLETE"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"