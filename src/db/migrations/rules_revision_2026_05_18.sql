-- Logiri rule revisions (2026-05-18)
-- Source: RULE_REVISION_SPEC_2026-05-18.md

-- =========================================================
-- ETA-05
-- Acceptance range: 20-80
-- =========================================================
BEGIN;

-- 1) Precheck: count + sample 10 URLs that WOULD fire with revised SQL
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
),
would_fire AS (
    SELECT p.url, p.word_count, COALESCE(g.total_impr, 0) AS impressions, COALESCE(g.weighted_pos, 99) AS weighted_pos
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.word_count > 0
      AND p.word_count < 1000
      AND p.is_noindex = FALSE
      AND p.url NOT LIKE '/horse-trailers-for-sale/%'
      AND COALESCE(g.total_impr, 0) >= 100
      AND COALESCE(g.weighted_pos, 99) <= 30
)
SELECT 'ETA-05 PRECHECK COUNT' AS section, COUNT(*) AS firing_count FROM would_fire;

WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
),
would_fire AS (
    SELECT p.url, p.word_count, COALESCE(g.total_impr, 0) AS impressions, COALESCE(g.weighted_pos, 99) AS weighted_pos
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.word_count > 0
      AND p.word_count < 1000
      AND p.is_noindex = FALSE
      AND p.url NOT LIKE '/horse-trailers-for-sale/%'
      AND COALESCE(g.total_impr, 0) >= 100
      AND COALESCE(g.weighted_pos, 99) <= 30
)
SELECT 'ETA-05 PRECHECK SAMPLE' AS section, url, word_count, impressions, weighted_pos
FROM would_fire
ORDER BY impressions DESC, weighted_pos ASC, url
LIMIT 10;

-- 2) Guarded update in DO block
DO $$
DECLARE
    old_trigger_sql text;
    old_count bigint := 0;
    new_count bigint := 0;
BEGIN
    SELECT trigger_sql INTO old_trigger_sql FROM seo_rules WHERE rule_id = 'ETA-05';

    IF old_trigger_sql IS NOT NULL AND btrim(old_trigger_sql) <> '' THEN
        EXECUTE format('SELECT COUNT(*) FROM (%s) old_q', old_trigger_sql) INTO old_count;
    END IF;

    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
        FROM page_crawl_snapshots
    ),
    gsc_summary AS (
        SELECT page,
               SUM(impressions) AS total_impr,
               ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
        FROM gsc_snapshots
        WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
        GROUP BY page
    )
    SELECT COUNT(*)
    INTO new_count
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.word_count > 0
      AND p.word_count < 1000
      AND p.is_noindex = FALSE
      AND p.url NOT LIKE '/horse-trailers-for-sale/%'
      AND COALESCE(g.total_impr, 0) >= 100
      AND COALESCE(g.weighted_pos, 99) <= 30;

    IF new_count BETWEEN 20 AND 80 THEN
        UPDATE seo_rules
        SET trigger_sql = $sql$
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
)
SELECT p.url, p.page_type, p.word_count, p.has_central_entity, p.central_entity_count
FROM latest p
LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
WHERE p.rn = 1
  AND p.page_type = 'outer'
  AND p.word_count > 0
  AND p.word_count < 1000
  AND p.is_noindex = FALSE
  AND p.url NOT LIKE '/horse-trailers-for-sale/%'
  AND COALESCE(g.total_impr, 0) >= 100
  AND COALESCE(g.weighted_pos, 99) <= 30
$sql$,
            updated_at = NOW(),
            updated_by = 'Brook'
        WHERE rule_id = 'ETA-05';

        RAISE NOTICE 'ETA-05 updated. old_count=%, new_count=%', old_count, new_count;
    ELSE
        RAISE NOTICE 'ETA-05 skipped. old_count=%, proposed_new_count=% (outside 20-80)', old_count, new_count;
    END IF;
END $$;

-- 3) Verification: new firing count + sample 10
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
),
now_firing AS (
    SELECT p.url, p.word_count, COALESCE(g.total_impr, 0) AS impressions, COALESCE(g.weighted_pos, 99) AS weighted_pos
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.word_count > 0
      AND p.word_count < 1000
      AND p.is_noindex = FALSE
      AND p.url NOT LIKE '/horse-trailers-for-sale/%'
      AND COALESCE(g.total_impr, 0) >= 100
      AND COALESCE(g.weighted_pos, 99) <= 30
)
SELECT 'ETA-05 VERIFY COUNT' AS section, COUNT(*) AS firing_count FROM now_firing;

WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
),
now_firing AS (
    SELECT p.url, p.word_count, COALESCE(g.total_impr, 0) AS impressions, COALESCE(g.weighted_pos, 99) AS weighted_pos
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.word_count > 0
      AND p.word_count < 1000
      AND p.is_noindex = FALSE
      AND p.url NOT LIKE '/horse-trailers-for-sale/%'
      AND COALESCE(g.total_impr, 0) >= 100
      AND COALESCE(g.weighted_pos, 99) <= 30
)
SELECT 'ETA-05 VERIFY SAMPLE' AS section, url, word_count, impressions, weighted_pos
FROM now_firing
ORDER BY impressions DESC, weighted_pos ASC, url
LIMIT 10;

COMMIT;

-- =========================================================
-- DDT-EEAT-07
-- Acceptance range: 30-80
-- =========================================================
BEGIN;

-- 1) Precheck: count + sample 10 URLs that WOULD fire with revised SQL
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
would_fire AS (
    SELECT p.url, p.page_type, p.word_count
    FROM latest p
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.is_noindex = FALSE
      AND p.word_count > 1000
      AND p.body_text_snippet NOT LIKE '%Z-Frame%'
      AND p.body_text_snippet NOT LIKE '%SafeTack%'
      AND p.body_text_snippet NOT LIKE '%SafeBump%'
      AND p.body_text_snippet NOT LIKE '%SafeKick%'
      AND p.url NOT LIKE '/blog/%'
      AND p.url NOT LIKE '%-tips/'
      AND p.url NOT LIKE '%-guide/'
      AND p.url NOT LIKE '%-mistakes%'
      AND p.url NOT LIKE '%-explained/'
      AND p.url NOT LIKE '%-101%'
      AND p.url NOT LIKE '/horse-trailer-safety%'
      AND p.url NOT LIKE '/horse-trailer-accident%'
      AND p.url NOT LIKE '/how-to-%'
      AND p.url NOT LIKE '/why-%'
      AND p.url NOT LIKE '/what-%'
      AND p.url NOT LIKE '/can-%'
      AND p.url NOT LIKE '%-vs-%'
)
SELECT 'DDT-EEAT-07 PRECHECK COUNT' AS section, COUNT(*) AS firing_count FROM would_fire;

WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
would_fire AS (
    SELECT p.url, p.page_type, p.word_count
    FROM latest p
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.is_noindex = FALSE
      AND p.word_count > 1000
      AND p.body_text_snippet NOT LIKE '%Z-Frame%'
      AND p.body_text_snippet NOT LIKE '%SafeTack%'
      AND p.body_text_snippet NOT LIKE '%SafeBump%'
      AND p.body_text_snippet NOT LIKE '%SafeKick%'
      AND p.url NOT LIKE '/blog/%'
      AND p.url NOT LIKE '%-tips/'
      AND p.url NOT LIKE '%-guide/'
      AND p.url NOT LIKE '%-mistakes%'
      AND p.url NOT LIKE '%-explained/'
      AND p.url NOT LIKE '%-101%'
      AND p.url NOT LIKE '/horse-trailer-safety%'
      AND p.url NOT LIKE '/horse-trailer-accident%'
      AND p.url NOT LIKE '/how-to-%'
      AND p.url NOT LIKE '/why-%'
      AND p.url NOT LIKE '/what-%'
      AND p.url NOT LIKE '/can-%'
      AND p.url NOT LIKE '%-vs-%'
)
SELECT 'DDT-EEAT-07 PRECHECK SAMPLE' AS section, url, page_type, word_count
FROM would_fire
ORDER BY word_count DESC, url
LIMIT 10;

-- 2) Guarded update in DO block
DO $$
DECLARE
    old_trigger_sql text;
    old_count bigint := 0;
    new_count bigint := 0;
BEGIN
    SELECT trigger_sql INTO old_trigger_sql FROM seo_rules WHERE rule_id = 'DDT-EEAT-07';

    IF old_trigger_sql IS NOT NULL AND btrim(old_trigger_sql) <> '' THEN
        EXECUTE format('SELECT COUNT(*) FROM (%s) old_q', old_trigger_sql) INTO old_count;
    END IF;

    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
        FROM page_crawl_snapshots
    )
    SELECT COUNT(*)
    INTO new_count
    FROM latest p
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.is_noindex = FALSE
      AND p.word_count > 1000
      AND p.body_text_snippet NOT LIKE '%Z-Frame%'
      AND p.body_text_snippet NOT LIKE '%SafeTack%'
      AND p.body_text_snippet NOT LIKE '%SafeBump%'
      AND p.body_text_snippet NOT LIKE '%SafeKick%'
      AND p.url NOT LIKE '/blog/%'
      AND p.url NOT LIKE '%-tips/'
      AND p.url NOT LIKE '%-guide/'
      AND p.url NOT LIKE '%-mistakes%'
      AND p.url NOT LIKE '%-explained/'
      AND p.url NOT LIKE '%-101%'
      AND p.url NOT LIKE '/horse-trailer-safety%'
      AND p.url NOT LIKE '/horse-trailer-accident%'
      AND p.url NOT LIKE '/how-to-%'
      AND p.url NOT LIKE '/why-%'
      AND p.url NOT LIKE '/what-%'
      AND p.url NOT LIKE '/can-%'
      AND p.url NOT LIKE '%-vs-%';

    IF new_count BETWEEN 30 AND 80 THEN
        UPDATE seo_rules
        SET trigger_sql = $sql$
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
)
SELECT p.url, p.page_type, p.word_count,
       (p.body_text_snippet LIKE '%Z-Frame%') AS mentions_z_frame,
       (p.body_text_snippet LIKE '%SafeTack%'
         OR p.body_text_snippet LIKE '%SafeBump%'
         OR p.body_text_snippet LIKE '%SafeKick%') AS mentions_proprietary
FROM latest p
WHERE p.rn = 1
  AND p.page_type = 'outer'
  AND p.is_noindex = FALSE
  AND p.word_count > 1000
  AND p.body_text_snippet NOT LIKE '%Z-Frame%'
  AND p.body_text_snippet NOT LIKE '%SafeTack%'
  AND p.body_text_snippet NOT LIKE '%SafeBump%'
  AND p.body_text_snippet NOT LIKE '%SafeKick%'
  AND p.url NOT LIKE '/blog/%'
  AND p.url NOT LIKE '%-tips/'
  AND p.url NOT LIKE '%-guide/'
  AND p.url NOT LIKE '%-mistakes%'
  AND p.url NOT LIKE '%-explained/'
  AND p.url NOT LIKE '%-101%'
  AND p.url NOT LIKE '/horse-trailer-safety%'
  AND p.url NOT LIKE '/horse-trailer-accident%'
  AND p.url NOT LIKE '/how-to-%'
  AND p.url NOT LIKE '/why-%'
  AND p.url NOT LIKE '/what-%'
  AND p.url NOT LIKE '/can-%'
  AND p.url NOT LIKE '%-vs-%'
$sql$,
            updated_at = NOW(),
            updated_by = 'Brook'
        WHERE rule_id = 'DDT-EEAT-07';

        RAISE NOTICE 'DDT-EEAT-07 updated. old_count=%, new_count=%', old_count, new_count;
    ELSE
        RAISE NOTICE 'DDT-EEAT-07 skipped. old_count=%, proposed_new_count=% (outside 30-80)', old_count, new_count;
    END IF;
END $$;

-- 3) Verification: new firing count + sample 10
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
now_firing AS (
    SELECT p.url, p.page_type, p.word_count
    FROM latest p
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.is_noindex = FALSE
      AND p.word_count > 1000
      AND p.body_text_snippet NOT LIKE '%Z-Frame%'
      AND p.body_text_snippet NOT LIKE '%SafeTack%'
      AND p.body_text_snippet NOT LIKE '%SafeBump%'
      AND p.body_text_snippet NOT LIKE '%SafeKick%'
      AND p.url NOT LIKE '/blog/%'
      AND p.url NOT LIKE '%-tips/'
      AND p.url NOT LIKE '%-guide/'
      AND p.url NOT LIKE '%-mistakes%'
      AND p.url NOT LIKE '%-explained/'
      AND p.url NOT LIKE '%-101%'
      AND p.url NOT LIKE '/horse-trailer-safety%'
      AND p.url NOT LIKE '/horse-trailer-accident%'
      AND p.url NOT LIKE '/how-to-%'
      AND p.url NOT LIKE '/why-%'
      AND p.url NOT LIKE '/what-%'
      AND p.url NOT LIKE '/can-%'
      AND p.url NOT LIKE '%-vs-%'
)
SELECT 'DDT-EEAT-07 VERIFY COUNT' AS section, COUNT(*) AS firing_count FROM now_firing;

WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
now_firing AS (
    SELECT p.url, p.page_type, p.word_count
    FROM latest p
    WHERE p.rn = 1
      AND p.page_type = 'outer'
      AND p.is_noindex = FALSE
      AND p.word_count > 1000
      AND p.body_text_snippet NOT LIKE '%Z-Frame%'
      AND p.body_text_snippet NOT LIKE '%SafeTack%'
      AND p.body_text_snippet NOT LIKE '%SafeBump%'
      AND p.body_text_snippet NOT LIKE '%SafeKick%'
      AND p.url NOT LIKE '/blog/%'
      AND p.url NOT LIKE '%-tips/'
      AND p.url NOT LIKE '%-guide/'
      AND p.url NOT LIKE '%-mistakes%'
      AND p.url NOT LIKE '%-explained/'
      AND p.url NOT LIKE '%-101%'
      AND p.url NOT LIKE '/horse-trailer-safety%'
      AND p.url NOT LIKE '/horse-trailer-accident%'
      AND p.url NOT LIKE '/how-to-%'
      AND p.url NOT LIKE '/why-%'
      AND p.url NOT LIKE '/what-%'
      AND p.url NOT LIKE '/can-%'
      AND p.url NOT LIKE '%-vs-%'
)
SELECT 'DDT-EEAT-07 VERIFY SAMPLE' AS section, url, page_type, word_count
FROM now_firing
ORDER BY word_count DESC, url
LIMIT 10;

COMMIT;

-- =========================================================
-- FC-R7
-- Acceptance range: 20-50
-- =========================================================
BEGIN;

-- 1) Precheck: count + sample 10 URLs that WOULD fire with revised SQL
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
),
would_fire AS (
    SELECT p.url, p.page_type, p.h1, p.title_tag, p.h1_matches_title,
           COALESCE(g.total_impr, 0) AS impressions, COALESCE(g.weighted_pos, 99) AS weighted_pos
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.is_noindex = FALSE
      AND (p.h1_matches_title IS NOT TRUE OR p.h1 IS NULL OR p.h1 = '')
      AND p.page_type != 'core'
      AND p.url NOT IN ('/','/horse-trailers/','/gooseneck-horse-trailers/','/bumper-pull-horse-trailers/','/living-quarters-horse-trailers/')
      AND COALESCE(g.weighted_pos, 99) >= 5
      AND COALESCE(g.total_impr, 0) >= 50
)
SELECT 'FC-R7 PRECHECK COUNT' AS section, COUNT(*) AS firing_count FROM would_fire;

WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
),
would_fire AS (
    SELECT p.url, p.page_type, p.h1, p.title_tag, p.h1_matches_title,
           COALESCE(g.total_impr, 0) AS impressions, COALESCE(g.weighted_pos, 99) AS weighted_pos
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.is_noindex = FALSE
      AND (p.h1_matches_title IS NOT TRUE OR p.h1 IS NULL OR p.h1 = '')
      AND p.page_type != 'core'
      AND p.url NOT IN ('/','/horse-trailers/','/gooseneck-horse-trailers/','/bumper-pull-horse-trailers/','/living-quarters-horse-trailers/')
      AND COALESCE(g.weighted_pos, 99) >= 5
      AND COALESCE(g.total_impr, 0) >= 50
)
SELECT 'FC-R7 PRECHECK SAMPLE' AS section, url, page_type, impressions, weighted_pos
FROM would_fire
ORDER BY impressions DESC, weighted_pos ASC, url
LIMIT 10;

-- 2) Guarded update in DO block
DO $$
DECLARE
    old_trigger_sql text;
    old_count bigint := 0;
    new_count bigint := 0;
BEGIN
    SELECT trigger_sql INTO old_trigger_sql FROM seo_rules WHERE rule_id = 'FC-R7';

    IF old_trigger_sql IS NOT NULL AND btrim(old_trigger_sql) <> '' THEN
        EXECUTE format('SELECT COUNT(*) FROM (%s) old_q', old_trigger_sql) INTO old_count;
    END IF;

    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
        FROM page_crawl_snapshots
    ),
    gsc_summary AS (
        SELECT page,
               SUM(impressions) AS total_impr,
               ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
        FROM gsc_snapshots
        WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
        GROUP BY page
    )
    SELECT COUNT(*)
    INTO new_count
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.is_noindex = FALSE
      AND (p.h1_matches_title IS NOT TRUE OR p.h1 IS NULL OR p.h1 = '')
      AND p.page_type != 'core'
      AND p.url NOT IN ('/','/horse-trailers/','/gooseneck-horse-trailers/','/bumper-pull-horse-trailers/','/living-quarters-horse-trailers/')
      AND COALESCE(g.weighted_pos, 99) >= 5
      AND COALESCE(g.total_impr, 0) >= 50;

    IF new_count BETWEEN 20 AND 50 THEN
        UPDATE seo_rules
        SET trigger_sql = $sql$
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
)
SELECT p.url, p.page_type, p.h1, p.title_tag, p.h1_matches_title,
       COALESCE(g.total_impr, 0) AS impressions,
       COALESCE(g.weighted_pos, 99) AS weighted_pos
FROM latest p
LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
WHERE p.rn = 1
  AND p.is_noindex = FALSE
  AND (p.h1_matches_title IS NOT TRUE OR p.h1 IS NULL OR p.h1 = '')
  AND p.page_type != 'core'
  AND p.url NOT IN ('/','/horse-trailers/','/gooseneck-horse-trailers/','/bumper-pull-horse-trailers/','/living-quarters-horse-trailers/')
  AND COALESCE(g.weighted_pos, 99) >= 5
  AND COALESCE(g.total_impr, 0) >= 50
$sql$,
            updated_at = NOW(),
            updated_by = 'Brook',
            trigger_source = 'page_crawl_snapshots'
        WHERE rule_id = 'FC-R7';

        RAISE NOTICE 'FC-R7 updated. old_count=%, new_count=%', old_count, new_count;
    ELSE
        RAISE NOTICE 'FC-R7 skipped. old_count=%, proposed_new_count=% (outside 20-50)', old_count, new_count;
    END IF;
END $$;

-- 3) Verification: new firing count + sample 10
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
),
now_firing AS (
    SELECT p.url, p.page_type, COALESCE(g.total_impr, 0) AS impressions, COALESCE(g.weighted_pos, 99) AS weighted_pos
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.is_noindex = FALSE
      AND (p.h1_matches_title IS NOT TRUE OR p.h1 IS NULL OR p.h1 = '')
      AND p.page_type != 'core'
      AND p.url NOT IN ('/','/horse-trailers/','/gooseneck-horse-trailers/','/bumper-pull-horse-trailers/','/living-quarters-horse-trailers/')
      AND COALESCE(g.weighted_pos, 99) >= 5
      AND COALESCE(g.total_impr, 0) >= 50
)
SELECT 'FC-R7 VERIFY COUNT' AS section, COUNT(*) AS firing_count FROM now_firing;

WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC, id DESC) AS rn
    FROM page_crawl_snapshots
),
gsc_summary AS (
    SELECT page,
           SUM(impressions) AS total_impr,
           ROUND(CAST(SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS NUMERIC), 1) AS weighted_pos
    FROM gsc_snapshots
    WHERE date_range = (SELECT MAX(date_range) FROM gsc_snapshots)
    GROUP BY page
),
now_firing AS (
    SELECT p.url, p.page_type, COALESCE(g.total_impr, 0) AS impressions, COALESCE(g.weighted_pos, 99) AS weighted_pos
    FROM latest p
    LEFT JOIN gsc_summary g ON g.page = 'https://www.doubledtrailers.com' || p.url
    WHERE p.rn = 1
      AND p.is_noindex = FALSE
      AND (p.h1_matches_title IS NOT TRUE OR p.h1 IS NULL OR p.h1 = '')
      AND p.page_type != 'core'
      AND p.url NOT IN ('/','/horse-trailers/','/gooseneck-horse-trailers/','/bumper-pull-horse-trailers/','/living-quarters-horse-trailers/')
      AND COALESCE(g.weighted_pos, 99) >= 5
      AND COALESCE(g.total_impr, 0) >= 50
)
SELECT 'FC-R7 VERIFY SAMPLE' AS section, url, page_type, impressions, weighted_pos
FROM now_firing
ORDER BY impressions DESC, weighted_pos ASC, url
LIMIT 10;

COMMIT;
