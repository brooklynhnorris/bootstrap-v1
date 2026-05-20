-- Adds http_status = 200 gating to stored trigger_sql rules that query page_crawl_snapshots.
-- Excludes TECH-R* by explicit rule_id allowlist provided by audit.

BEGIN;

WITH target_rules(rule_id) AS (
    VALUES
        ('AIS-001'),('AIS-002'),('AIS-003'),('AIS-004'),('AIS-005'),('AIS-007'),('AIS-008'),
        ('CF-001'),('CF-003'),('CF-004'),('CF-005'),('CF-006'),('CF-007'),('CF-008'),
        ('CFL-01'),('CFL-04'),
        ('CI-01'),('CI-02'),('CI-R1'),('CI-R3'),('CI-R4'),('CI-R6'),
        ('CTA-F1'),('CTA-F2'),('CTA-F3'),('CTA-F4'),('CTA-F5'),('CTA-F6'),('CTA-F7'),
        ('CWV-R1'),('CWV-R2'),('CWV-R4'),('CWV-R5'),('CWV-R6'),('CWV-R7'),
        ('DDT-EEAT-01'),('DDT-EEAT-02'),('DDT-EEAT-03'),('DDT-EEAT-04'),
        ('DDT-EEAT-05'),('DDT-EEAT-06'),('DDT-EEAT-07'),('DDT-EEAT-08'),
        ('DDT-LOCAL-01'),('DDT-LOCAL-02'),('DDT-LOCAL-03'),('DDT-LOCAL-04'),
        ('DDT-LOCAL-05'),('DDT-LOCAL-06'),('DDT-LOCAL-07'),
        ('DDT-SD-002'),('DDT-SD-003'),('DDT-SD-004'),('DDT-SD-005'),('DDT-SD-006'),
        ('ETA-001'),('ETA-002'),('ETA-003'),('ETA-004'),('ETA-005'),('ETA-006'),('ETA-05'),
        ('FC-R7'),
        ('ILA-001'),('ILA-002'),('ILA-003'),('ILA-005'),
        ('KIA-01'),('KIA-02'),('KIA-03'),('KIA-04'),('KIA-05'),('KIA-06'),
        ('KIA-R3'),('KIA-R6'),('KIA-R8'),
        ('MAO-01'),('MAO-02'),('MAO-03'),('MAO-04'),('MAO-05'),('MAO-06'),('MAO-07'),
        ('MAO-R2'),('MAO-R4'),('MAO-R6'),('MAO-R7'),
        ('OPQ-001'),('OPQ-002'),('OPQ-005'),('OPQ-006'),('OPQ-R1'),('OPQ-R2'),
        ('OPQ-R3'),('OPQ-R4'),('OPQ-R4b'),('OPQ-R5'),('OPQ-R6'),('OPQ-R7'),
        ('SCH-001'),('SCH-002'),('SCH-003'),('SCH-004'),('SCH-005'),('SCH-006'),
        ('SCH-007'),('SCH-008'),
        ('USE-001'),('USE-004'),('USE-006'),('USE-008'),('USE-R3'),('USE-R4'),('USE-R7')
),
eligible AS (
    SELECT
        r.rule_id,
        r.trigger_sql,
        lower(
            coalesce(
                substring(
                    r.trigger_sql
                    FROM '(?i)from\s+page_crawl_snapshots\s+(?:as\s+)?([a-z_][a-z0-9_]*)\b'
                ),
                ''
            )
        ) AS raw_alias
    FROM seo_rules r
    JOIN target_rules t ON t.rule_id = r.rule_id
    WHERE r.trigger_sql IS NOT NULL
      AND r.trigger_sql ILIKE '%page_crawl_snapshots%'
      AND r.trigger_sql !~* '\mhttp_status\M'
),
normalized AS (
    SELECT
        rule_id,
        trigger_sql,
        CASE
            WHEN raw_alias IN ('', 'where', 'join', 'left', 'right', 'inner', 'full', 'cross', 'on')
                THEN ''
            ELSE raw_alias || '.'
        END AS alias_prefix
    FROM eligible
),
rewritten AS (
    SELECT
        rule_id,
        CASE
            WHEN trigger_sql !~* '\mwhere\M' THEN trigger_sql
            WHEN trigger_sql ~* '\m(group\s+by|order\s+by|limit)\M' THEN
                regexp_replace(
                    trigger_sql,
                    '(?is)\s+(group\s+by|order\s+by|limit)\M',
                    ' AND ' || alias_prefix || 'http_status = 200 \1',
                    'i'
                )
            ELSE trigger_sql || ' AND ' || alias_prefix || 'http_status = 200'
        END AS new_trigger_sql
    FROM normalized
)
UPDATE seo_rules dst
SET trigger_sql = src.new_trigger_sql,
    updated_at = NOW(),
    updated_by = 'Codex'
FROM rewritten src
WHERE dst.rule_id = src.rule_id
  AND src.new_trigger_sql IS DISTINCT FROM dst.trigger_sql;

COMMIT;

-- Post-check audit:
-- SELECT rule_id
-- FROM seo_rules
-- WHERE is_active = TRUE
--   AND trigger_sql ILIKE '%page_crawl_snapshots%'
--   AND trigger_sql !~* '\mhttp_status\M'
--   AND rule_id NOT LIKE 'TECH-R%';
