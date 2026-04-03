#!/bin/bash
set -e

echo "Running startup tasks..."

# Strip BOM from index.php using Python (more reliable than sed hex escapes)
python3 -c "
import sys
f = '/var/www/html/public/index.php'
data = open(f,'rb').read()
if data[:3] == b'\xef\xbb\xbf':
    open(f,'wb').write(data[3:])
    print('BOM stripped from index.php')
else:
    print('No BOM found in index.php')
" 2>/dev/null || true
find /var/www/html/src -name "*.php" -exec sed -i 's/\r//' {} \;

# Wait for database to be ready
until php -r "
\$url = parse_url(getenv('DATABASE_URL'));
new PDO('pgsql:host='.\$url['host'].';port='.(\$url['port']??5432).';dbname='.ltrim(\$url['path'],'/'), \$url['user'], \$url['pass']);
" 2>/dev/null; do
  echo "Waiting for database..."
  sleep 2
done

echo "Database ready. Running migrations..."

# Create and migrate all data tables
php /var/www/html/bin/console app:ensure-schema 2>/dev/null || php -r "
\$url = parse_url(getenv('DATABASE_URL'));
\$pdo = new PDO('pgsql:host='.\$url['host'].';port='.(\$url['port']??5432).';dbname='.ltrim(\$url['path'],'/'), \$url['user'], \$url['pass']);

\$pdo->exec('CREATE TABLE IF NOT EXISTS semrush_snapshots (id SERIAL PRIMARY KEY, domain VARCHAR(255) NOT NULL, organic_keywords INT DEFAULT 0, organic_traffic INT DEFAULT 0, fetched_at TIMESTAMP NOT NULL)');

\$pdo->exec('CREATE TABLE IF NOT EXISTS gsc_snapshots (id SERIAL PRIMARY KEY, query VARCHAR(500) NOT NULL, page TEXT NOT NULL, clicks INT DEFAULT 0, impressions INT DEFAULT 0, ctr FLOAT DEFAULT 0, position FLOAT DEFAULT 0, date_range VARCHAR(50) DEFAULT NULL, fetched_at TIMESTAMP NOT NULL)');

\$pdo->exec('CREATE TABLE IF NOT EXISTS ga4_snapshots (id SERIAL PRIMARY KEY, page_path TEXT NOT NULL, sessions INT DEFAULT 0, pageviews INT DEFAULT 0, bounce_rate FLOAT DEFAULT 0, conversions INT DEFAULT 0, avg_engagement_time FLOAT DEFAULT 0, engaged_sessions INT DEFAULT 0, date_range VARCHAR(50) DEFAULT NULL, fetched_at TIMESTAMP NOT NULL)');

\$pdo->exec('CREATE TABLE IF NOT EXISTS team_members (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, role VARCHAR(50) NOT NULL, email VARCHAR(255), max_hours_per_week INT DEFAULT 40, is_active BOOLEAN DEFAULT true)');

\$pdo->exec('CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    rule_id VARCHAR(20),
    assigned_to VARCHAR(100),
    assigned_role VARCHAR(50),
    status VARCHAR(20) DEFAULT \'pending\',
    priority VARCHAR(20) DEFAULT \'medium\',
    estimated_hours FLOAT DEFAULT 1,
    logged_hours FLOAT DEFAULT 0,
    due_date DATE,
    recheck_type VARCHAR(50),
    recheck_date DATE,
    recheck_verified BOOLEAN DEFAULT false,
    recheck_result VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
)');

\$pdo->exec('CREATE TABLE IF NOT EXISTS conversations (id SERIAL PRIMARY KEY, user_id INT DEFAULT NULL, title VARCHAR(255) DEFAULT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, is_archived BOOLEAN DEFAULT FALSE)');
\$pdo->exec('CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, conversation_id INT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE, role VARCHAR(20) NOT NULL, content TEXT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)');
\$pdo->exec('CREATE TABLE IF NOT EXISTS rule_reviews (id SERIAL PRIMARY KEY, conversation_id INT DEFAULT NULL REFERENCES conversations(id) ON DELETE SET NULL, rule_id VARCHAR(20) NOT NULL, verdict VARCHAR(30) NOT NULL, feedback TEXT DEFAULT NULL, reviewed_by VARCHAR(100) DEFAULT NULL, reviewed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)');
\$pdo->exec('CREATE TABLE IF NOT EXISTS user_overrides (id SERIAL PRIMARY KEY, url TEXT NOT NULL, field VARCHAR(50) NOT NULL, original_value TEXT DEFAULT NULL, override_value TEXT NOT NULL, reason TEXT DEFAULT NULL, overridden_by VARCHAR(100) DEFAULT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(url, field))');

// Add missing columns to existing tables
\$gscCols = \$pdo->query(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'gsc_snapshots'\")->fetchAll(PDO::FETCH_COLUMN);
if (!in_array('date_range', \$gscCols)) { \$pdo->exec('ALTER TABLE gsc_snapshots ADD COLUMN date_range VARCHAR(50) DEFAULT NULL'); }

\$ga4Cols = \$pdo->query(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'ga4_snapshots'\")->fetchAll(PDO::FETCH_COLUMN);
if (!in_array('date_range', \$ga4Cols)) { \$pdo->exec('ALTER TABLE ga4_snapshots ADD COLUMN date_range VARCHAR(50) DEFAULT NULL'); }
if (!in_array('avg_engagement_time', \$ga4Cols)) { \$pdo->exec('ALTER TABLE ga4_snapshots ADD COLUMN avg_engagement_time FLOAT DEFAULT 0'); }
if (!in_array('engaged_sessions', \$ga4Cols)) { \$pdo->exec('ALTER TABLE ga4_snapshots ADD COLUMN engaged_sessions INT DEFAULT 0'); }

\$taskCols = \$pdo->query(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'tasks'\")->fetchAll(PDO::FETCH_COLUMN);
if (!in_array('recheck_type', \$taskCols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_type VARCHAR(50)'); }
if (!in_array('recheck_date', \$taskCols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_date DATE'); }
if (!in_array('recheck_verified', \$taskCols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_verified BOOLEAN DEFAULT false'); }
if (!in_array('recheck_result', \$taskCols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_result VARCHAR(20)'); }
if (!in_array('logged_hours', \$taskCols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN logged_hours FLOAT DEFAULT 0'); }
if (!in_array('recheck_days', \$taskCols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_days INT DEFAULT NULL'); }
if (!in_array('recheck_criteria', \$taskCols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_criteria TEXT DEFAULT NULL'); }

echo 'Tables ready.' . PHP_EOL;

// ── Create rule_feedback table ──
\$pdo->exec('CREATE TABLE IF NOT EXISTS rule_feedback (id SERIAL PRIMARY KEY, rule_id VARCHAR(50), task_id INT, url TEXT, feedback_type VARCHAR(50), what_worked TEXT, what_didnt TEXT, proposed_change TEXT, change_type VARCHAR(50) DEFAULT \\'none\\', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)');

// ── Create chat_learnings table ──
\$pdo->exec('CREATE TABLE IF NOT EXISTS chat_learnings (id SERIAL PRIMARY KEY, learning TEXT NOT NULL, category VARCHAR(50) DEFAULT \\'general\\', confidence INT DEFAULT 7, learned_from VARCHAR(50) DEFAULT \\'auto\\', is_active BOOLEAN DEFAULT TRUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)');

// ── Create seo_rules table ──
\$pdo->exec('CREATE TABLE IF NOT EXISTS seo_rules (id SERIAL PRIMARY KEY, rule_id VARCHAR(50) NOT NULL UNIQUE, rule_name TEXT, category VARCHAR(100), tier VARCHAR(10), trigger_source VARCHAR(100), trigger_condition TEXT, trigger_sql TEXT, threshold TEXT, diagnosis TEXT, action_output TEXT, priority VARCHAR(50) DEFAULT \\'medium\\', assigned VARCHAR(255), ai_search_relevance TEXT, is_active BOOLEAN DEFAULT TRUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_by VARCHAR(100))');

// ── Ensure page_crawl_snapshots has all required columns ──
\$tables = \$pdo->query(\"SELECT tablename FROM pg_tables WHERE schemaname = 'public'\")->fetchAll(PDO::FETCH_COLUMN);
if (in_array('page_crawl_snapshots', \$tables)) {
    \$pcsCols = \$pdo->query(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'page_crawl_snapshots'\")->fetchAll(PDO::FETCH_COLUMN);
    \$needed = [
        'target_query' => 'TEXT DEFAULT NULL',
        'target_query_impressions' => 'INT DEFAULT 0',
        'target_query_position' => 'FLOAT DEFAULT NULL',
        'target_query_clicks' => 'INT DEFAULT 0',
        'top_5_queries' => 'JSONB DEFAULT NULL',
        'parent_url' => 'TEXT DEFAULT NULL',
        'url_depth' => 'INT DEFAULT 0',
        'sibling_count' => 'INT DEFAULT 0',
        'images_without_alt' => 'INT DEFAULT 0',
        'images_with_generic_alt' => 'INT DEFAULT 0',
        'has_main_content_video' => 'BOOLEAN DEFAULT FALSE',
        'video_metadata_valid' => 'BOOLEAN DEFAULT FALSE',
        'video_topic_aligned' => 'BOOLEAN DEFAULT FALSE',
        'video_thumbnail_url' => 'TEXT DEFAULT NULL',
        'video_title' => 'TEXT DEFAULT NULL',
        'video_duration_seconds' => 'INT DEFAULT NULL',
        'video_upload_date' => 'VARCHAR(20) DEFAULT NULL',
        'has_zframe_mention' => 'BOOLEAN DEFAULT FALSE',
        'has_safetack_mention' => 'BOOLEAN DEFAULT FALSE',
        'has_safebump_mention' => 'BOOLEAN DEFAULT FALSE',
        'has_safekick_mention' => 'BOOLEAN DEFAULT FALSE',
        'has_zframe_definition' => 'BOOLEAN DEFAULT FALSE',
        'internal_link_count' => 'INT DEFAULT 0',
        'is_utility' => 'BOOLEAN DEFAULT FALSE',
        'has_core_link' => 'BOOLEAN DEFAULT FALSE',
        'has_faq_section' => 'BOOLEAN DEFAULT FALSE',
        'has_product_image' => 'BOOLEAN DEFAULT FALSE',
        'mobile_viewport_set' => 'BOOLEAN DEFAULT FALSE',
    ];
    foreach (\$needed as \$col => \$type) {
        if (!in_array(\$col, \$pcsCols)) {
            \$pdo->exec(\"ALTER TABLE page_crawl_snapshots ADD COLUMN {\$col} {\$type}\");
        }
    }
    echo 'page_crawl_snapshots columns ensured.' . PHP_EOL;
}

// ── Ensure tasks table has all required columns ──
\$taskCols2 = \$pdo->query(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'tasks'\")->fetchAll(PDO::FETCH_COLUMN);
\$taskNeeded = [
    'rule_id' => 'VARCHAR(50) DEFAULT NULL',
    'completed_at' => 'TIMESTAMP DEFAULT NULL',
];
foreach (\$taskNeeded as \$col => \$type) {
    if (!in_array(\$col, \$taskCols2)) {
        \$pdo->exec(\"ALTER TABLE tasks ADD COLUMN {\$col} {\$type}\");
    }
}

echo 'All schema migrations complete.' . PHP_EOL;
"

# Seed team members if empty
php -r "
\$url = parse_url(getenv('DATABASE_URL'));
\$pdo = new PDO('pgsql:host='.\$url['host'].';port='.(\$url['port']??5432).';dbname='.ltrim(\$url['path'],'/'), \$url['user'], \$url['pass']);
\$count = \$pdo->query('SELECT COUNT(*) FROM team_members')->fetchColumn();
if (\$count == 0) {
    \$pdo->exec(\"INSERT INTO team_members (name, role, email, max_hours_per_week) VALUES ('Brook', 'SEO + Content', 'brook@doubledtrailers.com', 40)\");
    \$pdo->exec(\"INSERT INTO team_members (name, role, email, max_hours_per_week) VALUES ('Kalib', 'Sales', 'kalib@doubledtrailers.com', 40)\");
    \$pdo->exec(\"INSERT INTO team_members (name, role, email, max_hours_per_week) VALUES ('Brad', 'Marketing', 'brad@doubledtrailers.com', 40)\");
    echo 'Team members seeded.' . PHP_EOL;
}
"

# Add name and team_role columns to user table if missing
php -r "
\$url = parse_url(getenv('DATABASE_URL'));
\$pdo = new PDO('pgsql:host='.\$url['host'].';port='.(\$url['port']??5432).';dbname='.ltrim(\$url['path'],'/'), \$url['user'], \$url['pass']);
\$tables = \$pdo->query(\"SELECT tablename FROM pg_tables WHERE schemaname = 'public'\")->fetchAll(PDO::FETCH_COLUMN);
if (in_array('user', \$tables)) {
    \$cols = \$pdo->query(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'user'\")->fetchAll(PDO::FETCH_COLUMN);
    if (!in_array('name', \$cols)) { \$pdo->exec('ALTER TABLE \"user\" ADD COLUMN name VARCHAR(100)'); }
    if (!in_array('team_role', \$cols)) { \$pdo->exec('ALTER TABLE \"user\" ADD COLUMN team_role VARCHAR(50)'); }
}
echo 'User table updated.' . PHP_EOL;
"

# Clear and rebuild Symfony cache on every deploy
rm -rf /var/www/html/var/cache/
mkdir -p /var/www/html/var/cache/prod
chown -R www-data:www-data /var/www/html/var/cache
chmod -R 777 /var/www/html/var/cache
php /var/www/html/bin/console cache:clear --env=prod --no-warmup 2>&1
php /var/www/html/bin/console cache:warmup --env=prod 2>&1
chown -R www-data:www-data /var/www/html/var/cache
chmod -R 777 /var/www/html/var/cache
chmod -R 777 /var/www/html/var/log
chmod +x /var/www/html/crawl.sh 2>/dev/null || true

# Seed SEMrush if empty
php -r "
\$url = parse_url(getenv('DATABASE_URL'));
\$pdo = new PDO('pgsql:host='.\$url['host'].';port='.(\$url['port']??5432).';dbname='.ltrim(\$url['path'],'/'), \$url['user'], \$url['pass']);
\$count = \$pdo->query('SELECT COUNT(*) FROM semrush_snapshots')->fetchColumn();
if (\$count == 0) {
    \$pdo->exec(\"INSERT INTO semrush_snapshots (domain, organic_keywords, organic_traffic, fetched_at) VALUES ('doubledtrailers.com', 200, 3, NOW())\");
    echo 'SEMrush snapshot seeded.' . PHP_EOL;
}
"

echo "Startup complete."

# Start Apache
exec apache2-foreground