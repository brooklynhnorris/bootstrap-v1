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

\$pdo->exec('CREATE TABLE IF NOT EXISTS seo_rules (
    id SERIAL PRIMARY KEY,
    rule_id VARCHAR(30) NOT NULL UNIQUE,
    name TEXT NOT NULL DEFAULT \\'\\',
    category VARCHAR(100) DEFAULT NULL,
    tier VARCHAR(50) DEFAULT \\'A\\',
    trigger_source TEXT DEFAULT \\'page_crawl_snapshots\\',
    trigger_condition TEXT DEFAULT \\'\\',
    trigger_sql TEXT DEFAULT \\'\\',
    threshold TEXT DEFAULT \\'\\',
    diagnosis TEXT DEFAULT \\'\\',
    action_output TEXT DEFAULT \\'\\',
    priority VARCHAR(20) DEFAULT \\'Medium\\',
    assigned VARCHAR(100) DEFAULT \\'Brook\\',
    ai_relevance TEXT DEFAULT \\'\\',
    full_text TEXT DEFAULT \\'\\',
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),
    updated_by VARCHAR(100) DEFAULT \\'system\\',
    created_at TIMESTAMP DEFAULT NOW()
)');
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

# Seed rules from system-prompt.txt into seo_rules table (first deploy only — skips if already populated)
php /var/www/html/bin/console app:seed-rules 2>&1 || echo "  [WARN] Rule seeding skipped"

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