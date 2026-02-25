    #!/bin/bash
set -e

echo "Running startup tasks..."

# Strip BOM from index.php
sed -i '1s/^\xEF\xBB\xBF//' /var/www/html/public/index.php 2>/dev/null || true
find /var/www/html/src -name "*.php" -exec sed -i 's/\r//' {} \;

# Wait for database to be ready using DATABASE_URL
until php -r "
\$url = parse_url(getenv('DATABASE_URL'));
new PDO('pgsql:host='.\$url['host'].';port='.(\$url['port']??5432).';dbname='.ltrim(\$url['path'],'/'), \$url['user'], \$url['pass']);
" 2>/dev/null; do
  echo "Waiting for database..."
  sleep 2
done

echo "Database ready. Running migrations..."

# Create core tables if they don't exist
php bin/console doctrine:schema:update --force --no-interaction 2>/dev/null || true

# Create data tables
php -r "
\$url = parse_url(getenv('DATABASE_URL'));
\$pdo = new PDO('pgsql:host='.\$url['host'].';port='.(\$url['port']??5432).';dbname='.ltrim(\$url['path'],'/'), \$url['user'], \$url['pass']);
\$pdo->exec('CREATE TABLE IF NOT EXISTS semrush_snapshots (id SERIAL PRIMARY KEY, domain VARCHAR(255) NOT NULL, organic_keywords INT DEFAULT 0, organic_traffic INT DEFAULT 0, fetched_at TIMESTAMP NOT NULL)');
\$pdo->exec('CREATE TABLE IF NOT EXISTS gsc_snapshots (id SERIAL PRIMARY KEY, query VARCHAR(500) NOT NULL, page TEXT NOT NULL, clicks INT DEFAULT 0, impressions INT DEFAULT 0, ctr FLOAT DEFAULT 0, position FLOAT DEFAULT 0, fetched_at TIMESTAMP NOT NULL)');
\$pdo->exec('CREATE TABLE IF NOT EXISTS ga4_snapshots (id SERIAL PRIMARY KEY, page_path TEXT NOT NULL, sessions INT DEFAULT 0, pageviews INT DEFAULT 0, bounce_rate FLOAT DEFAULT 0, conversions INT DEFAULT 0, fetched_at TIMESTAMP NOT NULL)');
\$pdo->exec('CREATE TABLE IF NOT EXISTS team_members (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, role VARCHAR(50) NOT NULL, email VARCHAR(255), max_hours_per_week INT DEFAULT 40, is_active BOOLEAN DEFAULT true)');
\$pdo->exec(\"CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    rule_id VARCHAR(20),
    assigned_to VARCHAR(100),
    assigned_role VARCHAR(50),
    status VARCHAR(20) DEFAULT 'pending',
    priority VARCHAR(20) DEFAULT 'medium',
    estimated_hours FLOAT DEFAULT 1,
    logged_hours FLOAT DEFAULT 0,
    due_date DATE,
    recheck_type VARCHAR(50),
    recheck_date DATE,
    recheck_verified BOOLEAN DEFAULT false,
    recheck_result VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
)\");

// Add columns if they dont exist (for existing tables)
\$cols = \$pdo->query(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'tasks'\")->fetchAll(PDO::FETCH_COLUMN);
if (!in_array('recheck_type', \$cols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_type VARCHAR(50)'); }
if (!in_array('recheck_date', \$cols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_date DATE'); }
if (!in_array('recheck_verified', \$cols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_verified BOOLEAN DEFAULT false'); }
if (!in_array('recheck_result', \$cols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN recheck_result VARCHAR(20)'); }
if (!in_array('logged_hours', \$cols)) { \$pdo->exec('ALTER TABLE tasks ADD COLUMN logged_hours FLOAT DEFAULT 0'); }

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

# Add name and team_role to user table if missing
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

chmod -R 777 /var/www/html/var/cache
chmod -R 777 /var/www/html/var/log

# Fetch fresh data on startup
echo "Fetching GSC and GA4 data..."
php /var/www/html/bin/console app:fetch-gsc 2>/dev/null || echo "GSC fetch failed"
php /var/www/html/bin/console app:fetch-ga4 2>/dev/null || echo "GA4 fetch failed"

# Insert SEMrush snapshot if none exists
php -r "
\$url = parse_url(getenv('DATABASE_URL'));
\$pdo = new PDO('pgsql:host='.\$url['host'].';port='.(\$url['port']??5432).';dbname='.ltrim(\$url['path'],'/'), \$url['user'], \$url['pass']);
\$count = \$pdo->query('SELECT COUNT(*) FROM semrush_snapshots')->fetchColumn();
if (\$count == 0) {
    \$pdo->exec(\"INSERT INTO semrush_snapshots (domain, organic_keywords, organic_traffic, fetched_at) VALUES ('doubledtrailers.com', 200, 3, NOW())\");
    echo 'SEMrush snapshot seeded.' . PHP_EOL;
}
"

# Set up cron to fetch data daily at 6am
apt-get update -qq && apt-get install -y -qq cron > /dev/null 2>&1
echo "0 6 * * * cd /var/www/html && php bin/console app:fetch-gsc >> /var/log/cron.log 2>&1" > /etc/cron.d/logiri
echo "0 6 * * * cd /var/www/html && php bin/console app:fetch-ga4 >> /var/log/cron.log 2>&1" >> /etc/cron.d/logiri
chmod 0644 /etc/cron.d/logiri
crontab /etc/cron.d/logiri
service cron start

echo "Startup complete."

# Start Apache
exec apache2-foreground

    
