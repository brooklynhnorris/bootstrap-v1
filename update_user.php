<?php
// Run inside container: php /var/www/html/update_user.php
$url = parse_url(getenv('DATABASE_URL'));
$pdo = new PDO(
    'pgsql:host=' . $url['host'] . ';port=' . ($url['port'] ?? 5432) . ';dbname=' . ltrim($url['path'], '/'),
    $url['user'],
    $url['pass']
);

// Update the first user with name and role
$stmt = $pdo->prepare('UPDATE "user" SET name = ?, team_role = ? WHERE id = (SELECT id FROM "user" LIMIT 1)');
$stmt->execute(['Brook', 'SEO + Content']);

echo "User updated with name 'Brook' and role 'SEO + Content'\n";

// Show all users
$users = $pdo->query('SELECT id, email, name, team_role FROM "user"')->fetchAll(PDO::FETCH_ASSOC);
foreach ($users as $u) {
    echo "  ID: {$u['id']} | Email: {$u['email']} | Name: {$u['name']} | Role: {$u['team_role']}\n";
}