<?php

use App\Entity\User;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . "/vendor/autoload.php";

if (file_exists(__DIR__ . "/.env")) {
    (new Dotenv())->usePutenv()->bootEnv(__DIR__ . "/.env");
}

$kernelClass = "App\\Kernel";
$kernel = new $kernelClass($_ENV["APP_ENV"] ?? "prod", (bool) ($_ENV["APP_DEBUG"] ?? false));
$kernel->boot();

$container = $kernel->getContainer();
$em = $container->get("doctrine")->getManager();
$hasher = $container->get("security.password_hasher");

$email = trim(readline("Admin email: "));
$pass  = trim(readline("Admin password: "));

if ($email === "" || $pass === "") {
    fwrite(STDERR, "Email and password are required.\n");
    exit(1);
}

$existing = $em->getRepository(User::class)->findOneBy(["email" => $email]);
if ($existing) {
    fwrite(STDERR, "User already exists: {$email}\n");
    exit(1);
}

$user = new User();
$user->setEmail($email);
$user->setRoles(["ROLE_ADMIN"]);
$user->setPassword($hasher->hashPassword($user, $pass));

$em->persist($user);
$em->flush();

echo "✅ Created admin user: {$email}\n";