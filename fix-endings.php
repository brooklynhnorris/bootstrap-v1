<?php
$file = file_get_contents('src/Controller/HomeController.php');
$fixed = str_replace("\r\n", "\n", $file);
$fixed = str_replace("\r", "\n", $fixed);
file_put_contents('src/Controller/HomeController.php', $fixed);
echo 'Done - ' . strlen($fixed) . ' bytes written' . PHP_EOL;