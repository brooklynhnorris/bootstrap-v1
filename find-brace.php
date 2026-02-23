<?php
$content = file_get_contents('src/Controller/HomeController.php');
preg_match_all('/\{[^\$\n][^\}]*\}/', $content, $matches);
print_r($matches[0]);