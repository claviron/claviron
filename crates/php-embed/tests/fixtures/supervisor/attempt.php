<?php
$key = __DIR__ . '/' . getenv('CLAVIRON_PHP_PROGRAM') . '-' . getenv('CLAVIRON_PHP_INSTANCE');
$lock = fopen($key . '.lock', 'c');
if (!flock($lock, LOCK_EX | LOCK_NB)) { file_put_contents(__DIR__ . '/overlap', 'yes'); exit(2); }
$n = file_exists($key) ? (int)file_get_contents($key) + 1 : 1;
file_put_contents($key, (string)$n);
if (getenv('HOLD') === 'yes') {
    while (!file_exists(__DIR__ . '/release')) { usleep(1000); }
} elseif (getenv('DAEMON') === 'yes') {
    while (!claviron_is_cancelled()) { usleep(1000); }
} elseif ($n < (int)getenv('SUCCEED_AT')) { exit(1); }
