<?php
if (file_exists(__DIR__ . '/fail')) { throw new Exception('injected dedicated failure'); }
if (getenv('APP_ENV') !== 'production') { throw new Exception('environment mismatch'); }
$marker = getenv('OUTPUT_DIR') . '/' . $_SERVER['CLAVIRON_PHP_INSTANCE'];
file_put_contents($marker . '.ready', 'yes');
while (!claviron_is_cancelled()) {
    if (file_exists($marker . '.exit')) { exit((int)file_get_contents($marker . '.exit')); }
    usleep(1000);
}
file_put_contents($marker . '.stopped', 'yes');
