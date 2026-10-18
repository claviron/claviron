<?php

if ($_SERVER['CLAVIRON_PHP_INSTANCE'] === '0') {
    switch (getenv('COMPLETION_CASE')) {
        case 'exit':
            exit();
        case 'exit_zero':
            exit(0);
        case 'return':
            return;
        case 'exit_positive':
            exit(7);
        case 'exit_negative':
            exit(-1);
        case 'exception':
            throw new Exception('instance failure');
        default:
            throw new Exception('unknown completion case');
    }
}

while (!file_exists(__DIR__ . '/release')) {
    usleep(1000);
}
