<?php

$verificationFailed = false;

function FAILED(string $message): void
{
    global $verificationFailed;
    $verificationFailed = true;
    fwrite(STDERR, "❌ \033[1;31m[FAILED]\033[0m {$message}\n");
}

function WARNING(string $message): void
{
    fwrite(STDERR, "\033[1;33m[WARNING]\033[0m {$message}\n");
}

function INFO(string $message): void
{
    fwrite(STDERR, "\033[1;34m[INFO]\033[0m {$message}\n");
}

function SUCCESS(string $message): void
{
    fwrite(STDERR, "\033[1;32m[✔]\033[0m {$message}\n");
}

// Read the installed runtime's effective settings without changing configuration.
function verify_default_mysql_socket(): void
{
    if (!extension_loaded('mysqli') && !extension_loaded('pdo_mysql')) {
        INFO("MySQL socket check skipped: no MySQL extension loaded.");
        return;
    }

    $sockets = [];
    foreach (['/var/lib/mysql/mysql.sock', '/run/mysqld/mysqld.sock', '/var/run/mysqld/mysqld.sock', '/var/run/mysql/mysql.sock', '/tmp/mysql.sock'] as $path) {
        if (@filetype($path) === 'socket') {
            $sockets[realpath($path) ?: $path] = true;
        }
    }
    $suggestion = count($sockets) === 1 ? array_key_first($sockets) : null;

    foreach (['mysqli' => 'mysqli.default_socket', 'pdo_mysql' => 'pdo_mysql.default_socket'] as $extension => $setting) {
        if (!extension_loaded($extension)) {
            continue;
        }
        $value = ini_get($setting);
        if ($value !== false && $value !== '' && @filetype($value) === 'socket') {
            if (is_writable($value)) {
                SUCCESS("{$setting}={$value}; socket is writable (database authentication not tested).");
            } else {
                FAILED("{$setting}={$value}; socket is not writable by the current user. Review socket permissions.");
            }
        } elseif ($suggestion !== null) {
            FAILED("{$setting}=" . ($value ?: '(unset)') . "; set {$setting}={$suggestion} in php.ini or the scanned INI override.");
        } else {
            FAILED("{$setting}=" . ($value ?: '(unset)') . "; no unique standard local MySQL socket found. Set a valid socket path in php.ini or the scanned INI override.");
        }
    }
}

function verify_default_postgres_socket(): void
{
    if (!extension_loaded('pgsql') && !extension_loaded('pdo_pgsql')) {
        INFO("PostgreSQL socket check skipped: no PostgreSQL extension loaded.");
        return;
    }

    $host = getenv('PGHOST');
    if ($host !== false && $host !== '' && $host[0] !== '/') {
        INFO("PGHOST selects a hostname or host list; local socket defaults are not checked.");
        return;
    }
    $port = getenv('PGPORT') ?: '5432';
    if (!ctype_digit($port)) {
        INFO("PGPORT is not a single numeric port; local socket defaults are not checked.");
        return;
    }

    $directories = $host ? [$host] : ['/run/postgresql', '/var/run/postgresql', '/tmp'];
    $found = [];
    foreach ($directories as $directory) {
        $socket = $directory . '/.s.PGSQL.' . $port;
        if (@filetype($socket) === 'socket') {
            $found[realpath($directory) ?: $directory] = is_writable($socket);
        }
    }
    foreach ($found as $directory => $writable) {
        $report = $writable ? 'SUCCESS' : 'FAILED';
        $report("PostgreSQL socket {$directory}/.s.PGSQL.{$port}" . ($writable ? ' is writable by the current user' : ' is not writable by the current user') . "; database authentication not tested.");
    }
    if (!$found) {
        if ($host) {
            FAILED("PGHOST selects a directory without a PostgreSQL socket for port {$port}. Correct PGHOST or the connection host.");
        } else {
            WARNING("No PostgreSQL socket found at the standard paths for port {$port}; the local default could not be verified.");
        }
    }
    if (!$host && count($found) === 1) {
        INFO('PGHOST is unset; assumed local socket directory is ' . array_key_first($found) . ". libpq's compiled default is not exposed by PHP.");
    }
    INFO("PostgreSQL socket paths are not php.ini settings; configure the connection host or PGHOST if needed.");
}

function verify_certificates(): void
{
    $systemCa = null;
    foreach (['/etc/pki/tls/certs/ca-bundle.crt', '/etc/ssl/certs/ca-certificates.crt', '/etc/ssl/cert.pem', '/usr/local/share/certs/ca-root-nss.crt'] as $path) {
        if (is_file($path) && is_readable($path)) {
            $systemCa = $path;
            break;
        }
    }

    foreach (['openssl' => 'openssl.cafile', 'curl' => 'curl.cainfo'] as $extension => $setting) {
        if (!extension_loaded($extension)) {
            INFO("{$setting} check skipped: {$extension} is not loaded.");
            continue;
        }
        $value = ini_get($setting);
        if ($value !== false && $value !== '' && is_file($value) && is_readable($value)) {
            if (extension_loaded('openssl') && @openssl_x509_read(file_get_contents($value)) === false) {
                FAILED("{$setting}={$value} does not contain a readable PEM certificate.");
            } else {
                SUCCESS("{$setting}={$value}; CA file is readable.");
            }
        } elseif ($systemCa !== null) {
            $report = $value ? 'FAILED' : 'WARNING';
            $report("{$setting}=" . ($value ?: '(unset)') . "; recommended explicit setting: {$setting}={$systemCa} in php.ini or the scanned INI override.");
        } else {
            $report = $value ? 'FAILED' : 'WARNING';
            $report("{$setting}=" . ($value ?: '(unset)') . "; no readable standard system CA file found. Review php.ini and system trust configuration.");
        }
    }

    if (extension_loaded('openssl')) {
        $locations = openssl_get_cert_locations();
        INFO('OpenSSL compiled CA file: ' . $locations['default_cert_file'] . ".");
        $capath = ini_get('openssl.capath');
        if ($capath !== false && $capath !== '' && (!is_dir($capath) || !is_readable($capath))) {
            FAILED("openssl.capath={$capath} is not a readable directory; correct it in php.ini or the scanned INI override.");
        }
    }
    INFO("CA checks do not test remote TLS connections or validate application client certificates/private keys. PostgreSQL TLS uses libpq connection settings.");
}

INFO('Configuration report: ' . ($argv[1] ?? 'current scan'));
INFO('Loaded php.ini: ' . (php_ini_loaded_file() ?: '(none)'));
if (php_ini_scanned_files()) {
    INFO('Additional INI files: ' . trim(php_ini_scanned_files()));
}
verify_default_mysql_socket();
verify_default_postgres_socket();
verify_certificates();

exit($verificationFailed ? 1 : 0);
