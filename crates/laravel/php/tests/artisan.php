<?php

declare(strict_types=1);

// External executable harness; no server, provider fleet or real application.
// php tests/artisan.php /path/to/vendor/autoload.php /path/to/compiled/lumenize
$autoload = realpath($argv[1] ?? '') ?: throw new LogicException('Supply a Composer autoloader');
$binary = realpath($argv[2] ?? '') ?: throw new LogicException('Supply the compiled Lumenize binary');
$base = sys_get_temp_dir().'/example-artisan-'.bin2hex(random_bytes(8));
$check = static function (bool $condition, string $message): void {
    if (! $condition) { throw new LogicException($message); }
};

mkdir($base.'/bootstrap', 0777, true);
mkdir($base.'/vendor', 0777, true);
file_put_contents($base.'/vendor/autoload.php', '<?php require '.var_export($autoload, true).';');
copy(__DIR__.'/fixtures/artisan/bootstrap/app.php', $base.'/bootstrap/app.php');
copy(__DIR__.'/fixtures/artisan/artisan.php', $base.'/artisan');

try {
    $execute = static function (array $command) use ($base): array {
        $environment = getenv();
        $environment['APP_RUNNING_IN_CONSOLE'] = 'true';
        $process = proc_open($command, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, $base, $environment);
        if (! is_resource($process)) { throw new LogicException('Cannot start console harness'); }
        fwrite($pipes[0], "example input\n");
        fclose($pipes[0]);
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);

        return [proc_close($process), $stdout, $stderr];
    };

    $args = ['example:run', 'literal argument; $VALUE', '--sample=two words'];
    foreach ([[PHP_BINARY, $base.'/artisan'], [$binary]] as $prefix) {
        [$status, $stdout, $stderr] = $execute([...$prefix, ...$args]);
        $check($status === 7, 'Command exit code was not preserved: '.$status.' '.$stderr);
        $lines = explode("\n", trim($stdout));
        $data = json_decode($lines[0], true, flags: JSON_THROW_ON_ERROR);
        $check($data['arguments'] === $args, 'Arguments changed');
        $check($data['command'] === 'example:run', 'Wrong Artisan command');
        $check($data['console'] === 'true' && $data['http'] === false, 'Incorrect execution context');
        $check($data['events'] === false, 'Standalone console claimed an event bus');
        $check($data['prepared'] === false, 'Standalone console claimed prepared host bindings');
        $check($data['stdin'] === 'example input', 'STDIN not inherited');
        $check(($lines[1] ?? null) === 'terminated:7', 'Kernel not terminated');
        $check(str_contains($stderr, 'example stderr'), 'STDERR not inherited');
    }
    [$status, $stdout] = $execute([$binary, 'example:zero']);
    $check($status === 0 && str_contains($stdout, 'terminated:0'), 'Successful completion failed');
    [$status, , $stderr] = $execute([$binary, 'example:fatal']);
    $check($status !== 0 && str_contains($stderr, 'example fatal error'), 'Fatal error was swallowed');
    if (function_exists('posix_kill')) {
        $process = proc_open([$binary, 'example:cancel'], [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, $base);
        $check(is_resource($process), 'Cannot start cancellation harness');
        fwrite($pipes[0], "example input\n");
        fclose($pipes[0]);
        // The execution itself announces readiness; no timing/polling guesses.
        $ready = fgets($pipes[1]);
        $check($ready !== false, 'Console did not reach cancellation loop');
        $pid = proc_get_status($process)['pid'];
        $check(posix_kill($pid, SIGINT), 'Cannot signal the owned console process');
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $status = proc_close($process);
        $check($status === 0 && str_contains($stdout, 'terminated:0'), 'Cooperative Ctrl-C failed: '.$stderr);
    }
    // Boot the real framework in the isolated application, not a deployment.
    // Its providers/cache files stay inside this disposable application root.
    mkdir($base.'/bootstrap/cache');
    copy(__DIR__.'/fixtures/artisan/laravel.php', $base.'/bootstrap/app.php');
    [$status, $stdout, $stderr] = $execute([$binary, '--version']);
    $check($status === 0 && str_contains($stdout, 'Laravel Framework'), 'Laravel console boot failed: '.$stderr);
    [$status, $stdout, $stderr] = $execute([$binary, 'list']);
    $check($status === 0 && str_contains($stdout, 'Available commands:'), 'Laravel command list failed: '.$stderr);
    echo "Artisan console contract passed: argv, terminal streams, termination, exit/fatal errors, Ctrl-C and Laravel kernel.\n";
} finally {
    unlink($base.'/vendor/autoload.php');
    unlink($base.'/bootstrap/app.php');
    unlink($base.'/artisan');
    foreach (glob($base.'/bootstrap/cache/*') ?: [] as $cacheFile) {
        unlink($cacheFile);
    }
    if (is_dir($base.'/bootstrap/cache')) { rmdir($base.'/bootstrap/cache'); }
    rmdir($base.'/vendor');
    rmdir($base.'/bootstrap');
    rmdir($base);
}
