#!/usr/bin/env php
<?php
declare(strict_types=1);
if (__LINE__ !== 4) { throw new Exception('shebang changed source line numbers'); }
require __DIR__ . '/identity.php';
if (getenv('CLAVIRON_PHP_RUNNER') !== false || isset($_SERVER['CLAVIRON_PHP_RUNNER'])) {
    throw new Exception('program inherited HTTP route runner');
}
$name = $_SERVER['CLAVIRON_PHP_PROGRAM'];
$instance = $_SERVER['CLAVIRON_PHP_INSTANCE'];
if (getenv('CLAVIRON_PHP_PROGRAM') !== $name || getenv('CLAVIRON_PHP_INSTANCE') !== $instance) { throw new Exception('program identity mismatch'); }
$expectedArgs = [__FILE__, '--program=' . $name, 'two words', '', '$HOME', 'çalış'];
if ($argv !== $expectedArgs || $argc !== count($expectedArgs) || $_SERVER['argv'] !== $argv || $_SERVER['argc'] !== $argc) { throw new Exception('script arguments mismatch'); }
if (getopt('', ['program:'])['program'] !== $name) { throw new Exception('native getopt arguments mismatch'); }
$argv[1] = 'local-' . $instance;
$value = getenv('PROGRAM_VALUE');
if ($value !== $name || $_SERVER['PROGRAM_VALUE'] !== $value) { throw new Exception('bootstrap environment mismatch'); }
if (getenv('FILE_VALUE') !== $name || $_SERVER['FILE_VALUE'] !== $name) { throw new Exception('program environment mismatch'); }
if (isset($_SERVER['REQUEST_METHOD']) || isset($_SERVER['HTTPS'])) { throw new Exception('program execution has HTTP metadata'); }
if ($_SERVER['CLAVIRON_PHP_MODEL'] !== 'task' || getenv('CLAVIRON_PHP_MODEL') !== 'task') { throw new Exception('program does not know its model'); }
// PHP defines these in the CLI SAPI only. A program is console-shaped and
// console code reaches for them unguarded — Laravel's Collision renderer dies
// on `Undefined constant ...\STDOUT` while printing a different error.
foreach (['STDIN', 'STDOUT', 'STDERR'] as $stream) {
    if (!defined($stream) || !is_resource(constant($stream))) { throw new Exception('missing console stream: ' . $stream); }
}
if (!putenv('PROGRAM_VALUE=' . $value . ':local')) { throw new Exception('putenv failed'); }
$prefix = getenv('OUTPUT_DIR') . '/' . $name . '-' . $instance;
echo 'dedicated-start:' . $name . ':' . $instance;
flush();
file_put_contents($prefix . '.ready', $_SERVER['CLAVIRON_THREAD_ID']);
while (!claviron_is_cancelled()) {
    if ($argv[1] !== 'local-' . $instance || $_SERVER['argv'] !== $expectedArgs) { throw new Exception('arguments crossed instances'); }
    if (getenv('PROGRAM_VALUE') !== $value . ':local') { throw new Exception('environment crossed threads'); }
    usleep(1000);
}
file_put_contents($prefix . '.stopped', 'yes');
