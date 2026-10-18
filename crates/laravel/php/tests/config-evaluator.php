<?php

declare(strict_types=1);

// Isolated helper-context contract. No application bootstrap/providers/DB.
// Supply an installed Laravel Composer autoloader; no deployment config reads.
require $argv[1];
require_once __DIR__.'/../src/Config/ConfigEvaluator.php';

use Claviron\Laravel\Config\ConfigEvaluator;
use Illuminate\Support\Str;

$check = static function (bool $condition): void {
    if (! $condition) { throw new LogicException('Config evaluator assertion failed'); }
};
$base = sys_get_temp_dir().'/example-config-context';
ConfigEvaluator::prepare($base, ['SAMPLE_VALUE' => 'first', 'SAMPLE_BOOL' => 'false']);
$check(env('SAMPLE_VALUE') === 'first');
$check(env('SAMPLE_BOOL') === false);
$check(env('MISSING_SAMPLE', 'fallback') === 'fallback');
$check(env('PATH') === null); // no implicit process-environment fallback
$check(base_path('document.php') === $base.'/document.php');
$check(Str::snake('ExampleConfig') === 'example_config');
$check(! app()->isBooted());
$check(config('app.name') === null); // Laravel's config files are not loaded
ConfigEvaluator::prepare($base, ['SAMPLE_VALUE' => 'second']);
$check(env('SAMPLE_VALUE') === 'second');
$check(env('SAMPLE_BOOL') === null);
echo "config evaluator helper context OK\n";
