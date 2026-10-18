<?php

declare(strict_types=1);

// Standalone contract test: no application boot, native runtime, DB or mail.
require $argv[1] ?? throw new RuntimeException('Pass the Composer autoload.php path.');

use Claviron\Laravel\Auth\Events\AuthEvent;
use Claviron\Laravel\Console\InvokeServeCommand;
use Claviron\Laravel\Events\FleetEvent;
use Claviron\Laravel\Invocation\Handler;
use Claviron\Laravel\Invocation\Registrar;
use Claviron\Laravel\Invocation\Registry;
use Claviron\Laravel\Invocation\Runner;
use Illuminate\Container\Container;

final class ExampleInvocationHandler implements Handler
{
    public function __construct() { throw new RuntimeException('Registration instantiated a handler.'); }
    public function handle(mixed $payload): mixed { return $payload; }
}

final class ExampleInvocationRegistrar implements Registrar
{
    public static int $calls = 0;
    public function register(Registry $invocations): void
    {
        self::$calls++;
        $invocations->handler(ExampleInvocationHandler::class);
        $invocations->handler(ExampleInvocationHandler::class);
        $invocations->event(AuthEvent::class);
        $invocations->event(AuthEvent::class);
    }
}

final class ConflictingAuthEvent extends FleetEvent
{
    public static function topic(): string { return AuthEvent::topic(); }
    public static function fromPayload(array $payload): self { return new self; }
}

final class ConflictingRegistrar implements Registrar
{
    public function register(Registry $invocations): void
    {
        $invocations->event(AuthEvent::class);
        $invocations->event(ConflictingAuthEvent::class);
    }
}

final class BadHandlerRegistrar implements Registrar
{
    public function register(Registry $invocations): void { $invocations->handler(stdClass::class); }
}

final class BadEventRegistrar implements Registrar
{
    public function register(Registry $invocations): void { $invocations->event('MissingInvocationEvent'); }
}

function check(bool $condition, string $message): void
{
    if (!$condition) { throw new RuntimeException($message); }
}

function rejects(callable $operation, string $exception): void
{
    try { $operation(); } catch (Throwable $failure) {
        check($failure instanceof $exception, 'Wrong failure: '.$failure::class.' '.$failure->getMessage());
        return;
    }
    throw new RuntimeException('Expected '.$exception);
}

$app = new Container;
$registry = Registry::fromConfig($app, [
    'registrars' => [ExampleInvocationRegistrar::class, ExampleInvocationRegistrar::class],
    'budget' => 0,
]);
check($registry->handlers() === [ExampleInvocationHandler::class], 'Handler declarations were not deduplicated.');
check($registry->events() === [AuthEvent::class], 'Event declarations were not deduplicated.');
check(ExampleInvocationRegistrar::$calls === 1, 'Registrar was evaluated twice.');
check($registry->budgetSeconds() === 0, 'Existing unbounded budget changed.');
check($registry->handlerClass(ExampleInvocationHandler::class) === ExampleInvocationHandler::class, 'Registered handler lookup failed.');

$otherProfile = Registry::fromConfig(new Container, []);
check($otherProfile->handlers() === [] && $otherProfile->events() === [], 'Registrations leaked into another application.');
check(Registry::fromConfig($app, ['budget' => 7])->budgetSeconds() === 7, 'Configured budget was not preserved.');

foreach ([
    ['registrars' => 'invalid'],
    ['registrars' => [stdClass::class]],
    ['registrars' => [Registrar::class]],
    ['registrars' => [[]]],
    ['budget' => -1],
    ['budget' => '0'],
    ['registrars' => [BadHandlerRegistrar::class]],
    ['registrars' => [BadEventRegistrar::class]],
    ['registrars' => [ConflictingRegistrar::class]],
] as $invalid) {
    rejects(fn () => Registry::fromConfig($app, $invalid), InvalidArgumentException::class);
}
rejects(fn () => $registry->handlerClass(stdClass::class), InvalidArgumentException::class);
rejects(fn () => $registry->event(AuthEvent::class), LogicException::class);
rejects(fn () => (new Runner($app, $otherProfile))->serve(), InvalidArgumentException::class);
rejects(fn () => (new Runner($app, $registry))->serve(), RuntimeException::class);

$definition = (new InvokeServeCommand)->getDefinition();
check($definition->getArguments() === [], 'invoke:serve still accepts positional registration.');
check(!$definition->hasOption('event') && !$definition->hasOption('budget'), 'invoke:serve still accepts registration/policy options.');

echo "Invocation registry contract passed.\n";
