<?php

declare(strict_types=1);

// Pure translation contract: no Laravel application, database, queue or host.
require $argv[1] ?? throw new RuntimeException('Pass an installed Laravel vendor/autoload.php.');

use Claviron\Laravel\Auth\DispatchAuthEvent;
use Claviron\Laravel\Auth\Events\AuthEvent;
use Illuminate\Auth\EloquentUserProvider;
use Illuminate\Auth\Events\Login;
use Illuminate\Auth\Events\Logout;
use Illuminate\Auth\Events\Registered;
use Illuminate\Auth\GenericUser;
use Illuminate\Container\Container;
use Illuminate\Events\Dispatcher;

$user = new GenericUser(['id' => 1, 'uuid' => 'subject-one']);
$users = new class($user) extends EloquentUserProvider {
    public array $lookups = [];
    public function __construct(private readonly GenericUser $user) {}
    public function retrieveByCredentials(array $credentials)
    {
        $this->lookups[] = $credentials;

        return $credentials === ['uuid' => 'subject-one'] ? $this->user : null;
    }
};
$dispatcher = new Dispatcher(new Container());
$received = [];
foreach ([Login::class, Logout::class, Registered::class] as $class) {
    $dispatcher->listen($class, function ($event) use (&$received): void { $received[] = $event; });
}
$listener = new DispatchAuthEvent($users, $dispatcher, 'uuid');
$check = static function (bool $condition): void {
    if (!$condition) { throw new RuntimeException('Auth event translation contract failed.'); }
};
foreach (['login', 'logout', 'registered'] as $kind) {
    $payload = ['version' => 1, 'kind' => $kind, 'subject' => 'subject-one'];
    if ($kind !== 'registered') { $payload['guard'] = 'web'; }
    if ($kind === 'login') {
        $payload += [
            'remember' => true,
            'username' => 'example-user',
            'name' => 'Example User',
            'occurred_at' => '2026-01-01T00:00:00Z',
            'ip' => '192.0.2.1',
            'identity' => '01234567-89ab-4cde-8fab-0123456789ab',
        ];
    }
    if ($kind === 'logout') { $payload['username'] = 'example-user'; }
    $decoded = AuthEvent::fromPayload($payload);
    $check($decoded->username === ($kind === 'registered' ? null : 'example-user'));
    $listener->handle($decoded);
}
$check(count($received) === 3);
$check($received[0] instanceof Login && $received[0]->user === $user && $received[0]->guard === 'web' && $received[0]->remember === true);
$check($received[1] instanceof Logout && $received[1]->user === $user && $received[1]->guard === 'web');
$check($received[2] instanceof Registered && $received[2]->user === $user);
$check(AuthEvent::fromPayload(['version' => 1, 'kind' => 'logout', 'subject' => 'subject-one', 'guard' => 'web'])->username === null);
foreach ([
    ['version' => 1, 'kind' => 'failed', 'subject' => 'subject-one'],
    ['version' => 1, 'kind' => 'login', 'subject' => 'subject-one'],
    ['version' => 1, 'kind' => 'registered', 'subject' => ''],
    ['version' => 2, 'kind' => 'registered', 'subject' => 'subject-one'],
    ['version' => 1, 'kind' => 'login', 'subject' => 'subject-one', 'guard' => 'web', 'remember' => 'false'],
    ['version' => 1, 'kind' => 'logout', 'subject' => 'subject-one', 'guard' => 'web', 'username' => 42],
] as $payload) {
    try { AuthEvent::fromPayload($payload); throw new LogicException('Invalid payload accepted.'); }
    catch (InvalidArgumentException) {}
}
try {
    $listener->handle(AuthEvent::fromPayload(['version' => 1, 'kind' => 'registered', 'subject' => 'missing']));
    throw new LogicException('Missing subject accepted.');
} catch (RuntimeException $error) {
    $check($error->getMessage() === 'Claviron auth event subject not found.');
}
$check(count($received) === 3);
echo "Native Login/Logout/Registered translation, payload validation and missing-subject rejection passed.\n";
