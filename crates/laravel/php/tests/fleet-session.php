<?php

declare(strict_types=1);

// Pure adapter regression: no Laravel application, database or native runtime.
require $argv[1];

use Claviron\Laravel\Auth\EloquentUserProvider;
use Claviron\Laravel\Auth\FleetSession;
use Claviron\Laravel\Auth\FleetSessionGuard;
use Illuminate\Auth\Authenticatable;
use Illuminate\Contracts\Auth\Authenticatable as AuthenticatableContract;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Hashing\BcryptHasher;
use Illuminate\Http\Request;

$identity = null;
$failure = false;
function claviron_auth(): ?array
{
    global $identity, $failure;
    if ($failure) { throw new RuntimeException('Host authentication unavailable'); }
    return $identity;
}

final class NativeTestUser extends Model implements AuthenticatableContract
{
    use Authenticatable;
    protected $dateFormat = 'Y-m-d H:i:s.u';
    protected $casts = ['id' => 'int', 'status' => 'int', 'created_at' => 'datetime'];
    protected $hidden = ['password'];
}

$provider = new class(new BcryptHasher(), NativeTestUser::class) extends EloquentUserProvider {
    public int $hydrations = 0;
    public function retrieveById($identifier) { throw new LogicException('Duplicate user query'); }
    public function retrieveByCredentials(array $credentials) { throw new LogicException('Duplicate user query'); }
    public function hydrateFromNative(array $attributes): AuthenticatableContract {
        ++$this->hydrations;
        return parent::hydrateFromNative($attributes);
    }
};
$check = static function (bool $condition): void {
    if (!$condition) { throw new RuntimeException('Fleet guard regression'); }
};
$session = new FleetSession($provider, 'uuid', ['status' => 1]);
$guard = new FleetSessionGuard(fn () => $session->user(), Request::create('/'), $provider);
$check($guard->user() === null && $provider->hydrations === 0);

$identity = ['subject' => 'user-one', 'attributes' => [
    'id' => 1, 'uuid' => 'user-one', 'status' => 1, 'password' => 'hidden-hash',
    'created_at' => '2026-01-01T12:00:00.123456+00:00',
    'precise_value' => '1.1234567890123456',
]];
$guard->setRequest(Request::create('/'));
$user = $guard->user();
$check($user instanceof NativeTestUser && $user->exists && !$user->isDirty());
$check($user->id === 1 && $user->created_at->format('u') === '123456');
$check($user->precise_value === '1.1234567890123456');
$check(!array_key_exists('password', $user->toArray()));
$check($guard->user() === $user && $provider->hydrations === 1);

$identity = ['subject' => 'user-two', 'attributes' => ['id' => 2, 'uuid' => 'user-two', 'status' => 1]];
$guard->setRequest(Request::create('/'));
$check($guard->id() === 2 && $provider->hydrations === 2);

$identity = null;
$guard->setRequest(Request::create('/'));
$check($guard->user() === null && $provider->hydrations === 2);

$identity = ['subject' => 'disabled-user', 'attributes' => ['id' => 3, 'uuid' => 'disabled-user', 'status' => -1]];
$guard->setRequest(Request::create('/'));
$check($guard->user() === null && $provider->hydrations === 2);

$identity = ['subject' => 'user-one', 'attributes' => ['id' => 2, 'uuid' => 'user-two', 'status' => 1]];
$guard->setRequest(Request::create('/'));
try {
    $guard->user();
    throw new LogicException('Mismatched subject was accepted');
} catch (RuntimeException $error) {
    $check($error->getMessage() === 'Native user attributes do not match the verified subject.');
}

$failure = true;
$guard->setRequest(Request::create('/'));
try {
    $guard->user();
    throw new LogicException('Host failure was swallowed');
} catch (RuntimeException $error) {
    $check($error->getMessage() === 'Host authentication unavailable');
}
echo "Native Eloquent hydration, zero duplicate queries, request isolation and fail-closed errors verified.\n";
