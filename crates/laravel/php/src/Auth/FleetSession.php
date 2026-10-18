<?php

declare(strict_types=1);

namespace Claviron\Laravel\Auth;

use Illuminate\Contracts\Auth\Authenticatable;
use Illuminate\Contracts\Auth\UserProvider;
use RuntimeException;

/** Maps the current native request's verified identity to a Laravel user. */
final class FleetSession
{
    public function __construct(
        private readonly UserProvider $provider,
        private readonly ?string $subjectKey = null,
        private readonly array $constraints = [],
    ) {
        if (!$provider instanceof NativeUserProvider) {
            throw new RuntimeException('The claviron guard requires a NativeUserProvider; database fallback would load the user twice.');
        }
        if ($subjectKey === '' || ($subjectKey === null && $constraints !== [])) {
            throw new RuntimeException('Fleet user constraints require a subject_key.');
        }
    }

    public function user(): ?Authenticatable
    {
        if (!function_exists('claviron_auth')) {
            throw new RuntimeException('The claviron auth guard requires Claviron auth + php_embed.');
        }

        $identity = \claviron_auth();

        if ($identity === null) {
            return null;
        }

        $attributes = $identity['attributes'] ?? null;
        if (!is_array($attributes)) {
            throw new RuntimeException('Native authentication did not provide raw user attributes.');
        }
        foreach ($this->constraints as $key => $expected) {
            if (!array_key_exists($key, $attributes)
                || (is_array($expected) ? !in_array($attributes[$key], $expected) : $attributes[$key] != $expected)) {
                return null;
            }
        }
        if ($this->subjectKey !== null && (string) ($attributes[$this->subjectKey] ?? '') !== $identity['subject']) {
            throw new RuntimeException('Native user attributes do not match the verified subject.');
        }
        $user = $this->provider->hydrateFromNative($attributes);
        $subject = $this->subjectKey === null ? $user->getAuthIdentifier() : ($attributes[$this->subjectKey] ?? null);
        if ((string) $subject !== $identity['subject']) {
            throw new RuntimeException('Native user attributes do not match the verified subject.');
        }
        return $user;
    }
}
