<?php

declare(strict_types=1);

namespace Claviron\Laravel\Http\Middleware;

use Closure;
use Illuminate\Auth\AuthenticationException;
use Illuminate\Contracts\Session\Session;
use Illuminate\Http\Request;
use Illuminate\Session\CacheBasedSessionHandler;
use Illuminate\Session\Middleware\StartSession;
use Illuminate\Session\NullSessionHandler;
use Illuminate\Support\Str;
use InvalidArgumentException;
use RuntimeException;
use Symfony\Component\HttpFoundation\Response;

/** Bind Laravel data to this request's verified native session, without cookies. */
class StartNativeSession extends StartSession
{
    protected bool $authenticated = false;

    protected ?string $nativeSessionId = null;

    private ?string $scope = null;

    private ?int $expiresAt = null;

    private ?string $dataId = null;

    public function handle($request, Closure $next, ?string $scope = null)
    {
        if ($scope === null || !preg_match('/\A[a-zA-Z0-9_.-]+\z/', $scope)) {
            throw new InvalidArgumentException('Native session middleware requires an explicit data namespace.');
        }
        if ($this->manager->getDefaultDriver() !== 'claviron') {
            throw new RuntimeException('Native session middleware requires session.driver=claviron.');
        }
        $this->scope = $scope;

        try {
            return parent::handle($request, $next);
        } finally {
            $this->scope = null;
            $this->authenticated = false;
            $this->nativeSessionId = null;
            $this->expiresAt = null;
            $this->dataId = null;
        }
    }

    public function getSession(Request $request)
    {
        $this->authenticated = false;
        $this->nativeSessionId = null;
        $this->expiresAt = null;
        $this->dataId = null;
        $native = $this->nativeIdentity($request);
        if ($native === null && !$this->allowsGuest($request)) {
            throw new AuthenticationException();
        }

        if ($native !== null) {
            $id = $native['session_id'] ?? null;
            $expires = $native['expires_at'] ?? null;
            if (!is_string($id) || !Str::isUuid($id) || !is_int($expires) || $expires <= time()) {
                throw new AuthenticationException();
            }
            if ($this->scope === null) {
                throw new RuntimeException('Native session middleware has no data namespace.');
            }
            $this->authenticated = true;
            $this->nativeSessionId = $id;
            $this->expiresAt = $expires;
            // Laravel requires a 40-character ID. This is not an auth credential.
            $this->dataId = sha1($this->scope.':php:'.$id);
        }

        $session = $this->manager->driver();
        $session->flush();
        $session->setId($this->dataId);
        $session->setHandler($this->authenticated
            ? $this->dataHandler()
            : new NullSessionHandler());

        return $session;
    }

    /** The host owns guard selection, verification, expiry and revocation. */
    protected function nativeIdentity(Request $request): ?array
    {
        if (!function_exists('claviron_auth')) {
            throw new RuntimeException('Native sessions require the Claviron authentication bridge.');
        }

        return \claviron_auth();
    }

    /** Applications may allow a transient login form, never persistent guest data. */
    protected function allowsGuest(Request $request): bool
    {
        return false;
    }

    protected function addCookieToResponse(Response $response, Session $session)
    {
        // Native auth owns its cookie or bearer; no Laravel session cookie.
    }

    protected function saveSession($request)
    {
        if ($this->authenticated && $this->expiresAt > time()) {
            // Recompute the remaining lifetime after the handler. Laravel data
            // regeneration must not create a session detached from native auth.
            $session = $request->session();
            $session->setId($this->dataId);
            $session->setHandler($this->dataHandler());
            parent::saveSession($request);
        }
    }

    private function dataHandler(): CacheBasedSessionHandler
    {
        $container = $this->manager->getContainer();
        $store = $container->make('config')->get('session.store') ?: null;

        return new CacheBasedSessionHandler(
            $container->make('cache')->store($store),
            ($this->expiresAt - time()) / 60,
        );
    }
}
