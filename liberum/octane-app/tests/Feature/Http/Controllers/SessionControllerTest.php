<?php

declare(strict_types=1);

use App\Models\User;
use Illuminate\Foundation\Testing\RefreshDatabase;

uses(RefreshDatabase::class);

it('keeps the visit count and selected user in the same session', function (): void {
    $user = User::factory()->create([
        'name' => 'Session User',
        'email' => 'session@example.com',
    ]);

    $first = $this->getJson(route('session.show'))
        ->assertOk()
        ->assertJsonPath('session.visits', 1)
        ->assertJsonPath('user.id', $user->id)
        ->assertJsonPath('user.name', 'Session User');
    $sessionId = $first->json('session.id');
    $firstSeenAt = $first->json('session.first_seen_at');
    $sessionCookie = $first->getCookie(config('session.cookie'))?->getValue();

    $this->withCredentials()
        ->withCookie(config('session.cookie'), $sessionCookie)
        ->getJson(route('session.show'))
        ->assertOk()
        ->assertJsonPath('session.id', $sessionId)
        ->assertJsonPath('session.visits', 2)
        ->assertJsonPath('session.first_seen_at', $firstSeenAt)
        ->assertJsonPath('user.email', 'session@example.com');
});

it('forgets the previous data and rotates the session id', function (): void {
    User::factory()->create();
    $first = $this->getJson(route('session.show'));
    $sessionId = $first->json('session.id');
    $sessionCookie = $first->getCookie(config('session.cookie'))?->getValue();

    $reset = $this->withCredentials()
        ->withCookie(config('session.cookie'), $sessionCookie)
        ->deleteJson(route('session.destroy'))
        ->assertOk()
        ->assertJsonPath('previous_id', $sessionId)
        ->assertJsonPath('session.visits', 0);

    expect($reset->json('session.id'))->not->toBe($sessionId);

    $resetCookie = $reset->getCookie(config('session.cookie'))?->getValue();

    $this->withCookie(config('session.cookie'), $resetCookie)
        ->getJson(route('session.show'))
        ->assertOk()
        ->assertJsonPath('session.id', $reset->json('session.id'))
        ->assertJsonPath('session.visits', 1);
});
