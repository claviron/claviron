<?php

use App\Events\DemoEvent;
use Illuminate\Foundation\Http\Middleware\PreventRequestForgery;
use Illuminate\Support\Facades\Event;

test('it dispatches an ordinary Laravel application event', function () {
    Event::fake([DemoEvent::class]);
    $this->withoutMiddleware(PreventRequestForgery::class);

    $this->getJson(route('events.demo'))
        ->assertAccepted()
        ->assertExactJson(['published' => true]);

    Event::assertDispatched(
        DemoEvent::class,
        fn (DemoEvent $event): bool => $event->message === 'fleet hello',
    );
});
