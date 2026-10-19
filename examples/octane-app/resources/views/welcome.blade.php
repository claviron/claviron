<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="csrf-token" content="{{ csrf_token() }}">
    <title>Claviron Laravel Broadcast</title>
    @php
        $buildManifest = json_decode(
            file_get_contents(public_path('build/manifest.json')),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
    @endphp
    <link rel="stylesheet" href="/build/{{ $buildManifest['resources/css/app.css']['file'] }}">
    <script type="module" src="/build/{{ $buildManifest['resources/js/app.js']['file'] }}"></script>
</head>
<body>
<main
    id="echo-demo"
    data-key="{{ env('PUSHER_APP_KEY') }}"
    data-host="{{ env('PUSHER_HOST', 'socket.home') }}"
    data-port="{{ env('PUSHER_PORT', 443) }}"
>
    <header>
        <p class="eyebrow">CLAVIRON · LARAVEL · ECHO</p>
        <h1>Native broadcast, familiar client.</h1>
        <p class="lead">Laravel calls the native broadcaster. Embedded PHP publishes into Orbit. PusherJS receives it over WebSocket.</p>
        <div class="status"><span id="status-dot"></span><span id="status">Connecting…</span></div>
    </header>

    <section class="grid">
        <article class="card">
            <h2>Native broadcast</h2>
            <form id="broadcast-form">
                <input id="message" name="message" value="Hello from Laravel" autocomplete="off">
                <button type="submit">Broadcast</button>
            </form>
            <p class="hint">Published directly from the persistent PHP application.</p>
        </article>

        <article class="card">
            <h2>Client whisper</h2>
            <input id="typing" placeholder="Open another tab and type…" autocomplete="off">
            <p class="hint">Sent as <code>client-typing</code>; the sender is excluded.</p>
        </article>

        <article class="card notification-card">
            <h2>Laravel notification</h2>
            <button id="notification" type="button">Send Notification</button>
            <p id="notification-preview" class="notification-preview">Waiting for a server notification…</p>
            <p class="hint">Delivered through Laravel's native notification channel.</p>
        </article>
    </section>

    <section class="card activity">
        <div class="activity-title">
            <h2>Live activity</h2>
            <button id="clear" class="quiet" type="button">Clear</button>
        </div>
        <ol id="events"></ol>
    </section>
</main>
</body>
</html>
