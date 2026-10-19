<?php

return [
    'default' => env('BROADCAST_CONNECTION', 'claviron'),
    'connections' => [
        'claviron' => [
            'driver' => 'claviron',
            'key' => env('PUSHER_APP_KEY'),
            'secret' => env('PUSHER_APP_SECRET'),
            // Demo only: routes/channels.php remains the authorization boundary.
            'require_authenticated_user' => false,
        ],
        'log' => [
            'driver' => 'log',
        ],
        'null' => [
            'driver' => 'null',
        ],
    ],
];
