<?php

return [
    'scheme' => parse_url((string) env('APP_URL'), PHP_URL_SCHEME),
];
