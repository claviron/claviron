<?php
$count = 0;
while (($request = claviron_application_receive()) !== null) {
    claviron_application_respond([
        'status' => 200,
        'headers' => ['content-type' => ['text/plain']],
        'body' => $argv[1].'#'.(++$count).' '.$request['method'].' '.$request['uri'].' '.$request['body'],
    ]);
}
