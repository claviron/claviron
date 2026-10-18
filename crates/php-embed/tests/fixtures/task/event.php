<?php

$directory = getenv('OUTPUT_DIR');
if (claviron_event_subscribe_many(['task.test', 'task.other']) !== 2) {
    throw new Exception('task event subscriptions were not created');
}
file_put_contents($directory . '/event.ready', 'yes');

$event = claviron_event_receive(5000);
if (!$event instanceof Claviron\Runtime\Event
    || $event->topic !== 'task.test'
    || $event->payload !== ['source' => 'rust']) {
    throw new Exception('task received an invalid event');
}

claviron_event_publish('task.reply', ['source' => 'php']);
file_put_contents($directory . '/event.received', 'yes');
