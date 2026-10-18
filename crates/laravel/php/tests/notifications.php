<?php

declare(strict_types=1);

// Adapter contract only: no application boot, queue backend, mail or runtime.
require $argv[1];

use Claviron\Laravel\Notifications\Handler;
use Claviron\Laravel\Notifications\Request;
use Illuminate\Contracts\Notifications\Dispatcher;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Notifications\Notification;

$dispatcher = new class implements Dispatcher {
    public array $sent = [];
    public bool $fail = false;
    public function send($notifiables, $notification) {
        if ($this->fail) { throw new RuntimeException('Admission failed'); }
        $this->sent[] = [$notifiables, $notification];
    }
    public function sendNow($notifiables, $notification, ?array $channels = null) {
        throw new LogicException('Notification adapter bypassed Laravel queue policy');
    }
};
$handler = new class($dispatcher) extends Handler {
    protected function recipient(Request $request): object {
        return (object) ['reference' => $request->recipient];
    }
    protected function notification(Request $request): Notification {
        return match ($request->name) {
            'queued' => new class extends Notification implements ShouldQueue {},
            'sync' => new Notification(),
            default => throw new InvalidArgumentException('Notification is not allowed'),
        };
    }
};
$check = static function (bool $valid): void {
    if (!$valid) { throw new LogicException('Notification contract regression'); }
};
$request = new Request('queued', 'recipient-reference', ['exact' => '1.1234567890123456']);
$check(Request::fromPayload($request->toPayload())->data === $request->data);
$check($handler->handle($request->toPayload())['status'] === 'queued');
$check(count($dispatcher->sent) === 1 && $dispatcher->sent[0][0]->reference === 'recipient-reference');
$check($handler->handle((new Request('sync', 'recipient-reference'))->toPayload())['status'] === 'dispatched');
foreach ([null, [], ['version' => 2], ['version' => 1, 'name' => '', 'recipient' => 'x', 'data' => []]] as $invalid) {
    try {
        $handler->handle($invalid);
        throw new LogicException('Malformed payload accepted');
    } catch (InvalidArgumentException) {}
}
try {
    $handler->handle((new Request('arbitrary.class', 'recipient-reference'))->toPayload());
    throw new LogicException('Unknown notification accepted');
} catch (InvalidArgumentException) {}
$dispatcher->fail = true;
try {
    $handler->handle($request->toPayload());
    throw new LogicException('Queue failure swallowed');
} catch (RuntimeException $error) {
    $check($error->getMessage() === 'Admission failed');
}
echo "Notification decode, recipient boundary, queue policy and failure propagation passed.\n";
