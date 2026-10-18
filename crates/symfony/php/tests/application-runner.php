<?php

declare(strict_types=1);

use Claviron\Symfony\ApplicationRunner;
use Symfony\Component\Config\Loader\LoaderInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\StreamedResponse;
use Symfony\Component\HttpKernel\HttpKernelInterface;
use Symfony\Component\HttpKernel\Kernel;

function claviron_runtime_available(): bool
{
    return $GLOBALS['runtime'];
}

function claviron_application_receive(): mixed
{
    return array_shift($GLOBALS['messages']);
}

function claviron_application_respond(array $response): void
{
    $GLOBALS['order'][] = 'respond';
    $GLOBALS['responses'][] = $response;
    if ($GLOBALS['failRespond']) {
        throw new RuntimeException('transport failed');
    }
}

require $argv[1] ?? throw new InvalidArgumentException('Pass an installed Symfony autoloader.');
require dirname(__DIR__).'/src/ApplicationRunner.php';

/** No container, application, native runtime or filesystem is booted. */
final class ProbeKernel extends Kernel
{
    public int $boots = 0;
    public int $shutdowns = 0;
    public array $requests = [];

    public function boot(): void { ++$this->boots; }
    public function shutdown(): void { ++$this->shutdowns; }
    public function registerBundles(): iterable { return []; }
    public function registerContainerConfiguration(LoaderInterface $loader): void {}

    public function handle(Request $request, int $type = HttpKernelInterface::MAIN_REQUEST, bool $catch = true): Response
    {
        $this->requests[] = $request;
        return $request->getPathInfo() === '/stream'
            ? new StreamedResponse(static function (): void {})
            : new Response($request->getContent(), 201, ['x-example' => ['one', 'two']]);
    }

    public function terminate(Request $request, Response $response): void
    {
        $GLOBALS['order'][] = 'terminate';
    }
}

function check(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

$runtime = true;
$failRespond = false;
$order = $responses = [];
$message = ['uri' => '/example?x=1', 'method' => 'POST', 'body' => "binary\0body",
    'headers' => ['x-client' => ['one', 'two']], 'server' => ['HTTPS' => 'on']];
$messages = [$message, $message];
$kernel = new ProbeKernel('test', false);
ApplicationRunner::run($kernel);
check($kernel->boots === 1 && $kernel->shutdowns === 1 && count($kernel->requests) === 2, 'Kernel ownership changed.');
check($order === ['respond', 'terminate', 'respond', 'terminate'], 'Response must precede termination.');
check($kernel->requests[0] !== $kernel->requests[1], 'Requests must not share mutable state.');
check($kernel->requests[0]->query->get('x') === '1' && $kernel->requests[0]->isSecure(), 'Request envelope changed.');
check($kernel->requests[0]->headers->all('x-client') === ['one', 'two'], 'Repeated request headers lost.');
check($responses[0]['body'] === "binary\0body" && $responses[0]['status'] === 201, 'Response changed.');
check($responses[0]['headers']['x-example'] === ['one', 'two'] && !isset($responses[0]['claviron']), 'Demo data or header loss.');

foreach (['stream', 'transport', 'event', 'cli'] as $scenario) {
    $runtime = $scenario !== 'cli';
    $failRespond = $scenario === 'transport';
    $order = $responses = [];
    $messages = [$scenario === 'event' ? new stdClass() : array_replace($message, ['uri' => $scenario === 'stream' ? '/stream' : '/example'])];
    $kernel = new ProbeKernel('test', false);
    try {
        ApplicationRunner::run($kernel);
        throw new LogicException('Expected explicit rejection: '.$scenario);
    } catch (RuntimeException|UnexpectedValueException $error) {
        check(!$error instanceof LogicException, $error->getMessage());
    }
    check($kernel->shutdowns === ($runtime ? 1 : 0), 'Failure must release only the owned kernel.');
    check($order === match ($scenario) {
        'stream' => ['terminate'],
        'transport' => ['respond', 'terminate'],
        default => [],
    }, 'Failure cleanup order changed.');
}

echo "Symfony kernel ownership, typed request mapping, buffered responses and failure cleanup passed.\n";
