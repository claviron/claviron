<?php

declare(strict_types=1);

namespace Claviron\Symfony;

use Claviron\Runtime\Event;
use Claviron\Symfony\Link\LinkBridge;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\HttpKernel\TerminableInterface;

/** Runs one application-owned Symfony kernel on Claviron's buffered transport. */
final class ApplicationRunner
{
    public static function run(KernelInterface&TerminableInterface $kernel, bool $links = false): void
    {
        if (!function_exists('claviron_runtime_available') || !claviron_runtime_available()) {
            throw new \RuntimeException('Run application.php through Claviron PhpApplicationService.');
        }

        try {
            $kernel->boot();
            $linkBridge = $links ? LinkBridge::forKernel($kernel) : null;

            while (($message = claviron_application_receive()) !== null) {
                if ($message instanceof Event && $linkBridge !== null) {
                    if ($linkBridge->deliver($message)) {
                        continue;
                    }
                    throw new \UnexpectedValueException('The Symfony adapter does not handle application event '.$message->topic.'.');
                }
                if (!is_array($message)) {
                    throw new \UnexpectedValueException('The Symfony HTTP adapter does not handle application events.');
                }

                $request = self::request($message);
                $response = $kernel->handle($request);

                try {
                    $content = $response->getContent();
                    if ($content === false) {
                        throw new \RuntimeException('The buffered Claviron application adapter cannot send a streamed response.');
                    }

                    claviron_application_respond([
                        'status' => $response->getStatusCode(),
                        'headers' => $response->headers->all(),
                        'body' => $content,
                    ]);
                } finally {
                    $kernel->terminate($request, $response);
                    unset($request, $response, $message);
                }
            }
        } finally {
            $kernel->shutdown();
        }
    }

    /**
     * @param array{uri: string, method: string, body: string,
     *     headers: array<string, list<string>>, server?: array<string, mixed>} $message
     */
    private static function request(array $message): Request
    {
        $request = Request::create(
            $message['uri'] === '' ? '/' : $message['uri'],
            $message['method'],
            server: $message['server'] ?? [],
            content: $message['body'],
        );

        foreach ($message['headers'] as $name => $values) {
            $request->headers->set($name, $values);
        }

        return $request;
    }
}
