<?php

declare(strict_types=1);

namespace Claviron\Laravel\Invocation;

use Claviron\Laravel\Events\FleetEvent;
use Illuminate\Contracts\Container\Container;
use InvalidArgumentException;
use LogicException;
use ReflectionClass;

/** Container-local declaration catalog. Only the serving task subscribes. */
final class Registry
{
    /** @var array<class-string<Handler>, true> */
    private array $handlers = [];

    /** @var array<class-string, true> */
    private array $events = [];

    /** @var array<string, class-string> */
    private array $topics = [];

    private bool $sealed = false;

    private function __construct(private readonly int $budgetSeconds) {}

    /**
     * Resolve the registrar list already selected by the application/profile.
     * No APP_PROFILE convention, native runtime, subscription or IO is required.
     */
    public static function fromConfig(Container $app, array $config): self
    {
        $classes = $config['registrars'] ?? [];
        $budget = $config['budget'] ?? 0;
        if (!is_array($classes) || !array_is_list($classes)) {
            throw new InvalidArgumentException('claviron.invocations.registrars must be a list of Registrar classes.');
        }
        if (!is_int($budget) || $budget < 0) {
            throw new InvalidArgumentException('claviron.invocations.budget must be a non-negative integer in seconds.');
        }

        $registry = new self($budget);
        foreach ($classes as $class) {
            if (!is_string($class) || !is_subclass_of($class, Registrar::class)
                || !(new ReflectionClass($class))->isInstantiable()) {
                throw new InvalidArgumentException('Every invocation registrar must be a concrete '.Registrar::class.'.');
            }
        }
        foreach (array_unique($classes) as $class) {
            $app->make($class)->register($registry);
        }
        $registry->sealed = true;

        return $registry;
    }

    /** @param class-string<Handler> $class */
    public function handler(string $class): void
    {
        $this->assertOpen();
        if (!is_subclass_of($class, Handler::class) || !(new ReflectionClass($class))->isInstantiable()) {
            throw new InvalidArgumentException("[$class] must be a concrete ".Handler::class.'.');
        }
        $this->handlers[$class] = true;
    }

    /** @param class-string $class */
    public function event(string $class): void
    {
        $this->assertOpen();
        if (!class_exists($class) || (new ReflectionClass($class))->isAbstract()) {
            throw new InvalidArgumentException("[$class] must be a concrete event class.");
        }
        if (is_a($class, FleetEvent::class, true) && !is_callable([$class, 'fromPayload'])) {
            throw new InvalidArgumentException("[$class] has no public static fromPayload decoder.");
        }
        $topic = is_a($class, FleetEvent::class, true) ? $class::topic() : $class;
        if ($topic === '' || (isset($this->topics[$topic]) && $this->topics[$topic] !== $class)) {
            throw new InvalidArgumentException("Event topic [$topic] has an empty or conflicting decoder registration.");
        }
        $this->topics[$topic] = $class;
        $this->events[$class] = true;
    }

    /** @return list<class-string<Handler>> */
    public function handlers(): array { return array_keys($this->handlers); }

    /** @return list<class-string> */
    public function events(): array { return array_keys($this->events); }

    public function budgetSeconds(): int { return $this->budgetSeconds; }

    /** A frame cannot ask the container to instantiate an unregistered class. */
    public function handlerClass(string $operation): string
    {
        if (!isset($this->handlers[$operation])) {
            throw new InvalidArgumentException("Invocation operation [$operation] is not registered in this application.");
        }

        return $operation;
    }

    private function assertOpen(): void
    {
        if ($this->sealed) {
            throw new LogicException('Invocation registrations are sealed before serving.');
        }
    }
}
