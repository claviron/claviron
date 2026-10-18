<?php

declare(strict_types=1);

// Isolated console contract; no Laravel providers, database or deployment config.
return new class {
    public function make(string $contract): object
    {
        if ($contract !== \Illuminate\Contracts\Console\Kernel::class) {
            throw new LogicException('Unexpected console contract');
        }

        return new class {
            public function handle($input, $output): int
            {
                $output->writeln(json_encode([
                    'arguments' => array_slice($_SERVER['argv'], 1),
                    'command' => $input->getFirstArgument(),
                    'console' => getenv('APP_RUNNING_IN_CONSOLE'),
                    'http' => function_exists('claviron_runtime_available') && claviron_runtime_available() ? claviron_is_on_http() : false,
                'events' => function_exists('claviron_events_available') && claviron_events_available(),
                'prepared' => function_exists('claviron_runtime_configured') && claviron_runtime_configured(),
                    'stdin' => trim(fgets(STDIN)),
                ], JSON_THROW_ON_ERROR));
                $output->getErrorOutput()->writeln('example stderr');

                if ($input->getFirstArgument() === 'example:cancel') {
                    while (! claviron_is_cancelled()) {}

                    return 0;
                }

                if ($input->getFirstArgument() === 'example:fatal') {
                    throw new RuntimeException('example fatal error');
                }

                return $input->getFirstArgument() === 'example:zero' ? 0 : 7;
            }

            public function terminate($input, int $status): void
            {
                echo 'terminated:'.$status.PHP_EOL;
            }
        };
    }
};
