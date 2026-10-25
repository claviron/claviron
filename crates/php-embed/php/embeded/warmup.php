<?php

declare(strict_types=1);

return (static function (): ?int {
    if (!function_exists('opcache_compile_file') || opcache_get_status(false) === false) {
        return null;
    }

    $folders = json_decode(getenv('CLAVIRON_PHP_WARMUP'), true, flags: JSON_THROW_ON_ERROR);
    $visited = [];
    $cached = 0;
    foreach ($folders as $folder) {
        if (claviron_is_cancelled()) {
            throw new RuntimeException('PHP OPcache warmup cancelled');
        }
        $root = realpath($folder);
        if ($root === false || !is_dir($root)) {
            throw new RuntimeException("PHP OPcache warmup folder is unavailable: {$folder}");
        }
        if (in_array(strtolower(basename($root)), ['test', 'tests'], true)) {
            continue;
        }
        // Do not follow nested directory links: roots may overlap, but a link
        // must not introduce cycles or silently expand the configured tree.
        $files = new RecursiveIteratorIterator(
            new RecursiveCallbackFilterIterator(
                new RecursiveDirectoryIterator($root, FilesystemIterator::SKIP_DOTS),
                static fn (SplFileInfo $entry): bool => !$entry->isLink()
                    && !($entry->isDir() && in_array(strtolower($entry->getFilename()), ['test', 'tests'], true)),
            ),
        );
        foreach ($files as $file) {
            if (claviron_is_cancelled()) {
                throw new RuntimeException('PHP OPcache warmup cancelled');
            }
            if ($file->isLink() || !$file->isFile() || strtolower($file->getExtension()) !== 'php') {
                continue;
            }
            $path = $file->getRealPath();
            if ($path === false) {
                throw new RuntimeException('PHP OPcache warmup file disappeared: '.$file->getPathname());
            }
            if (isset($visited[$path])) {
                continue;
            }
            $visited[$path] = true;
            try {
                // Recompiling an already cached file can register its unconditional
                // function declarations again in the current PHP request scope.
                if (!opcache_is_script_cached($path) && !opcache_compile_file($path)) {
                    throw new RuntimeException('compilation failed');
                }
                if (!opcache_is_script_cached($path)) {
                    throw new RuntimeException('OPcache did not retain the file');
                }
                ++$cached;
            } catch (Throwable $error) {
                error_log("PHP OPcache warmup skipped {$path}: ".$error->getMessage());
            }
        }
    }

    return $cached;
})();
