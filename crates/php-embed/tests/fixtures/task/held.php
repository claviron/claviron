<?php if ($argc !== 1 || $argv !== [__FILE__] || $_SERVER['argv'] !== $argv) { throw new Exception('empty args mismatch'); } while (!file_exists(getenv('OUTPUT_DIR') . '/release')) { usleep(1000); }
