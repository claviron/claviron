<?php
foreach (['CLAVIRON_HOSTNAME', 'CLAVIRON_RUNNER', 'CLAVIRON_RUN_MODE', 'CLAVIRON_WORKER_ID', 'CLAVIRON_WORKER_COUNT', 'CLAVIRON_PROCESS_ID', 'CLAVIRON_THREAD_ID'] as $key) {
    if (!isset($_SERVER[$key]) || getenv($key) !== $_SERVER[$key]) { throw new Exception('identity mismatch: ' . $key); }
}
if (getenv('CLAVIRON_RUN_MODE') !== 'standalone' || (int)getenv('CLAVIRON_PROCESS_ID') !== getmypid()) { throw new Exception('invalid process identity'); }
if (getenv('CLAVIRON_WORKER_NUMBER') !== false || isset($_SERVER['CLAVIRON_WORKER_NUMBER'])) { throw new Exception('standalone has worker number'); }
if (getenv('CLAVIRON_TASK_NAME') !== false || isset($_SERVER['CLAVIRON_TASK_NAME'])) { throw new Exception('PHP inherited task identity'); }
