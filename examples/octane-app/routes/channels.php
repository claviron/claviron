<?php

use Illuminate\Support\Facades\Broadcast;

// Deliberately open for the local Echo integration demo.
Broadcast::channel('demo', static fn (): bool => true);
