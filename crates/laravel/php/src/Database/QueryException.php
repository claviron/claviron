<?php

declare(strict_types=1);

namespace Claviron\Laravel\Database;

use PDOException;
use Throwable;

/**
 * A database failure in the shape PDO reports one.
 *
 * The native connection has no PDO handle, but everything downstream of a
 * failed query reads one as though it did: `ConcurrencyErrorDetector` decides
 * whether a transaction is worth retrying from the SQLSTATE, lost-connection
 * detection decides whether to reconnect, and packages recognise a unique
 * violation the same way. `Illuminate\Database\QueryException` copies both the
 * code and `errorInfo` from whatever it wraps, so carrying them here is what
 * keeps all of that working unchanged.
 *
 * `Exception::$code` is an int by declaration and a SQLSTATE is not — `40P01`
 * is the deadlock one. PDO assigns the string anyway from C; a subclass can do
 * the same, which is the whole reason this class exists rather than a plain
 * `PDOException`.
 */
final class QueryException extends PDOException
{
    /**
     * Pulls the SQLSTATE out of a native failure and re-reports it as PDO would.
     *
     * The runtime formats database errors as `SQLSTATE[XXXXX]: message`, which
     * is PDO's own format; anything else — a connection that was never
     * established, a statement that went unanswered — has no SQLSTATE to find
     * and is passed through with an empty one, exactly as PDO does for errors
     * that never reached the server.
     */
    public static function from(Throwable $failure): self
    {
        $message = $failure->getMessage();
        $sqlState = preg_match('/SQLSTATE\[([0-9A-Za-z]{5})]/', $message, $matches) === 1
            ? $matches[1]
            : '';

        return new self($message, $sqlState, $failure);
    }

    private function __construct(string $message, string $sqlState, Throwable $previous)
    {
        parent::__construct($message, 0, $previous);

        $this->code = $sqlState;
        $driverCode = preg_match('/SQLSTATE\[[0-9A-Za-z]{5}\]: [^:]+: (\d+) /', $message, $matches) === 1
            ? (int) $matches[1]
            : null;
        $this->errorInfo = [$sqlState, $driverCode, $message];
    }
}
