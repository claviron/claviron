/* A native module: a library Claviron loads into its own process.
 *
 * A module exports two functions and calls Claviron only through the host table
 * it is handed, so it does not link against Claviron:
 *
 *     uint32_t claviron_module_abi_version(void);
 *     uint32_t claviron_module_register(const claviron_host *host, claviron_registrar *registrar);
 *
 * Register says what exists and nothing else: it opens no port, starts no thread
 * and reads no file, because it also runs for a configuration check and in a
 * master. How a module runs (instances, placement) is modules.yaml's. Its own
 * settings are its own, in files it reads in boot.
 *
 * Byte inputs live only for the call; Claviron never retains their pointers.
 * Tables are copied during registration. Contexts must stay valid until finalize
 * returns. A library is never unloaded.
 *
 * Native code runs on threads of its own, never on Claviron's I/O workers, and
 * cannot be interrupted: a call that hangs holds up drain. A C++ exception must
 * not cross this ABI. A memory fault takes the whole process with it.
 */
#ifndef CLAVIRON_MODULE_H
#define CLAVIRON_MODULE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CLAVIRON_MODULE_ABI_VERSION 1u

typedef enum {
    CLAVIRON_OK = 0,
    CLAVIRON_ERROR = 1,
    CLAVIRON_INVALID_ARGUMENT = 2,
    CLAVIRON_CLOSED = 3,
    CLAVIRON_BUSY = 4,
    CLAVIRON_UNSUPPORTED = 5,
    CLAVIRON_PANIC = 6
} claviron_status;

typedef enum {
    CLAVIRON_LOG_ERROR = 1,
    CLAVIRON_LOG_WARN = 2,
    CLAVIRON_LOG_INFO = 3,
    CLAVIRON_LOG_DEBUG = 4
} claviron_log_level;

/* Bytes borrowed for the call they are passed to or returned from. */
typedef struct {
    const uint8_t *data;
    uint64_t length;
} claviron_slice;

typedef struct claviron_registrar claviron_registrar;
typedef struct claviron_cancel claviron_cancel;

/* What this process is running with, for boot to find its own files. */
typedef struct {
    uint32_t struct_size;
    claviron_slice config_dir;
    claviron_slice environment;
} claviron_boot_info;

/* One per module, optional. boot runs once per process before anything else in
 * the module, on a thread of its own. A nonzero status stops the server's boot.
 * finalize runs once, after every service and handler has ended. */
typedef struct {
    uint32_t struct_size;
    void *context;
    uint32_t (*boot)(const claviron_boot_info *info, void *context);
    uint32_t (*reload)(void *context);
    void (*finalize)(void *context);
} claviron_lifecycle;

/* A managed background service. run has its own thread and ends when it is asked
 * to, by watching cancel. Returning nonzero leaves the service failed; nothing
 * restarts it implicitly. With dispatch pool, run may execute concurrently. */
typedef struct {
    uint32_t struct_size;
    void *context;
    uint32_t (*run)(claviron_cancel *cancel, void *context);
} claviron_service;

typedef struct claviron_host {
    uint32_t struct_size;
    uint32_t abi_version;

    /* Registration: valid only while claviron_module_register runs. */
    uint32_t (*register_lifecycle)(claviron_registrar *registrar, const claviron_lifecycle *lifecycle);
    uint32_t (*register_service)(claviron_registrar *registrar, claviron_slice name, const claviron_service *service);

    /* Any time, any thread. */
    void (*log)(uint32_t level, claviron_slice message);

    /* Cancellation of a running service. notify runs callback once, on an
     * unspecified thread, when cancelled; at once if it already is. Whichever
     * fits the module's own loop: poll cancelled, block in wait, or be told. */
    uint32_t (*cancelled)(const claviron_cancel *cancel);
    uint32_t (*cancel_wait)(const claviron_cancel *cancel);
    uint32_t (*cancel_notify)(claviron_cancel *cancel, void (*callback)(void *context), void *context);
} claviron_host;

#ifdef __cplusplus
}
#endif

#endif
