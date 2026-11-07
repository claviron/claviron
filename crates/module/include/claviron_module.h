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

/* One exchange. Valid from its request callback until its closed callback has
 * returned; nothing may touch it afterwards. It may be answered later and from
 * another thread: returning from request does not end it. */
typedef struct claviron_request claviron_request;

typedef uint32_t (*claviron_request_fn)(claviron_request *request, void *context);
/* Bytes borrowed for the call. end is 1 on the last call, with or without bytes. */
typedef uint32_t (*claviron_body_fn)(claviron_request *request, const uint8_t *data, uint64_t length, uint32_t end, void *context);
/* Exactly once for every request that reached request. status is CLAVIRON_OK when
 * the reply was delivered, nonzero otherwise. Free request_data here. */
typedef void (*claviron_closed_fn)(claviron_request *request, uint32_t status, void *context);

/* request is required. writable says output room has returned after a write that
 * answered CLAVIRON_BUSY. Handlers run at most `instances` at a time and share the
 * module's globals, so with instances above one they must be thread-safe.
 * A request returning nonzero before a reply began is answered 500. */
typedef struct {
    uint32_t struct_size;
    claviron_request_fn request;
    claviron_body_fn body;
    claviron_request_fn writable;
    claviron_closed_fn closed;
} claviron_http_handler;

typedef enum {
    CLAVIRON_MESSAGE_TEXT = 1,
    CLAVIRON_MESSAGE_BINARY = 2,
    CLAVIRON_MESSAGE_CLOSE = 3
} claviron_message_kind;

/* Bytes borrowed for the call; for a close, code is set and data is the reason.
 * A close with code 1001 that the client did not send says the application is
 * stopping: finish what is owed and close. The next message arrives after this
 * call returns, so a slow handler slows its client, not memory. */
typedef uint32_t (*claviron_message_fn)(claviron_request *request, uint32_t kind, uint32_t code, const uint8_t *data, uint64_t length, void *context);

/* An HTTP/1.1 upgrade or an HTTP/2 extended CONNECT to this application. open
 * decides: session_accept, or response_begin and response_finish to refuse. It may
 * decide later, from another thread. What stands in front of the application has
 * already applied. */
typedef struct {
    uint32_t struct_size;
    claviron_request_fn open;
    claviron_message_fn message;
    claviron_request_fn writable;
    claviron_closed_fn closed;
} claviron_session_handler;

/* start runs before the application takes requests and stop after the last one
 * has closed, at boot and shutdown and for every management start and stop. */
typedef struct {
    uint32_t struct_size;
    void *context;
    uint32_t (*start)(void *context);
    uint32_t (*stop)(void *context);
    const claviron_http_handler *http;
    /* Optional. Without it a session attempt is answered 501. */
    const claviron_session_handler *websocket;
} claviron_application;

typedef enum {
    CLAVIRON_FIELD_METHOD = 1,
    CLAVIRON_FIELD_SCHEME = 2,
    CLAVIRON_FIELD_PATH = 3,
    CLAVIRON_FIELD_QUERY = 4,
    CLAVIRON_FIELD_PEER = 5,
    CLAVIRON_FIELD_LOCAL = 6,
    CLAVIRON_FIELD_VERSION = 7,
    CLAVIRON_FIELD_HEADER_NAME = 8,
    CLAVIRON_FIELD_HEADER_VALUE = 9
} claviron_request_field;

/* The policy Claviron applies to this request. UINT64_MAX is off. */
typedef struct {
    uint32_t struct_size;
    uint64_t max_request_body;
    uint64_t max_response_body;
} claviron_request_policy;

typedef struct claviron_host {
    uint32_t struct_size;
    uint32_t abi_version;

    /* Registration: valid only while claviron_module_register runs. */
    uint32_t (*register_lifecycle)(claviron_registrar *registrar, const claviron_lifecycle *lifecycle);
    uint32_t (*register_service)(claviron_registrar *registrar, claviron_slice name, const claviron_service *service);
    uint32_t (*register_application)(claviron_registrar *registrar, claviron_slice name, const claviron_application *application);

    /* Any time, any thread. */
    void (*log)(uint32_t level, claviron_slice message);

    /* Cancellation of a running service. notify runs callback once, on an
     * unspecified thread, when cancelled; at once if it already is. Whichever
     * fits the module's own loop: poll cancelled, block in wait, or be told. */
    uint32_t (*cancelled)(const claviron_cancel *cancel);
    uint32_t (*cancel_wait)(const claviron_cancel *cancel);
    uint32_t (*cancel_notify)(claviron_cancel *cancel, void (*callback)(void *context), void *context);

    /* Request head. What comes back is borrowed until closed returns, and nothing
     * is copied. index selects a header, from 0 below request_header_count. */
    uint32_t (*request_field)(const claviron_request *request, uint32_t field, uint64_t index, claviron_slice *out);
    uint64_t (*request_header_count)(const claviron_request *request);
    uint32_t (*request_policy)(const claviron_request *request, claviron_request_policy *out);
    uint32_t (*request_set_data)(claviron_request *request, void *data);
    void *(*request_data)(const claviron_request *request);
    /* Ask for the body through the body callback, one piece at a time: the next
     * arrives after the last callback returns. An unread body is discarded. */
    uint32_t (*request_read_body)(claviron_request *request);

    /* The reply. Its head goes out with the first write or with finish. write
     * copies the bytes before returning and answers CLAVIRON_BUSY while the last
     * piece is unsent; writable then says when to try again. One write followed
     * by finish needs no waiting, whatever its size. */
    uint32_t (*response_begin)(claviron_request *request, uint32_t status);
    uint32_t (*response_header)(claviron_request *request, claviron_slice name, claviron_slice value);
    uint32_t (*response_write)(claviron_request *request, claviron_slice data);
    uint32_t (*response_finish)(claviron_request *request);

    /* Accept a session, with any headers set by response_header. max_message_bytes
     * bounds a text or binary message either way, and is required. session_send
     * copies its bytes and answers CLAVIRON_BUSY while the last is unsent; writable
     * says when to try again. A close ends the session. */
    uint32_t (*session_accept)(claviron_request *request, uint64_t max_message_bytes);
    uint32_t (*session_send)(claviron_request *request, uint32_t kind, uint32_t code, claviron_slice data);
} claviron_host;

#ifdef __cplusplus
}
#endif

#endif
