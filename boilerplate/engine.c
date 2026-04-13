/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/time.h>

#include "monitor_ioctl.h"
#pragma GCC diagnostic ignored "-Wstringop-truncation"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is full, unless we're shutting down */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    /* If shutting down, don't accept new items */
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Copy item into the next tail slot, then advance tail */
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /* Wake up the consumer - there's something to read now */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is empty, unless we're shutting down */
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    /* If shutting down AND nothing left, we're done */
    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Copy item out from head slot, then advance head */
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /* Wake up any producer that was waiting for space */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    char log_path[PATH_MAX];
    int fd;

    /* Make sure the logs directory exists */
    mkdir(LOG_DIR, 0755);

    while (1) {
        /* Block until there's a log item, or shutdown */
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break; /* shutdown signal and buffer empty */

        /* Build the log file path for this container */
        snprintf(log_path, sizeof(log_path), "%s/%s.log",
                 LOG_DIR, item.container_id);

        /* Open (or create) the log file, append mode */
        fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open log file");
            continue;
        }

        /* Write the chunk */
        if (write(fd, item.data, item.length) < 0)
	    perror("logging_thread: write");
	 close(fd);
    }

    /* Drain anything left in the buffer after shutdown */
    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        snprintf(log_path, sizeof(log_path), "%s/%s.log",
                 LOG_DIR, item.container_id);
        fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0)
            continue;
        if (write(fd, item.data, item.length) < 0)
	    perror("logging_thread: write");
        close(fd);
    }

    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    fprintf(stderr, "child_fn: started, command=%s\n", cfg->command);

    if (sethostname(cfg->id, strlen(cfg->id)) != 0) {
        perror("child_fn: sethostname");
        return 1;
    }
    fprintf(stderr, "child_fn: sethostname OK\n");

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) {
        perror("child_fn: mount private");
        return 1;
    }
    fprintf(stderr, "child_fn: mount private OK\n");

    if (chroot(cfg->rootfs) != 0) {
        perror("child_fn: chroot");
        return 1;
    }
    fprintf(stderr, "child_fn: chroot OK\n");

    if (chdir("/") != 0) {
        perror("child_fn: chdir");
        return 1;
    }
    fprintf(stderr, "child_fn: chdir OK\n");

    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("child_fn: mount /proc");
        return 1;
    }
    fprintf(stderr, "child_fn: mount proc OK\n");

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("child_fn: dup2 stdout");
        return 1;
    }
    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("child_fn: dup2 stderr");
        return 1;
    }
    close(cfg->log_write_fd);
    fprintf(stderr, "child_fn: dup2 OK\n");

    if (cfg->nice_value != 0) {
        if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) != 0)
            perror("child_fn: setpriority (non-fatal)");
    }

    /* Split command string into argv */
    char cmd_copy[CHILD_COMMAND_LEN];
    strncpy(cmd_copy, cfg->command, sizeof(cmd_copy) - 1);
    cmd_copy[sizeof(cmd_copy) - 1] = '\0';

    char *exec_args[32];
    int nargs = 0;
    char *tok = strtok(cmd_copy, " ");
    while (tok && nargs < 31) {
        exec_args[nargs++] = tok;
        tok = strtok(NULL, " ");
    }
    exec_args[nargs] = NULL;

    if (nargs == 0) {
        fprintf(stderr, "child_fn: empty command\n");
        return 1;
    }

    fprintf(stderr, "child_fn: about to execv %s with %d args\n", exec_args[0], nargs);
    fflush(stderr);
    execv(exec_args[0], exec_args);
    /* execv failed — write error manually since perror goes nowhere useful */
    char errbuf[128];
    snprintf(errbuf, sizeof(errbuf), "child_fn: execv failed: %s\n", strerror(errno));
    write(STDERR_FILENO, errbuf, strlen(errbuf));
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
/* Global supervisor context pointer for signal handlers */
static supervisor_ctx_t *g_ctx = NULL;

static void sigchld_handler(int sig)
{
    (void)sig;
    int saved_errno = errno;
    int status;
    pid_t pid;

    /* Reap all dead children, update their metadata */
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx) continue;

        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(status)) {
                    c->exit_code = WEXITSTATUS(status);
                    c->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    c->exit_signal = WTERMSIG(status);
                    /* killed by hard limit if SIGKILL and not stop_requested */
                    c->state = (c->exit_signal == SIGKILL)
                                   ? CONTAINER_KILLED
                                   : CONTAINER_STOPPED;
                }
                /* Unregister from kernel monitor */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd,
                                            c->id, c->host_pid);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
    errno = saved_errno;
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

void *pipe_reader_thread(void *arg)
{
    typedef struct { int fd; supervisor_ctx_t *ctx;
                     char id[CONTAINER_ID_LEN]; } pipe_reader_arg_t;
    pipe_reader_arg_t *parg = (pipe_reader_arg_t *)arg;

    log_item_t item;
    ssize_t n;

    while ((n = read(parg->fd, item.data, sizeof(item.data))) > 0) {
        item.length = (size_t)n;
        strncpy(item.container_id, parg->id, sizeof(item.container_id) - 1);
        bounded_buffer_push(&parg->ctx->log_buffer, &item);
    }

    close(parg->fd);
    free(parg);
    return NULL;
}

/* Spawn one container, returns host PID or -1 on error */
static pid_t spawn_container(supervisor_ctx_t *ctx,
                             const control_request_t *req)
{
    int pipe_fds[2];
    pid_t pid;

    /* Create pipe: container writes to pipe_fds[1],
       supervisor reads from pipe_fds[0] */
    if (pipe(pipe_fds) != 0) {
        perror("spawn_container: pipe");
        return -1;
    }

    /* Allocate stack for the clone'd child */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        perror("spawn_container: malloc stack");
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }

    /* Set up child config */
    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        perror("spawn_container: malloc cfg");
        free(stack);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }
    strncpy(cfg->id, req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs, req->rootfs, sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command, sizeof(cfg->command) - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipe_fds[1];

    /* Clone with new PID, UTS, and mount namespaces */
    pid = clone(child_fn,
                stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                cfg);

    /* Parent closes the write end — only child writes to it */
    close(pipe_fds[1]);

    if (pid < 0) {
        perror("spawn_container: clone");
        free(stack);
        free(cfg);
        close(pipe_fds[0]);
        return -1;
    }

    /* Start a producer thread to read from pipe and push to log buffer */
    /* We'll do this inline with a detached thread */
    typedef struct { int fd; supervisor_ctx_t *ctx;
                     char id[CONTAINER_ID_LEN]; } pipe_reader_arg_t;
    pipe_reader_arg_t *parg = malloc(sizeof(pipe_reader_arg_t));
    parg->fd = pipe_fds[0];
    parg->ctx = ctx;
    strncpy(parg->id, req->container_id, sizeof(parg->id) - 1);

    pthread_t reader_thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    /* Inline lambda via nested function isn't C99, use a static helper */
    /* We define pipe_reader_thread after this function */
    extern void *pipe_reader_thread(void *);
    pthread_create(&reader_thread, &attr, pipe_reader_thread, parg);
    pthread_attr_destroy(&attr);

    free(stack); /* stack can be freed after clone returns in parent */
    free(cfg);

    return pid;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    int client_fd;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) { errno = rc; perror("bounded_buffer_init"); return 1; }

    /* 1. Try to open kernel monitor (non-fatal if not loaded) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "Note: /dev/container_monitor not available "
                        "(kernel module not loaded)\n");

    /* 2. Create UNIX domain socket */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(ctx.server_fd, 10) < 0) {
        perror("listen"); return 1;
    }

    /* 3. Signal handlers */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 4. Start logging thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) { errno = rc; perror("pthread_create"); return 1; }

    fprintf(stderr, "Supervisor ready. base-rootfs=%s socket=%s\n",
            rootfs, CONTROL_PATH);

    /* 5. Event loop */
    while (!ctx.should_stop) {
        /* Use select with timeout so we can check should_stop */
        fd_set fds;
        struct timeval tv = {1, 0};
        FD_ZERO(&fds);
        FD_SET(ctx.server_fd, &fds);

        int ready = select(ctx.server_fd + 1, &fds, NULL, NULL, &tv);
        if (ready <= 0) continue;

        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) continue;

        control_request_t req;
        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        if (read(client_fd, &req, sizeof(req)) != sizeof(req)) {
            close(client_fd);
            continue;
        }

        /* Handle each command */
        if (req.kind == CMD_START || req.kind == CMD_RUN) {
            /* Check for duplicate ID */
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *existing = ctx.containers;
            while (existing) {
                if (strcmp(existing->id, req.container_id) == 0 &&
                    existing->state == CONTAINER_RUNNING) {
                    resp.status = -1;
                    snprintf(resp.message, sizeof(resp.message),
                             "Container '%s' already running",
                             req.container_id);
                    break;
                }
                existing = existing->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (resp.status == 0) {
                pid_t pid = spawn_container(&ctx, &req);
                if (pid < 0) {
                    resp.status = -1;
                    snprintf(resp.message, sizeof(resp.message),
                             "Failed to spawn container");
                } else {
                    /* Add metadata record */
                    container_record_t *rec = calloc(1, sizeof(*rec));
                    strncpy(rec->id, req.container_id, sizeof(rec->id) - 1);
                    rec->host_pid = pid;
                    rec->started_at = time(NULL);
                    rec->state = CONTAINER_RUNNING;
                    rec->soft_limit_bytes = req.soft_limit_bytes;
                    rec->hard_limit_bytes = req.hard_limit_bytes;
                    snprintf(rec->log_path, sizeof(rec->log_path),
                             "%s/%s.log", LOG_DIR, req.container_id);

                    pthread_mutex_lock(&ctx.metadata_lock);
                    rec->next = ctx.containers;
                    ctx.containers = rec;
                    pthread_mutex_unlock(&ctx.metadata_lock);

                    /* Register with kernel monitor */
                    if (ctx.monitor_fd >= 0)
                        register_with_monitor(ctx.monitor_fd,
                                              req.container_id, pid,
                                              req.soft_limit_bytes,
                                              req.hard_limit_bytes);

                    resp.status = 0;
                    snprintf(resp.message, sizeof(resp.message),
                             "Started container '%s' pid=%d",
                             req.container_id, pid);

                    /* For CMD_RUN: wait for container to finish */
                    if (req.kind == CMD_RUN) {
                        if (write(client_fd, &resp, sizeof(resp)) < 0)
				perror("write response");
                        int wstatus;
                        waitpid(pid, &wstatus, 0);
                        snprintf(resp.message, sizeof(resp.message),
                                 "Container '%s' exited", req.container_id);
                    }
                }
            }

        } else if (req.kind == CMD_PS) {
            pthread_mutex_lock(&ctx.metadata_lock);
            char *p = resp.message;
            int remaining = sizeof(resp.message);
            container_record_t *c = ctx.containers;
            if (!c) {
                snprintf(p, remaining, "No containers.");
            } else {
                while (c && remaining > 0) {
                    int written = snprintf(p, remaining,
                        "[%s] pid=%-6d state=%-8s soft=%luMiB hard=%luMiB\n",
                        c->id, c->host_pid,
                        state_to_string(c->state),
                        c->soft_limit_bytes >> 20,
                        c->hard_limit_bytes >> 20);
                    p += written;
                    remaining -= written;
                    c = c->next;
                }
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            resp.status = 0;

        } else if (req.kind == CMD_LOGS) {
            /* Find log path and send contents */
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *c = ctx.containers;
            while (c) {
                if (strcmp(c->id, req.container_id) == 0) {
                    snprintf(resp.message, sizeof(resp.message), "%.255s", c->log_path);
                    resp.status = 0;
                    break;
                }
                c = c->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            if (resp.status != 0) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "No container '%s' found", req.container_id);
            }

        } else if (req.kind == CMD_STOP) {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *c = ctx.containers;
            while (c) {
                if (strcmp(c->id, req.container_id) == 0 &&
                    c->state == CONTAINER_RUNNING) {
                    c->state = CONTAINER_STOPPED;
                    kill(c->host_pid, SIGTERM);
                    resp.status = 0;
                    snprintf(resp.message, sizeof(resp.message),
                             "Sent SIGTERM to '%s' pid=%d",
                             req.container_id, c->host_pid);
                    break;
                }
                c = c->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            if (resp.status != 0) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "No running container '%s'", req.container_id);
            }
        }

        if (write(client_fd, &resp, sizeof(resp)) < 0)
		perror("write response");
        close(client_fd);
    }

    /* Shutdown */
    fprintf(stderr, "Supervisor shutting down...\n");
    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free container list */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        free(c);
        c = next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    fprintf(stderr, "Supervisor exited cleanly.\n");
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor. Is it running?\n");
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != sizeof(*req)) {
        perror("write"); close(fd); return 1;
    }

    if (read(fd, &resp, sizeof(resp)) != sizeof(resp)) {
        perror("read"); close(fd); return 1;
    }

    /* For CMD_LOGS, print the actual log file contents */
    if (req->kind == CMD_LOGS && resp.status == 0) {
        FILE *f = fopen(resp.message, "r");
        if (f) {
            char buf[1024];
            while (fgets(buf, sizeof(buf), f))
                printf("%s", buf);
            fclose(f);
        } else {
            printf("(log file empty or not found: %s)\n", resp.message);
        }
    } else {
        printf("%s\n", resp.message);
    }

    close(fd);
    return (resp.status == 0) ? 0 : 1;
}


static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
