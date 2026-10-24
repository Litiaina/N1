/* Test-only LD_PRELOAD fault injection. Never link this into the engine.
 *
 * cc -shared -fPIC -pthread -Wall -Wextra -Werror -o batch-faults.so \
 *    tools/tests/small-batch-syscalls.c -ldl
 *
 * An absent N1_TEST_BATCH_TRIGGER file leaves every call unchanged. Once the
 * harness creates that file AFTER engine readiness, only syncfs directory FDs
 * underneath colon-separated N1_TEST_BATCH_ROOTS are counted. One request and
 * one filesystem group must be active in this process; WIDTH is that group's
 * distinct-filesystem count, not its object count.
 *
 * Failures are injected AFTER real syncfs returns, proving fail-closed ACK
 * behavior, not simulating physical lost writes. A pause also occurs AFTER a
 * real barrier finishes, for a process-kill test, never a power-cut claim.
 * No credentials or environment values are written to the trace.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int (*real_syncfs)(int);
static pthread_once_t resolve_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t state_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned long calls;
static unsigned long completed[64];

static void resolve_syncfs(void)
{
    *(void **)(&real_syncfs) = dlsym(RTLD_NEXT, "syncfs");
}

static unsigned long number(const char *name)
{
    const char *value = getenv(name);
    char *end = NULL;
    unsigned long parsed;
    if (!value || !*value)
        return 0;
    errno = 0;
    parsed = strtoul(value, &end, 10);
    return errno || !end || *end ? 0 : parsed;
}

static int matching_root(int fd)
{
    char proc[64], target[PATH_MAX + 1];
    const char *roots = getenv("N1_TEST_BATCH_ROOTS");
    const char *start;
    ssize_t length;
    if (!roots || !*roots)
        return 0;
    if (snprintf(proc, sizeof(proc), "/proc/self/fd/%d", fd) >= (int)sizeof(proc))
        return 0;
    length = readlink(proc, target, PATH_MAX);
    if (length < 0)
        return 0;
    target[length] = '\0';
    for (start = roots; *start;) {
        const char *end = strchr(start, ':');
        size_t n = end ? (size_t)(end - start) : strlen(start);
        while (n > 1 && start[n - 1] == '/')
            --n;
        if (n && start[0] == '/' && (size_t)length >= n &&
            !strncmp(start, target, n) && (target[n] == '\0' || target[n] == '/'))
            return 1;
        if (!end)
            break;
        start = end + 1;
    }
    return 0;
}

static int write_all(int fd, const char *bytes, size_t length)
{
    while (length) {
        ssize_t wrote = write(fd, bytes, length);
        if (wrote < 0 && errno == EINTR)
            continue;
        if (wrote <= 0)
            return -1;
        bytes += wrote;
        length -= (size_t)wrote;
    }
    return 0;
}

/* Called under state_lock, so records cannot interleave between workers. */
static void trace(const char *record, size_t length)
{
    const char *path = getenv("N1_TEST_BATCH_TRACE");
    if (path && *path) {
        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (fd >= 0) {
            (void)write_all(fd, record, length);
            (void)close(fd);
        }
    }
}

static void marker(unsigned long round, unsigned long width, const char *kind)
{
    const char *base = getenv("N1_TEST_BATCH_MARKER_BASE");
    char path[PATH_MAX + 1], temporary[PATH_MAX + 1], record[256];
    int fd, count;
    if (!base || !*base)
        return;
    count = snprintf(path, sizeof(path), "%s.%sround-%lu.json", base, kind, round);
    if (count < 0 || count >= (int)sizeof(path))
        return;
    count = snprintf(temporary, sizeof(temporary), "%s.tmp-%ld", path, (long)getpid());
    if (count < 0 || count >= (int)sizeof(temporary))
        return;
    fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0)
        return;
    count = snprintf(record, sizeof(record),
        "{\"round\":%lu,\"width\":%lu,\"completed\":%lu,\"after_real_syncfs\":true}\n",
        round, width, completed[round - 1]);
    if (count > 0 && count < (int)sizeof(record) &&
        write_all(fd, record, (size_t)count) == 0) {
        /* Complete record before the harness can observe marker existence. */
        (void)link(temporary, path);
    }
    (void)close(fd);
    (void)unlink(temporary);
}

int syncfs(int fd)
{
    const char *trigger = getenv("N1_TEST_BATCH_TRIGGER");
    unsigned long width, call, round;
    struct stat info;
    int actual_result, actual_errno, result, returned_errno, pause = 0;
    char record[512];
    int length;

    (void)pthread_once(&resolve_once, resolve_syncfs);
    if (!real_syncfs) {
        errno = ENOSYS;
        return -1;
    }
    if (!trigger || access(trigger, F_OK) != 0 || !matching_root(fd))
        return real_syncfs(fd);

    width = number("N1_TEST_BATCH_WIDTH");
    if (!width || width > 32) {
        errno = EINVAL;
        return -1;
    }
    (void)pthread_mutex_lock(&state_lock);
    call = ++calls;
    round = (call - 1) / width + 1;
    (void)pthread_mutex_unlock(&state_lock);

    actual_result = real_syncfs(fd);
    actual_errno = actual_result < 0 ? errno : 0;
    result = actual_result;
    returned_errno = actual_errno;
    if (round == number("N1_TEST_BATCH_FAIL_ROUND") && actual_result == 0) {
        result = -1;
        returned_errno = EIO;
    }
    (void)pthread_mutex_lock(&state_lock);
    if (round <= sizeof(completed) / sizeof(completed[0]))
        ++completed[round - 1];
    length = snprintf(record, sizeof(record),
        "{\"event\":\"syncfs\",\"call\":%lu,\"round\":%lu,\"fd\":%d,"
        "\"device\":%llu,\"actual_result\":%d,\"actual_errno\":%d,\"returned_result\":%d,\"errno\":%d}\n",
        call, round, fd, fstat(fd, &info) == 0 ? (unsigned long long)info.st_dev : 0,
        actual_result, actual_errno, result, returned_errno);
    if (length > 0 && length < (int)sizeof(record))
        trace(record, (size_t)length);
    if (round <= sizeof(completed) / sizeof(completed[0]) && completed[round - 1] == width) {
        marker(round, width, "");
        if (round == number("N1_TEST_BATCH_PAUSE_ROUND")) {
            pause = 1;
            marker(round, width, "paused-");
        }
    }
    (void)pthread_mutex_unlock(&state_lock);

    if (pause) {
        const char *release = getenv("N1_TEST_BATCH_RELEASE");
        const struct timespec delay = { .tv_sec = 0, .tv_nsec = 10000000 };
        while (!release || access(release, F_OK) != 0)
            (void)nanosleep(&delay, NULL);
    }
    errno = returned_errno;
    return result;
}
