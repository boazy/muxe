#include <dlfcn.h>
#include <errno.h>
#include <libproc.h>
#include <mach/mach.h>
#include <signal.h>
#include <stdint.h>
#include <string.h>
#include <sys/proc_info.h>
#include <sys/socket.h>
#include <sys/sysctl.h>
#include <sys/proc.h>
#include <sys/un.h>
#include <unistd.h>

_Static_assert(sizeof(audit_token_t) == 8 * sizeof(uint32_t), "audit token ABI");

int muxe_process_current_token(uint32_t words[8]) {
    audit_token_t token;
    mach_msg_type_number_t count = TASK_AUDIT_TOKEN_COUNT;
    kern_return_t result = task_info(mach_task_self(), TASK_AUDIT_TOKEN, (task_info_t)&token, &count);
    if (result != KERN_SUCCESS || count != TASK_AUDIT_TOKEN_COUNT) return EPERM;
    memcpy(words, &token, sizeof(token));
    return 0;
}

/* SDK-declared types stay in C. libproc labels its interfaces private/volatile. */
int muxe_process_boot_id(char *buffer, uint32_t capacity) {
    size_t size = capacity;
    if (sysctlbyname("kern.bootsessionuuid", buffer, &size, NULL, 0) != 0)
        return errno;
    if (size == 0 || size > capacity || buffer[size - 1] != '\0')
        return EINVAL;
    return 0;
}

int muxe_process_validate_token(const uint32_t words[8]) {
    typedef int (*path_fn)(audit_token_t *, void *, uint32_t);
    path_fn path_exact = (path_fn)dlsym(RTLD_DEFAULT, "proc_pidpath_audittoken");
    if (path_exact == NULL) return ENOTSUP;
    audit_token_t token;
    memcpy(&token, words, sizeof(token));
    char path[PROC_PIDPATHINFO_MAXSIZE];
    errno = 0;
    if (path_exact(&token, path, sizeof(path)) <= 0)
        return errno ? errno : ESRCH;
    return 0;
}

/* arg=1 includes the exiting/zombie hash entry, unlike ordinary PID lookup. */
/* 1: completed cleanup; 0: still present; negative errno: query failed. */
int muxe_process_pid_completed(int pid) {
    if (pid <= 0) return -EINVAL;
    struct proc_bsdshortinfo info;
    errno = 0;
    int bytes = proc_pidinfo(pid, PROC_PIDT_SHORTBSDINFO, 1, &info, sizeof(info));
    if (bytes == sizeof(info)) return info.pbsi_status == SZOMB ? 1 : 0;
    if (bytes == 0 && errno == ESRCH) return 1;
    return -(errno ? errno : EINVAL);
}

int muxe_process_socket_peer(int fd, int pid, const uint32_t words[8]) {
    int result = muxe_process_validate_token(words);
    if (result != 0) return result;
    struct socket_fdinfo local;
    if (proc_pidfdinfo(getpid(), fd, PROC_PIDFDSOCKETINFO, &local, sizeof(local)) != sizeof(local))
        return errno ? errno : EINVAL;
    if (local.psi.soi_family != AF_UNIX || local.psi.soi_type != SOCK_STREAM ||
        local.psi.soi_so == 0 || local.psi.soi_proto.pri_un.unsi_conn_so == 0)
        return EINVAL;
    /* Bounded inspection: refuse rather than allocate or truncate a large table. */
    struct proc_fdinfo fds[4096];
    int required = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, NULL, 0);
    if (required <= 0) return errno ? errno : ESRCH;
    if ((size_t)required >= sizeof(fds)) return EOVERFLOW;
    int bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, fds, sizeof(fds));
    if (bytes <= 0 || (size_t)bytes >= sizeof(fds) || bytes % sizeof(fds[0]) != 0)
        return errno ? errno : EOVERFLOW;
    int matched = 0;
    for (int i = 0; i < bytes / (int)sizeof(fds[0]); ++i) {
        if (fds[i].proc_fdtype != PROX_FDTYPE_SOCKET) continue;
        struct socket_fdinfo peer;
        if (proc_pidfdinfo(pid, fds[i].proc_fd, PROC_PIDFDSOCKETINFO, &peer, sizeof(peer)) != sizeof(peer))
            continue;
        if (peer.psi.soi_family == AF_UNIX && peer.psi.soi_type == SOCK_STREAM &&
            peer.psi.soi_so == local.psi.soi_proto.pri_un.unsi_conn_so &&
            peer.psi.soi_proto.pri_un.unsi_conn_so == local.psi.soi_so) {
            matched = 1;
            break;
        }
    }
    result = muxe_process_validate_token(words);
    if (result != 0) return result;
    return matched ? 0 : ENOTCONN;
}

int muxe_process_signal_token(const uint32_t words[8]) {
    typedef int (*signal_fn)(audit_token_t *, int);
    /* Older systems may lack this API. Never load-fail or use kill(pid). */
    signal_fn signal_exact = (signal_fn)dlsym(RTLD_DEFAULT, "proc_signal_with_audittoken");
    if (signal_exact == NULL) return ENOTSUP;
    audit_token_t token;
    memcpy(&token, words, sizeof(token));
    /* This libproc API returns an errno value directly. */
    return signal_exact(&token, SIGKILL);
}
