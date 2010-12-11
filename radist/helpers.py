import os, sys
import socket
import pty
import pwd
import subprocess
import exceptions

__all__ = ['r_exec', 'r_popen2', 'r_popen3']

PATH = os.environ['PATH'].split(os.path.pathsep) + ['/usr/local/radist/bin/']

def find_in_path(file, search_path=PATH):
    for path in search_path:
        c_file = os.path.join(path, file)
        if os.path.exists(c_file):
            return c_file
    return None

__RUNNERS = ('PYRADIST_RUNNER', 'r2sh', 'rsh', 'ssh')
__LRUNNERS = ('PYRADIST_LRUNNER', 'bash', 'sh')

def get_runner(r_list):
    if r_list[0] in os.environ:
        return os.environ[r_list[0]]
    for runner in r_list[1:]:
        xrunner = find_in_path(runner)
        if xrunner is not None:
            return xrunner

    raise exceptions.ImportError("can't find any RUNNER")

def is_local(host):
    return socket.gethostname() == host

def is_current_user(user):
    if user is None:
        return True
    try:
        pw = pwd.getpwnam(user.lstrip('-'))
        if pw.pw_uid == os.getuid() and pw.pw_gid == os.getgid():
            return True
        return False
    except KeyError:
        # where is no user with name
        # XXX it's better to inform user
        return True

def get_user(user=None):
    if user is None:
        return os.environ.get('PYRADIST_USER', None)
    else:
        return user

RUNNER = get_runner(__RUNNERS)
LRUNNER = get_runner(__LRUNNERS)
SUDO = find_in_path('sudo')
R_SUB2 = 32 # who matters
R_SUB3 = 64

assert os.P_WAIT not in (R_SUB2, R_SUB3)
assert os.P_NOWAIT not in (R_SUB2, R_SUB3)

def r_exec(host, cmd, flags=os.P_WAIT, stdin=None, stdout=None, stderr=None, user=None):
    """Executes cmd on host using runner.

    If flags equals to os.P_WAIT, routine returns exit code of runner.

    If flags equals to os.P_NOWAIT, routine returns pid of runner.

    If flags equals to radist.R_SUB2, routine returns tuple
    (pid, stdin, stdout). If stdin or stdout is file do not include descriptor
    to tuple.

    If flags equals to radist.R_SUB3, routine returns tuple
    (pid, stdin, stdout, stderr) like radist.R_SUB2."""
    # subprocess.Popen checks if stdin, stdout and stderr is file,
    # but this behavour wasn't described, that's why i fork myself...
    stds = {pty.STDIN_FILENO: stdin, pty.STDOUT_FILENO: stdout, pty.STDERR_FILENO: stderr}
    xstds = {}
    user = get_user(user)
    for std in stds.values():
        assert isinstance(std, (type(None), file)), str(type(std))
    if flags in (R_SUB2, R_SUB3):
        if stdin is None:
            r_end, w_end = os.pipe()
            xstds[pty.STDIN_FILENO] = w_end, r_end
        if stdout is None:
            xstds[pty.STDOUT_FILENO] = os.pipe()
    if flags == R_SUB3 and stderr is None:
        xstds[pty.STDERR_FILENO] = os.pipe()
    pid = os.fork()
    if pid == 0:
        for fd in stds:
            if stds[fd] is not None:
                os.dup2(stds[fd].fileno(), fd)
        for fd in xstds:
            pout, pin = xstds[fd]
            os.close(pout)
            os.dup2(pin, fd)
        if is_local(host):
            if not is_current_user(user):
                return os.execl(SUDO, 'sudo', '-u', user.lstrip('-'), LRUNNER, '-c', cmd)
            else:
                return os.execl(LRUNNER, LRUNNER, "-c", cmd)
        else:
            if user:
                if user.startswith('-'):
                    return os.execl(RUNNER, RUNNER, host,
                        'sudo', '-u', user.lstrip('-'),
                        'sh', '-c', '"%s"' % cmd.replace('"', r'\"'))
                elif '@' in user:
                    remote, sudo = user.split('@', 1)
                    return os.execl(RUNNER, RUNNER, '-l', remote, host,
                        'sudo', '-u', sudo,
                        'sh', '-c', '"%s"' % cmd.replace('"', r'\"'))
                else:
                    return os.execl(RUNNER, RUNNER, '-l', user, host, cmd)
            else:
                return os.execl(RUNNER, RUNNER, host, cmd)
    elif pid == -1:
        # XXX change Exception to something other
        raise Exception("Can't fork")
    else:
        if flags == os.P_NOWAIT:
            return pid
        elif flags == os.P_WAIT:
            pid, status = os.waitpid(pid, 0)
            if os.WIFSIGNALED(status):
                return -os.WSTOPSIG(status)
            elif os.WIFEXITED(status):
                return os.WEXITSTATUS(status)
            else:
                # XXX change Exception to something other
                raise Exception("Unknown return status %d" % status)
        elif flags in (R_SUB2, R_SUB3):
            result = [pid]
            for fd in xstds:
                pout, pin = xstds[fd]
                if fd == pty.STDIN_FILENO:
                    mode = 'w'
                else:
                    mode = 'r'
                os.close(pin)
                result.append(os.fdopen(pout, mode))
            return result

def r_popen3(host, cmd, **kwargs):
    """Returns popen3 on remote host."""
    return r_exec(host, cmd, flags=R_SUB3, **kwargs)[1:]

def r_popen2(host, cmd, **kwargs):
    """Returns popen2 on remote host."""
    return r_exec(host, cmd, flags=R_SUB2, **kwargs)[1:]
