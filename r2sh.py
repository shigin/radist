import os, sys, pty
import fcntl
import socket
import select

R2SH_PORT = 55555
TIMEOUT = 400
BUFFSIZE = 4096

def r2sh(hostname, command, user=None):
    # create err socket, listen on any address
    err = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    err.bind(('', 0))
    err.listen(1)
    err.setblocking(False)
    _, err_port = err.getsockname()

    # connect to R2SH port, send err socket's port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((hostname, R2SH_PORT))
    sock.send("%hu\0" % err_port)
    sock.setblocking(False)

    poll = select.poll()
    poll.register(err, select.POLLIN)
    poll.register(sock, select.POLLIN | select.POLLHUP)
    result = poll.poll(TIMEOUT)
    if result:
        fd, event = result[0]
        if fd == sock.fileno():
            raise Exception("Server can't connect to error pipe")
        err_out, addr = err.accept()
        err.close()
    else:
        raise Exception("timeout...")

    sock.setblocking(True)
    if not user:
        user = os.getlogin()
    sock.send('%s\0%s\0' % (os.getlogin(), user))
    sock.send(command + '\0')
    answer = sock.recv(1)
    if answer != '\0':
        answer = err_out.recv(4096)
        raise Exception("Can't connect to remote server %s, reason[%d]: %s" % \
                (hostname, len(answer), answer))
    err_out.setblocking(True)
    return sock, err_out

def r2sh_popen2(hostname, command, **kwargs):
    inout, err = r2sh(hostname, command, **kwargs)
    return os.fdopen(inout.fileno(), 'r'), os.fdopen(inout.fileno(), 'w')

def xopen2(hostname, command, **kwargs):
    inout, err = r2sh(hostname, command, **kwargs)
    return FileSocket(inout, 'r'), FileSocket(inout, 'w')

def main(hostname, command, user=None, buffsize=BUFFSIZE):
    inout, error = r2sh(hostname, command, user)
    poll = select.poll()
    poll.register(inout, select.POLLIN)# | select.POLLOUT)
    poll.register(pty.STDIN_FILENO, select.POLLIN)
    poll.register(error, select.POLLIN)
    fcntl.fcntl(pty.STDIN_FILENO, fcntl.F_SETFL, os.O_NONBLOCK)
    err_fd = error.fileno()
    io_fd = inout.fileno()
    stop = 0
    while stop != 0x11:
        result = poll.poll()
        for fd, event in result:
            if fd == io_fd:
                #if event == select.POLLIN:
                x = inout.recv(buffsize)
                if len(x) != 0:
                    os.write(pty.STDOUT_FILENO, x)
                else:
                    stop |= 0x10
                #elif event == select.POLLOUT:
            if fd == pty.STDIN_FILENO:
                assert event & select.POLLIN
                x = os.read(pty.STDIN_FILENO, buffsize)
                if len(x) != 0:
                    inout.send(x)
                else:
                    # EOF
                    poll.unregister(pty.STDIN_FILENO)
                    inout.shutdown(socket.SHUT_WR)
            if fd == err_fd:
                assert event & select.POLLIN 
                x = error.recv(buffsize)
                if len(x) != 0:
                    os.write(pty.STDERR_FILENO, x)
                else:
                    # EOF
                    poll.unregister(error.fileno())
                    error.close()
                    stop |= 0x11

if __name__ == '__main__':
    import optparse
    usage = "[-l user] hostname command"
    parser = optparse.OptionParser(usage=usage)
    parser.disable_interspersed_args()
    parser.set_defaults(user=None)
    parser.add_option('-l', dest="login", 
        help="login to use")
    options, args = parser.parse_args()
    if len(args) < 2:
        parser.error("can't find hostname or command")
    hostname = args[0]
    command = ' '.join(args[1:])
    main(hostname, command, user=options.user)
