'''Plumbing for site manager interaction.'''

import socket
import sys
import threading

def _connect_tcp_socket(ip, port):
    '''Connect a TCP socket to the given IP/port.'''
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    return sock

class SiteManagerHandle(object):
    '''A class to read data from the site manager and set commands to it.'''
    ip = None
    port_up = None
    port_down = None

    callback = None

    running = False
    socket_down = None

    def __init__(self, ip, callback, port_down=5000, port_up=5051):
        self.ip = ip
        self.callback = callback
        self.port_up = port_up
        self.port_down = port_down
   
    def _reader(self):
        '''Read data from the down socket and pass it to the callback.'''
        buf = ''
        self.running = True
        while self.running:
            # Read until we get a newline
            while buf.find('\n') < 0:
                res = self.socket_down.recv(1024)
                if res == '':
                    break
                buf += res
            
            # Call the callback on the line.
            if res != '':
                line, buf = buf.split('\n', 1)
                self.callback(line)

    def connect(self):
        '''Connect to the site manager, pass received data to the callback.'''
        self.socket_down = _connect_tcp_socket(self.ip, self.port_down)
        thread = threading.Thread(target=self._reader)
        thread.daemon = True
        thread.start()

    def disconnect(self):
        '''Disconnect from the site manager.'''
        self.running = False
        if self.socket_down:
            self.socket_down.shutdown(socket.SHUT_RDWR)
            self.socket_down.close()
            self.socket_down = None

    def send_command(self, node, cmd, period=0):
        '''Send the command `cmd` to `node` via the site manager.'''
        if len(cmd) == 0 or cmd[-1] != '\n' or cmd.count('\n') != 1:
            print >> sys.stderr, 'send_command(): A command must contain', \
                  'exactly one trailing newline character.'
            return

        socket_up = _connect_tcp_socket(self.ip, self.port_up)
        line = '{} {} {} {}'.format(period, node.ip, node.port, cmd)
        socket_up.send(line)
        socket_up.close()
