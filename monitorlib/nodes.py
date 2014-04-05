'''Classes that represent the different node types.'''

from datetime import datetime
import Queue
import os
import re
import socket
import subprocess
import sys
import time

_RE_PING = r'(?P<at>\d+) (?P<ip>[\d+\.]+):(?P<port>\d+) ' + \
           r'(?P<type>[^ ]+) {} (?P<id>[^ ]+ )?{{(?P<attributes>[^}}]+)}}.*'
_TAIL_DATE_FMT = '%y-%m-%d %H:%M:%S'
_NODE_TIMEOUT = 4.0
_MAX_LOG_EVENTS = 100

class Node(object):
    '''Base class for nodes.'''
    ip = None
    port = None
    host = None
    last_seen = 0
    gid = None
    app_id = None

    log_prefix = None
    log_events = None
    monitor = None

    def __init__(self, monitor, info):
        self.monitor = monitor
        self.gid = info['gid']
        self.log_events = list()

        # Register callback for pings from this node.
        re_ping = re.compile(_RE_PING.format(info['gid']))
        monitor.add_listener(re_ping, self._ping_listener)

    def _ping_listener(self, _, match):
        '''Handle a ping from the node.'''
        groups = match.groupdict()
        if not self.last_seen:
            self._first_ping(groups)
        if 'id' in groups:
            self.app_id = groups['id']
        self.last_seen = time.time()

    def _first_ping(self, groups):
        '''Handle the first ping we receive for this node.'''
        self.ip = groups['ip']
        self.port = groups['port']
        ip = self.ip.replace('.', '\\.')

        # XXX If the IP cannot be resolved to a host name, this
        # operation may take quite a long time during which we
        # block the sitemgr thread, since this function is called
        # from a callback.
        try:
            self.host = socket.gethostbyaddr(self.ip)[0]
        except socket.herror:
            self.host = self.ip

        # Set prefix for all messages from this node.
        self.log_prefix = r'(?P<at>\d+) {}:{} '.format(ip, self.port)

        # Register callback for log events (needed for tail())
        re_le_all = re.compile(self.log_prefix + r'LE_ALL (?P<logline>.*)')
        self.monitor.add_listener(re_le_all, self._le_all_listener)

    def tail(self, n=10, raw=False, do_print=True):
        '''Return recently received log events.
        
           If `do_print` is True (default), the result will be printed.
        '''
        events = list(self.log_events[-n:])

        if raw:
            # Just return the lines without formatting
            lines = [line for line, _ in events]
        else:
            # Make the lines look pretty.
            lines = list()
            for _, match in events:
                groups = match.groupdict()
                ts = float(groups['at'])/1000.
                timestr = datetime.fromtimestamp(ts).strftime(_TAIL_DATE_FMT)
                line = '[{} {:>25}] {}'.format(timestr, self, groups['logline'])
                lines.append(line)
            if not lines:
                lines.append('[{:>25}] <<NO OUTPUT>>'.format(self))
                
        if do_print:
            print '\n'.join(lines)
        return lines

    def _le_all_listener(self, line, match):
        '''Callback for any log event (LE_ALL) from the node.'''
        self.log_events.append((line, match))
        if len(self.log_events) > _MAX_LOG_EVENTS:
            self.log_events.pop(0)

    def is_online(self):
        '''Return whether a ping was received from the node recently.'''
        return time.time() - self.last_seen < _NODE_TIMEOUT

    def block_until_online(self):
        '''Blocks until the node is online.'''
        while not self.is_online():
            time.sleep(1.0)

    def is_app_running(self):
        '''Return whether the app is running.'''
        return self.app_id != None

    def _send_command(self, cmd, period=0):
        '''Send a command to the vclient instance.'''
        self.monitor.sitemgr.send_command(self, cmd + '\n', period=period)

    def set_position(self, pos):
        '''Set the position of the node.'''
        self._send_command('CTRL_POSITION_UPDATE {}'.format(pos))

    def start_app(self, args):
        '''Start the app on the node.'''
        argsstr = ' '.join('args=' + a for a in args)
        self._send_command('CTRL_NET_UP_REQ ' + argsstr)

    def stop_app(self):
        '''Stop the app on the node.'''
        self._send_command('CTRL_NET_DOWN_REQ')

    def send_to_app(self, msg, period=0):
        '''Send a string to the stdin of the app.'''
        self._send_command('CTRL_SEND_TO_APP {}'.format(msg), period=period)

    def expect(self, pattern):
        '''Return a function that blocks until a matching event is received.
        
           The returned function blocks until a log event that matches the
           regex `pattern` is received from the node. The returned function
           takes an optional `timeout` argument. You may not call the returned
           function more than once.
        '''
        queue = Queue.Queue(maxsize=1)
        regex = re.compile(self.log_prefix + '.*' +  pattern + '.*')
        def callback(line, match):
            '''Called when a line matching `pattern` is received.'''
            try:
                queue.put((line, match), block=False)
            except Queue.Full:
                pass

        def block(timeout=365*24*60*60):
            '''Blocks until `callback` is called.'''
            try:
                res = queue.get(True, timeout)
            except Queue.Empty:
                res = None
            except KeyboardInterrupt:
                # Clean up and raise the exception
                self.monitor.remove_listener(regex, callback)
                raise

            self.monitor.remove_listener(regex, callback)
            return res

        self.monitor.add_listener(regex, callback)
        return block

    def send_to_app_and_expect(self, msg, pattern, timeout=1.0, tries=1):
        '''Send `msg` to the app and block until `pattern` is received.'''
        for _ in xrange(tries):
            block = self.expect(pattern)
            self.send_to_app(msg)
            res = block(timeout=timeout)
            if res:
                return True
        return False 

    def send_sequence_to_app(self, seq, timeout=3.0, tries=4):
        '''Send a sequence of commands to the app.

           `seq` must be a sequence of `(cmd, pattern)` tuples, where `cmd`
           is a string describing the command to send, and `pattern` is a
           pattern that is expected to be received after the command.
        '''
        for cmd, pattern in seq:
            if not self.send_to_app_and_expect(cmd, pattern, timeout, tries):
                return False
        return True

    def __repr__(self):
        return '{}@{}'.format(self.gid, self.host)

class TelosB(Node):
    '''A TelosB sensor node (also known as Tmote Sky).'''
    def __init__(self, monitor, info):
        self.rime = info['rime_addr']
        super(TelosB, self).__init__(monitor, info)

    def open_serial(self):
        '''Open the serial port.'''
        # Call twice to work around site manager issues.
        self.start_app(['-b115200', './tty'])
        self.start_app(['-b115200', './tty'])

    def close_serial(self, block=False):
        '''Close the serial port.'''
        self.stop_app()
        if block:
            while self.is_serial_open():
                time.sleep(1.0)

    def is_serial_open(self):
        '''Return if the serial port has been opened.'''
        return self.is_app_running()

    def block_until_serial_open(self):
        '''Block until the serial port has been opened.'''
        while not self.is_serial_open():
            time.sleep(1.0)

    def write_serial(self, msg):
        '''Write a message to the serial port.'''
        self.send_to_app(msg)

    def program(self, ihex_file, quiet=True):
        '''Flash `ihex_file` to the node.'''
        if quiet:
            out = open(os.devnull, 'w')
        else:
            # Set out to a clone of stdout. We can close this
            # cloned file object without closing stdout.
            out = os.fdopen(os.dup(sys.stdout.fileno()), 'w')

        # Need to close serial port to flash.
        serial_was_open = self.is_serial_open()
        if serial_was_open:
            self.close_serial(block=True)

        # Copy the image file
        user_host = 'root@{}'.format(self.host)
        node_dir = '/var/wisenet/nodes/{}/'.format(self.gid)
        res = subprocess.call(['scp', ihex_file,
                               '{}:{}/imgs'.format(user_host, node_dir)],
                               stdout=out)
        if res != 0:
            print 'Failed to copy image to {}.'.format(self)
            out.close()
            return False

        # Program the image
        ihex_file_base = os.path.basename(ihex_file)
        cmd = 'cd {} && ./program imgs/{}'.format(node_dir, ihex_file_base)
        res = subprocess.call(['ssh', user_host, cmd], stdout=out)
        if res != 0:
            print 'Failed to program image on node {}.'.format(self)
            out.close()
            return False

        # XXX Delete the image?

        if serial_was_open:
            self.open_serial()
        out.close()
        return True

    def bsl_reset(self):
        '''Perform a bootstrap loader reset.'''
        user_host = 'root@{}'.format(self.host)
        cmd = 'cd /var/wisenet/nodes/{}/ && ./reset'.format(self.gid)
        res = subprocess.call(['ssh', user_host, cmd])
        return res == 0
