'''Monitor class implementation.'''

from datetime import datetime
import re

from monitorlib.sitemanagerhandle import SiteManagerHandle
import monitorlib.nodelist as nodelist

_LOG_DATE_FMT = '%Y-%m-%d %H:%M:%S'

class Monitor(object):
    '''The Monitor class represents a handle to the testbed.'''
    sitemgr = None
    nodes = None
    listeners = None

    def __init__(self, host, nodefile, port_up=5000, port_down=5051):
        self.listeners = list()
        self.nodes = nodelist.parse_node_file(nodefile, self)
        self.sitemgr = SiteManagerHandle(host, self._notify_listeners, \
                                         port_up=port_up, port_down=port_down)
        self.sitemgr.connect()

    def _notify_listeners(self, line):
        '''Callback for log events from the site manager handle.'''
        for (regex, callback) in self.listeners:
            match = regex.match(line)
            if match:
                callback(line, match)

    def shutdown(self):
        '''Disconnect from the site manager.'''
        self.sitemgr.disconnect()

    def add_listener(self, regex, callback):
        '''Call `callback`, whenever a line matching `regex` is received.'''
        self.listeners.append((regex, callback))

    def remove_listener(self, regex, callback):
        '''Remove a listener.'''
        self.listeners.remove((regex, callback))

    def log_to_file(self, filename, mode='w'):
        '''Write all incoming log events to `filename`'''
        assert mode == 'a' or mode == 'w', 'Mode must be _a_ppend or _w_rite.'

        def callback(_, match):
            '''Write one log event to `filename`'''
            groups = match.groupdict()
            now = datetime.now().strftime(_LOG_DATE_FMT)

            ip = groups['ip']
            port = groups['port']

            # Look up the node's rime and gid
            gid = '??'
            rime = '??'
            node = self.nodes.select(ip=ip, port=port)
            if len(node) == 1:
                gid = node[0].gid
                if hasattr(node[0], 'rime'):
                    rime = node[0].rime

            # Write the line
            f.write('{} {} {} {}\n'.format(now, gid, rime, groups['logline']))
            f.flush()

        #  Open the file, register the callback
        f = open(filename, mode)
        re_le_all = re.compile(r'(?P<at>\d+) (?P<ip>[^:]+):(?P<port>[^ ]+) ' +
                               r'LE_ALL (?P<logline>.*)')
        self.add_listener(re_le_all, callback)
