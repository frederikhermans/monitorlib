'''Monitor class implementation.'''

from sitemanagerhandle import SiteManagerHandle
import nodelist

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
