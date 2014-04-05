'''Loading and handling lists of nodes.'''

from fnmatch import fnmatch
import multiprocessing.pool
import re

import nodes as monitorlib_nodes

_NODE_FILE_LINE = re.compile(r'(?P<gid>[^\s]+)\s+' +
                             r'(?P<type>[^\s]+)\s+' +
                             r'(?P<tos_id>[^\s]+)\s+' +
                             '(?P<rime_addr>[^ ]+.)')

def _attr_match(obj, attr, val):
    '''Check if `obj` has attribute `attr` and it has value `val`.
    
       If `val` is of type string, a wildcard match against the
       string representation of the attribute will be performed.
    '''

    if hasattr(obj, attr):
        if isinstance(val, str) or isinstance(val, unicode):
            if fnmatch(str(getattr(obj, attr)), val):
                return True
    return False

def parse_node_file(filename, monitor):
    '''Return a list of nodes in `filename`.'''
    nodes = list()

    with open(filename) as f:
        for linenum, line in enumerate(f):
            # Strip comments.
            line, _, _ = line.partition('#')

            if line.strip() == '':
                continue

            match = _NODE_FILE_LINE.match(line)
            if not match:
                raise Exception('Failed to parse line {}'.format(linenum+1))

            groups = match.groupdict()
            # Get a reference to the class of the specified type.
            class_ = getattr(monitorlib_nodes, groups['type'])
            # Create an instance and put it in the list
            nodes.append(class_(monitor, groups))

    return NodeList(nodes)

class NodeList(object):
    '''A convenience class for select and operating on a list of nodes.'''

    def __init__(self, nodes):
        self.nodes = list(nodes)
        self.nodes.sort(key=str)

    def select(self, **kwargs):
        '''Return a list of nodes all having certain attributes.
        
           Attributes and values are passed in the keyword dict.
           See `_attr_match` for how wildcards are used in this
           function.'''

        res = list()
        for node in self.nodes:
            all_match = True
            for key, value in kwargs.iteritems():
                if not _attr_match(node, key, value):
                    all_match = False
            if all_match:
                res.append(node)
        return NodeList(res)

    def _sequential(self, name):
        '''Returns a function that runs `name` on all nodes sequentially.'''
        def run_sequential(*args, **kwargs):
            res = dict()
            for node in self.nodes:
                res[node] = getattr(node, name)(*args, **kwargs)
            return res
        return run_sequential

    def _parallel(self, name):
        '''Returns a function that runs `name` on all nodes in parallel.

           The returned function blocks until all results have been received.'''
        def run_parallel(*args, **kwargs):
            async_res = dict()
            pool = multiprocessing.pool.ThreadPool(len(self.nodes))
            for node in self.nodes:
                res = pool.apply_async(getattr(node, name), args, kwargs)
                async_res[node] = res
            return {n: r.get() for n, r in async_res.iteritems()}
        return run_parallel

    def __getattr__(self, name):
        try:
            return getattr(super(NodeList, self), name)
        except AttributeError:
            pass

        if name.endswith('_parallel'):
            parallel = True
            name = name[:-len('_parallel')]
        else:
            parallel = False

        if name in dir(self) and callable(getattr(self.nodes[0], name)):
            # name is implemented and callable.
            if parallel:
                return self._parallel(name)
            else:
                return self._sequential(name)

        raise AttributeError("No attribute '{}'".format(name))

    def __dir__(self):
        # List of set of attributes in nodes
        nodeattrs = [set(dir(n)) for n in self.nodes]
        
        # Calculate the intersection of all these sets to determine
        # the attributes that are present in all sets.
        nodedir = nodeattrs[0].intersection(*nodeattrs[1:])
        nodedir = [a for a in nodedir if not a.startswith('__')]

        return nodedir

    def __getitem__(self, index):
        if isinstance(index, str) or isinstance(index, unicode):
            return NodeList(n for n in self.nodes if fnmatch(str(n), index))
        else:
            return self.nodes[index]

    def __getslice__(self, start, stop, step=1):
        '''Return NodeList that contains a slice of this NodeList.'''
        return NodeList(self.nodes[start:stop:step])

    def __len__(self):
        '''Return the number of nodes in this list.'''
        return len(self.nodes)

    def __add__(self, other):
        '''Return NodeList that contains nodes in this object and in `other`'''
        return NodeList(set(self.nodes + other.nodes))

    def __sub__(self, other):
        '''Return NodeList that does not contain any nodes in `other`.'''
        return NodeList(set(self.nodes) - set(other.nodes))

    def __repr__(self):
        return 'NodeList({})'.format(self.nodes)
