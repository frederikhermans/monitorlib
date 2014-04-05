#!/usr/bin/python
'''Flash an ihex to TelosB nodes in parallel.'''

import sys
import time

import monitorlib

def print_result(node, success):
    if success:
        print '{} programmed successfully.'.format(node)
    else:
        print '{} failed to program!'.format(node)

def main(argv):
    if len(argv) != 4:
        print >> sys.stderr, 'Usage: program_telosb.py SITEMGR NODEFILE IHEX'
        sys.exit(1)

    _, sitemgr, nodefile, ihex_file = argv

    # Connect to the site manager.
    monitor = monitorlib.Monitor(sitemgr, nodefile)
    # Sleep for a while so all nodes get a chance to come online
    time.sleep(2)

    # Select only TelosB nodes that are online.
    telosb = monitor.nodes['telosb*']
    for node in telosb:
        if not node.is_online():
            print 'Ignoring {} because it is offline.'.format(node)
            telosb.remove(node)

    # Program the nodes
    print 'Programming', telosb, '...'
    telosb.program_parallel(ihex_file, callback_=print_result)

if __name__ == '__main__':
    main(sys.argv)
