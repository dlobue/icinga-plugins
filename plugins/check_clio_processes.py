#!/usr/bin/python2.6
# Example Nagios plugin to check disk usage.
# It is kept very basic to serve as tutoral example. For the tutorial, see the
# nagiosplugin documentation.
# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from datetime import datetime
from pprint import pformat

import nagiosplugin
import pymongo

class ProcessCheck(nagiosplugin.Check):

    name = 'Process check'
    version = '0.1'

    def __init__(self, optparser, logger):
        optparser.set_usage('usage: %prog [options] <hostname of server to check> <string to search for in process cmdline>')
        optparser.description = "Monitor the number of processes running with the specified name."
        optparser.version = self.version
        optparser.add_option(
            '-s', '--server', default='localhost',
            help='clio database server to query (default: %default)')
        optparser.add_option(
            '-w', '--warning', default=None, metavar='RANGE',
            help='warning threshold (default: %default)')
        optparser.add_option(
            '-c', '--critical', default='1:', metavar='RANGE',
            help='warning threshold (default: %default)')

    def process_args(self, options, args):
        self.warning = options.warning
        self.critical = options.critical
        self.db_server = options.server
        try:
            self.server = args[0]
        except IndexError:
            print('What server am I supposed to check?!')
            import sys
            sys.exit(3)

        try:
            self.search_string = args[1]
        except IndexError:
            print('What process am I supposed to look for?!')
            import sys
            sys.exit(3)

    def obtain_data(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'system_%s' % datetime.now().strftime('%Y%m')
        field = 'data.processes'
        result = db[coll_name].find_one({'host': self.server},
                                         sort=[('ts', pymongo.DESCENDING)],
                                         fields=[field, 'ts'])

        assert (datetime.utcnow() - result['ts']).seconds < 60, "result is over a minute old!"

        processes = []
        for pid,properties in result['data']['processes'].iteritems():
            if self.search_string in properties['cmdline']:
                processes.append((
                    pid,
                    properties['name'],
                    properties['cmdline'],
                ))

        self.processes = processes
        self.found_count = len(processes)
        self.measures = [nagiosplugin.Measure(
            'processes_found', self.found_count, warning=self.warning, critical=self.critical)]

    def default_message(self):
        fields = ('pid', 'name', 'cmdline')
        processes = (pformat(zip(fields, p)) for p in self.processes)
        processes_str = '\n'.join(processes)
        return 'Found the following processes:\n%s' % processes_str


main = nagiosplugin.Controller(ProcessCheck)
if __name__ == '__main__':
   main()
