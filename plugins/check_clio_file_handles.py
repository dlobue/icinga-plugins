#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo

class FileHandleCheck(nagiosplugin.Check):

    name = 'available_file_handles'
    version = '0.1'

    def __init__(self, optparser, logger):
        optparser.set_usage('usage: %prog [options] <hostname of server to check>')
        optparser.description = 'Check number of available file handles'
        optparser.version = self.version
        optparser.add_option(
            '-s', '--server', default='localhost',
            help='clio database server to query (default: %default)')
        optparser.add_option(
            '-w', '--warning', default='10:', metavar='RANGE',
            help='warning threshold (default: %default)')
        optparser.add_option(
            '-c', '--critical', default='1:', metavar='RANGE',
            help='critical threshold (default: %default)')
        self._optparser = optparser
        self._logger = logger

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

    def obtain_data(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'system_%s' % datetime.utcnow().strftime('%Y%m')
        field = 'data.fh'
        res = db[coll_name].find_one({'host': self.server},
                                         sort=[('ts', pymongo.DESCENDING)],
                                         fields=[field, 'ts'])

        assert (datetime.utcnow() - res['ts']).seconds < 60, "stale data! is arke running?"

        self.data = data = res['data']['fh']
        self.measures = [nagiosplugin.Measure(
            'available_file_handles', data['max'] - data['open'], warning=self.warning, critical=self.critical)]

    def default_message(self):
        return '%s open file handles out of a max of %s' % (self.data['open'], self.data['max'])


main = nagiosplugin.Controller(FileHandleCheck)
if __name__ == '__main__':
   main()
