#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo

class AllDiskSpaceCheck(nagiosplugin.Check):

    name = 'disk space'
    version = '0.1'

    def __init__(self, optparser, logger):
        optparser.set_usage('usage: %prog [options] <hostname of server to check>')
        optparser.description = 'Check disk usage of the root partition'
        optparser.version = self.version
        optparser.add_option(
            '-s', '--server', default='localhost',
            help='clio database server to query (default: %default)')
        optparser.add_option(
            '-w', '--warning', default='50', metavar='RANGE',
            help='warning threshold (default: %default%)')
        optparser.add_option(
            '-c', '--critical', default='75', metavar='RANGE',
            help='warning threshold (default: %default%)')
        self._optparser = optparser
        self._logger = logger

    def process_args(self, options, args):
        self.warning = options.warning.rstrip('%')
        self.critical = options.critical.rstrip('%')
        self.db_server = options.server
        try:
            self.server = args[0]
        except IndexError:
            print('What server am I supposed to check?!')
            import sys
            sys.exit(3)

    def obtain_data(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'system_%s' % datetime.now().strftime('%Y%m')
        field = 'data.fs'
        res = db[coll_name].find_one({'host': self.server},
                                         sort=[('ts', pymongo.DESCENDING)],
                                         fields=[field, 'ts'])
        assert (datetime.utcnow() - res['ts']).seconds < 60, "stale data! is arke running?"

        self.usages = {}
        self.measures = []

        for fs in res['data']['fs']:
            percent_used = res['data']['fs'][fs]['percent']
            self.usages[fs] = percent_used
            self.measures.append(nagiosplugin.Measure(
                fs, percent_used, '%', self.warning, self.critical, 0, 100))

    def default_message(self):
        return '\n'.join(['%s is %i%% full' % (fs,used) for fs,used in self.usages.iteritems()])


main = nagiosplugin.Controller(AllDiskSpaceCheck)
if __name__ == '__main__':
   main()
