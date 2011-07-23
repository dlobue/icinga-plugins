#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo

class MongodbReplLagCheck(nagiosplugin.Check):

    name = 'mongodb replication lag'
    version = '0.1'

    def __init__(self, optparser, logger):
        optparser.set_usage('usage: %prog [options] <hostname of server to check>')
        optparser.description = 'Check cpu usage (not load)'
        optparser.version = self.version
        optparser.add_option(
            '-s', '--server', default='localhost',
            help='clio database server to query (default: %default)')
        optparser.add_option(
            '-w', '--warning', default='10', metavar='RANGE',
            help='warning threshold (default: %default%)')
        optparser.add_option(
            '-c', '--critical', default='15', metavar='RANGE',
            help='warning threshold (default: %default%)')

    def process_args(self, options, args):
        self.warning = options.warning.rstrip('%')
        self.critical = options.critical.rstrip('%')
        self.db_server = options.server
        try:
            self.server = args.pop(0)
        except IndexError:
            print('What server am I supposed to check?!')
            import sys
            sys.exit(3)

    def obtain_data(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'mongodb_%s' % datetime.now().strftime('%Y%m')
        field = 'data.repl_status'
        res = db[coll_name].find_one({'host': self.server},
                                     sort=[('ts', pymongo.DESCENDING)],
                                     fields=[field, 'ts'],
                                    )

        assert (datetime.utcnow() - res['ts']).seconds < 60, "stale data! is arke running?"
        
        if res['data']['repl_status'] is None:
            self.primary = None
            self.repl_lag = 0
        else:
            members = res['data']['repl_status']['members']
            primary = None
            me = None

            for member in members:
                if member.get('self', False):
                    me = member
                if member.get('state', None) == 1:
                    primary = member

            if primary is me:
                self.primary = True
                self.repl_lag = 0
            else:
                self.primary = False
                self.repl_lag = primary['optime']['t'] - me['optime']['t']

        self.measures = [nagiosplugin.Measure(
            'mongodb_repl_lag', self.repl_lag, warning=self.warning, critical=self.critical)]

    def default_message(self):
        if self.primary is None:
            return 'not in a replica set'
        if self.primary:
            return 'currently the primary server. not lagging behind self.'
        return 'optime is %i behind primary' % self.repl_lag


main = nagiosplugin.Controller(MongodbReplLagCheck)
if __name__ == '__main__':
   main()
