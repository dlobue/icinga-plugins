#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo

class AliveCheck(nagiosplugin.Check):

    name = 'alive check'
    version = '0.1'

    def __init__(self, optparser, logger):
        optparser.set_usage('usage: %prog [options] <hostname of server to check>')
        optparser.description = 'Check SSH banner exchange lag'
        optparser.version = self.version
        optparser.add_option(
            '-s', '--server', default='localhost',
            help='clio database server to query (default: %default)')
        optparser.add_option(
            '-w', '--warning', default='1', metavar='RANGE',
            help='warning threshold (default: %default)')
        optparser.add_option(
            '-c', '--critical', default='5', metavar='RANGE',
            help='warning threshold (default: %default)')
        optparser.add_option(
            '-m', '--minimum', default=5, type="int",
            help='minimum number of results that is acceptable to work with (default: %default)')

    def process_args(self, options, args):
        self.warning = options.warning.rstrip('%')
        self.critical = options.critical.rstrip('%')
        self.db_server = options.server
        self.minimum = options.minimum
        try:
            self.server = args[0]
        except IndexError:
            print('What server am I supposed to check?!')
            import sys
            sys.exit(3)

    def obtain_data(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'ssh_hello_%s' % datetime.now().strftime('%Y%m')
        found = db[coll_name].find_one(sort=[('_id', pymongo.DESCENDING)],
                                       skip=2, #the latest result set is probably still receiving results.
                                      )

        #assert (datetime.utcnow() - found['_id']).seconds < 60, "stale data! is arke running?"

        coll_name = 'system_%s' % datetime.now().strftime('%Y%m')
        res = db[coll_name].find_one({'host': self.server},
                                         sort=[('ts', pymongo.DESCENDING)],
                                         fields=['ts'])

        #assert ((datetime.utcnow() - res['ts']).seconds < 60) or ((datetime.utcnow() - found['_id']).seconds < 60), "stale data! is arke running?"
        self.alive = sent_data_recently =  (datetime.utcnow() - res['ts']).seconds < 60
        ssh_data_fresh = (datetime.utcnow() - found['_id']).seconds < 60



        results = [ x for x in found['data'] if x['to'] == self.server ]

        if ssh_data_fresh and results:

            if all(( x['lag'] == -1 for x in results)):
                avg_lag = -1
            else:
                avg_lag = sum(( x['lag'] for x in results if x['lag'] >= 0 )) / len(results)

            self.lag = avg_lag
            self.measures = [nagiosplugin.Measure(
                'alive-ssh_lag', self.lag, warning=self.warning, critical=self.critical)]
        elif sent_data_recently:
            self.measures = [nagiosplugin.Measure(
                'alive', self.alive, critical=0)]
                #'alive', self.alive, warning=self.warning, critical=0)]


    def default_message(self):
        if hasattr(self, 'lag'):
            return 'average latency to perform SSH banner exchange: %f' % self.lag
        elif hasattr(self, 'alive'):
            return "has sent in data recently and is alive, but is having network problems"


main = nagiosplugin.Controller(AliveCheck)
if __name__ == '__main__':
    main()
