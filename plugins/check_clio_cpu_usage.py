#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo

class CPUCheck(nagiosplugin.Check):

    name = 'cpu use percent'
    version = '0.1'

    def __init__(self, optparser, logger):
        optparser.set_usage('usage: %prog [options] <hostname of server to check>')
        optparser.description = 'Check cpu usage (not load)'
        optparser.version = self.version
        optparser.add_option(
            '-s', '--server', default='localhost',
            help='clio database server to query (default: %default)')
        optparser.add_option(
            '-w', '--warning', default='80', metavar='RANGE',
            help='warning threshold (default: %default%)')
        optparser.add_option(
            '-c', '--critical', default='95', metavar='RANGE',
            help='warning threshold (default: %default%)')

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
        field = 'data.cpu_times'
        res = db[coll_name].find({'host': self.server},
                                     sort=[('ts', pymongo.DESCENDING)],
                                     fields=[field, 'ts'],
                                     limit=2,
                                    )

        t2 = res.next()
        t1 = res.next()

        assert (datetime.utcnow() - t2['ts']).seconds < 60, "stale data! is arke running?"

        t1_all = sum(t1['data']['cpu_times'].values())
        t1_busy = t1_all - t1['data']['cpu_times']['idle']

        t2_all = sum(t2['data']['cpu_times'].values())
        t2_busy = t2_all - t2['data']['cpu_times']['idle']

        busy_delta = t2_busy - t1_busy
        all_delta = t2_all - t1_all
        busy_perc = (busy_delta / all_delta) * 100

        interval = (t2['ts'] - t1['ts'])
        self.interval = '%i.%i' % (interval.seconds, interval.microseconds)
        self.usage = busy_perc
        self.measures = [nagiosplugin.Measure(
            'cpu_use_percent', self.usage, '%', self.warning, self.critical, 0, 100)]

    def default_message(self):
        return '%i%% of cpu used over last %s seconds' % (self.usage, self.interval)


main = nagiosplugin.Controller(CPUCheck)
if __name__ == '__main__':
   main()
