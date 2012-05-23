#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
from pyes import ES, query

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
            '-p', '--port', default=9200,
            help='clio database server port (default: %default)')
        optparser.add_option(
            '--conn_type', default='http',
            #'-t', '--conn_type', default='http',
            help='clio database server connection type [http, https, thrift] (default: %default)')
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
        self.db_port = options.port
        self.db_conn_type = options.conn_type
        try:
            self.server = args[0]
        except IndexError:
            print('What server am I supposed to check?!')
            import sys
            sys.exit(3)

    def _obtain_data_es(self, field):
        conn = ES("%s:%s" % (self.db_server, self.db_port))

        q = query.Search(query.TermQuery('host', self.server),
                         sort=[dict(ts=dict(order='desc'))],
                         fields=[field, 'ts'],
                        )
        res = conn.search_raw(q,
                          'clio', #TODO: turn into a parameter
                          'system',
                          size=2
                         )


        assert not res['timed_out']
        assert res['hits']['total']

        return [_['fields'] for _ in res['hits']['hits']]

    def obtain_data(self):

        field = 'data.cpu_times'

        t2, t1 = self._obtain_data_es(field)


        assert (datetime.utcnow() - t2['ts']).seconds < 60, "stale data! is arke running?"

        t1_all = sum(t1[field].values())
        t1_busy = t1_all - t1[field]['idle']

        t2_all = sum(t2[field].values())
        t2_busy = t2_all - t2[field]['idle']

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
