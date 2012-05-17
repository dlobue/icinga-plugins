#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo
from pyes import ES, query

class SSHLagCheck(nagiosplugin.Check):

    name = 'ssh hello response latency'
    version = '0.2'

    def __init__(self, optparser, logger):
        optparser.set_usage('usage: %prog [options] <hostname of server to check>')
        optparser.description = 'Check SSH banner exchange lag'
        optparser.version = self.version
        optparser.add_option(
            '-s', '--server', default='localhost',
            help='clio database server to query (default: %default)')
        optparser.add_option(
            '-P', '--port', default=9200,
            help='clio database server port (default: %default)')
        optparser.add_option(
            '-T', '--conn_type', default='http',
            help='clio database server connection type [http, https, thrift] (default: %default)')
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
        self.db_port = options.port
        self.db_conn_type = options.conn_type
        self.minimum = options.minimum
        try:
            self.server = args[0]
        except IndexError:
            print('What server am I supposed to check?!')
            import sys
            sys.exit(3)

    def _obtain_data_es(self, field=None, size=1):
        conn = ES("%s:%s" % (self.db_server, self.db_port))

        q = query.Search(query.TermQuery('host', self.server),
                         sort=[dict(ts=dict(order='desc'))],
                         fields=[field, 'ts'],
                        )
        res = conn.search(q,
                          'clio', #TODO: turn into a parameter
                          'ssh_hello', #TODO: turn into a parameter
                          size=size
                         )


        assert not res['timed_out']
        assert res['hits']['total']

        res = [_['fields'] for _ in res['hits']['hits']]
        if size == 1:
            res = res[0]
        return res

    def _obtain_data_mongo(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'ssh_hello_%s' % datetime.utcnow().strftime('%Y%m')
        found = db[coll_name].find(sort=[('_id', pymongo.DESCENDING)],
                                       skip=1, #the latest result set is probably still receiving results.
                                      )
        return found

    def obtain_data(self):
        #XXX: this should be a GET!

        found = self._obtain_data_es(size=2)[-1]

        assert (datetime.utcnow() - found['_id']).seconds < 60, "stale data! is arke running?"

        results = [ x for x in found['data'] if x['to'] == self.server ]

        assert len(results) > 0, "no results!"
        assert len(results) > self.minimum, "not enough results! only found %i results." % len(results)

        if all(( x['lag'] == -1 for x in results)):
            avg_lag = -1
        else:
            avg_lag = sum(( x['lag'] for x in results if x['lag'] >= 0 )) / len(results)

        self.lag = avg_lag
        self.measures = [nagiosplugin.Measure(
            'ssh_lag', self.lag, warning=self.warning, critical=self.critical)]

    def default_message(self):
        return 'average latency to perform SSH banner exchange: %f' % self.lag


main = nagiosplugin.Controller(SSHLagCheck)
if __name__ == '__main__':
    main()

