#!/usr/bin/python2

from datetime import datetime
from time import time

import nagiosplugin
import pymongo
from pyes import ES, query

class AliveCheck(nagiosplugin.Check):

    name = 'alive check'
    version = '0.2'
    SSH_INTERVAL = 30

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

    def _obtain_data_ssh_es(self):
        conn = ES("%s:%s" % (self.db_server, self.db_port))

        interval = self.SSH_INTERVAL
        timestamp = int(time())
        timestamp = (timestamp - (timestamp % interval)) - interval

        res = conn.get('clio', 'ssh_hello', timestamp)
        return res

    def _obtain_data_system_es(self, size=1):
        conn = ES("%s:%s" % (self.db_server, self.db_port))

        q = query.Search(query.TermQuery('host', self.server),
                         sort=[dict(ts=dict(order='desc'))],
                         fields=['ts'],
                        )
        res = conn.search_raw(q,
                          'clio', #TODO: turn into a parameter
                          'system',
                          size=size
                         )


        assert not res['timed_out']
        assert res['hits']['total']

        res = [_['fields'] for _ in res['hits']['hits']]
        if size == 1:
            res = res[0]
        return res

    def _obtain_data_ssh_mongo(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'ssh_hello_%s' % datetime.utcnow().strftime('%Y%m')
        found = db[coll_name].find_one(sort=[('_id', pymongo.DESCENDING)],
                                       skip=1, #the latest result set is probably still receiving results.
                                      )

        return found
        #assert (datetime.utcnow() - found['_id']).seconds < 60, "stale data! is arke running?"

    def _obtain_data_system_mongo(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'system_%s' % datetime.utcnow().strftime('%Y%m')
        res = db[coll_name].find_one({'host': self.server},
                                         sort=[('ts', pymongo.DESCENDING)],
                                         fields=['ts'])

        return res
        #assert (datetime.utcnow() - res['ts']).seconds < 60, "stale data! is arke running?"
        #assert ((datetime.utcnow() - res['ts']).seconds < 60) or ((datetime.utcnow() - found['_id']).seconds < 60), "stale data! is arke running?"

    def obtain_data(self):

        res = self._obtain_data_system_es()
        found = self._obtain_data_ssh_es()

        self.alive = sent_data_recently =  (datetime.utcnow() - res['ts']).seconds < 60

        try:
            ssh_data_fresh = (datetime.utcnow() - found['_id']).seconds < 60
        except KeyError:
            ssh_data_fresh = True



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
                'alive', int(self.alive), critical=0)]
                #'alive', self.alive, warning=self.warning, critical=0)]


    def default_message(self):
        if hasattr(self, 'lag'):
            return 'average latency to perform SSH banner exchange: %f' % self.lag
        elif hasattr(self, 'alive') and self.alive:
            return "has sent in data recently and is alive, but is having network problems"


main = nagiosplugin.Controller(AliveCheck)
if __name__ == '__main__':
    main()
