#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo
from pyes import ES, query

from utils import decode_record_timestamp

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
            '-p', '--port', default=9200,
            help='clio database server port (default: %default)')
        optparser.add_option(
            '--conn_type', default='http',
            #'-t', '--conn_type', default='http',
            help='clio database server connection type [http, https, thrift] (default: %default)')
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
        self.db_port = options.port
        self.db_conn_type = options.conn_type
        try:
            self.server = args[0]
        except IndexError:
            print('What server am I supposed to check?!')
            import sys
            sys.exit(3)

    def _obtain_data_es(self, field, size=1):
        conn = ES("%s:%s" % (self.db_server, self.db_port))
        utcnow = datetime.utcnow()

        q = query.Search(query.TermQuery('host', self.server),
                         sort=[dict(timestamp=dict(order='desc'))],
                         fields=[field, 'timestamp'],
                        )
        res = conn.search_raw(q,
                          utcnow.strftime('clio_%Y%m'),
                          'system',
                          size=size
                         )


        assert not res['timed_out']
        assert res['hits']['total']

        records = [decode_record_timestamp(_['fields']) for _ in res['hits']['hits']]
        if res['hits']['total'] == 1:
            records = records[0]

        return records

    def _obtain_data_mongo(self, field):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'system_%s' % datetime.utcnow().strftime('%Y%m')
        res = db[coll_name].find_one({'host': self.server},
                                         sort=[('timestamp', pymongo.DESCENDING)],
                                         fields=[field, 'timestamp'])

        return res

    def obtain_data(self):
        field = 'data.fs'

        res = self._obtain_data_es(field)

        assert (datetime.utcnow() - res['timestamp']).seconds < 60, "stale data! is arke running? timestamp: %s" % res['timestamp']

        self.usages = {}
        self.measures = []

        for fs in res[field]:
            percent_used = res[field][fs]['percent']
            self.usages[fs] = percent_used
            self.measures.append(nagiosplugin.Measure(
                fs, percent_used, '%', self.warning, self.critical, 0, 100))

    def default_message(self):
        return '\n'.join(['%s is %i%% full' % (fs,used) for fs,used in self.usages.iteritems()])


main = nagiosplugin.Controller(AllDiskSpaceCheck)
if __name__ == '__main__':
   main()
