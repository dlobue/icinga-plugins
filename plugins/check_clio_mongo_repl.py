#!/usr/bin/python2

from datetime import datetime
from pprint import pformat

import nagiosplugin
import pymongo
from pyes import ES, query

from utils import decode_record_timestamp

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
            '-P', '--port', default=9200,
            help='clio database server port (default: %default)')
        optparser.add_option(
            '-T', '--conn_type', default='http',
            help='clio database server connection type [http, https, thrift] (default: %default)')
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
        self.db_port = options.port
        self.db_conn_type = options.conn_type
        try:
            self.server = args.pop(0)
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
                          'mongodb',
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
        coll_name = 'mongodb_%s' % datetime.utcnow().strftime('%Y%m')
        res = db[coll_name].find_one({'host': self.server},
                                     sort=[('timestamp', pymongo.DESCENDING)],
                                     fields=[field, 'timestamp'],
                                    )
        return res

    def obtain_data(self):
        field = 'data.repl_status'
        res = self._obtain_data_es(field)

        assert (datetime.utcnow() - res['timestamp']).seconds < 60, "stale data! is arke running?"
        
        if res[field] is None:
            self.primary = None
            self.repl_lag = 0
        else:
            members = res[field]['members']
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
                self.repl_lag = max(0, primary['optime']['t'] - me['optime']['t'])

            #assert primary['optime']['t'] >= me['optime']['t'], "optime of master is less than the slave. the hell?\n%s" % pformat(res)
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
