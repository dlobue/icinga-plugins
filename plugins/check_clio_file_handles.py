#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo
from pyes import ES, query

from utils import decode_record_timestamp

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
            '-P', '--port', default=9200,
            help='clio database server port (default: %default)')
        optparser.add_option(
            '-T', '--conn_type', default='http',
            help='clio database server connection type [http, https, thrift] (default: %default)')
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

        res = [decode_record_timestamp(_['fields']) for _ in res['hits']['hits']]
        if len(res) == 1:
            res = res[0]

        return res

    def _obtain_data_mongo(self, field):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'system_%s' % datetime.utcnow().strftime('%Y%m')
        res = db[coll_name].find_one({'host': self.server},
                                         sort=[('timestamp', pymongo.DESCENDING)],
                                         fields=[field, 'timestamp'])
        return res


    def obtain_data(self):
        field = 'data.fh'
        res = self._obtain_data_es(field)
        #TODO: switching between mongo and es a switch
        #TODO: es concatenates nested fields. mongo doesn't. deal with it.

        assert (datetime.utcnow() - res['timestamp']).seconds < 60, "stale data! is arke running? timestamp: %s" % res['timestamp']

        self.data = data = res[field]
        self.measures = [nagiosplugin.Measure(
            'available_file_handles', data['max'] - data['open'], warning=self.warning, critical=self.critical)]

    def default_message(self):
        return '%s open file handles out of a max of %s' % (self.data['open'], self.data['max'])


main = nagiosplugin.Controller(FileHandleCheck)
if __name__ == '__main__':
   main()
