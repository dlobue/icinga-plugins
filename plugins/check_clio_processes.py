#!/usr/bin/python2

from datetime import datetime
from pprint import pformat
import re

import nagiosplugin
import pymongo
from pyes import ES, query

from .utils import decode_record_timestamp

class ProcessCheck(nagiosplugin.Check):

    name = 'Process check'
    version = '0.1'

    def __init__(self, optparser, logger):
        optparser.set_usage('usage: %prog [options] <hostname of server to check> <string to search for in process cmdline> [<port process must be listening on>]')
        optparser.description = ("Monitor the number of processes running with the specified name.\n"
                                 "Supports matches made using python regex")
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
            '-i', '--case_insensitive', action='store_true', default=False,
            help='clio database server to query (default: %default)')
        optparser.add_option(
            '-p', '--port_match', default='any',
            help=('if multiple ports are given, does a process need to contain '
                  'all the given ports in order to match, or will any do? '
                  '(valid values: any, all. default: %default)'))
        optparser.add_option(
            '-w', '--warning', default=None, metavar='RANGE',
            help='warning threshold (default: %default)')
        optparser.add_option(
            '-c', '--critical', default='1:', metavar='RANGE',
            help='warning threshold (default: %default)')

    def process_args(self, options, args):
        self.warning = options.warning
        self.critical = options.critical
        self.db_server = options.server
        self.db_port = options.port
        self.db_conn_type = options.conn_type
        self.port_match = options.port_match
        if self.port_match not in ('any', 'all'):
            print('Invalid match type! Valid values are "any" and "all". Got: %s' % self.port_match)
            import sys
            sys.exit(3)

        try:
            self.server = args.pop(0)
        except IndexError:
            print('What server am I supposed to check?!')
            import sys
            sys.exit(3)

        try:
            self.search_string = args.pop(0)
            self.search_obj = re.compile(self.search_string, options.case_insensitive and re.I)
        except IndexError:
            print('What process am I supposed to look for?!')
            import sys
            sys.exit(3)

        if args:
            ports = set()
            for arg in args:
                if any(((x in arg) for x in (',',' ','\t'))):
                    args.extend(arg.replace(',',' ').split())
                else:
                    ports.add(int(arg))

            self.listening_ports = ports

        else:
            self.listening_ports = None


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
        result = db[coll_name].find_one({'host': self.server},
                                         sort=[('timestamp', pymongo.DESCENDING)],
                                         fields=[field, 'timestamp'])

        return result


    def obtain_data(self):
        field = 'data.processes'
        result = self._obtain_data_es(field)
        assert (datetime.utcnow() - result['timestamp']).seconds < 60, "stale data! is arke running? timestamp: %s" % result['timestamp']

        def is_listening(connections, port):
            return any((x for x in connections if x['status'] == u'LISTEN' and x['local_address'][1] == port))


        parent_pids = []
        processes = []
        for properties in result[field]:
            pid = properties['pid']
            if self.search_obj.search(properties['cmdline']) and \
               properties['status'] not in ('zombie', 'dead', 'stopped', 'tracing stop'):

                if self.listening_ports:
                    if getattr(__builtins__, self.port_match)((
                        is_listening(properties['connections'], p) for p in self.listening_ports )):

                        parent_pids.append(pid)
                        processes.append((
                            pid,
                            properties['name'],
                            properties['cmdline'],
                        ))
                else:
                    processes.append((
                        pid,
                        properties['name'],
                        properties['cmdline'],
                    ))

        if parent_pids:
            parent_pids = map(int, parent_pids)
            for properties in result[field]:
                if properties['ppid'] in parent_pids:
                    processes.append((
                        properties['pid'],
                        properties['name'],
                        properties['cmdline'],
                    ))


        self.processes = processes
        self.found_count = len(processes)
        self.measures = [nagiosplugin.Measure(
            'processes_found', self.found_count, warning=self.warning, critical=self.critical)]

    def default_message(self):
        fields = ('pid', 'name', 'cmdline')
        processes = (pformat(zip(fields, p)) for p in self.processes)
        processes_str = '\n'.join(processes)
        return 'Found the following %i processes:\n%s' % (self.found_count, processes_str)


main = nagiosplugin.Controller(ProcessCheck)
if __name__ == '__main__':
   main()
