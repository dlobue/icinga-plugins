#!/usr/bin/python2

from datetime import datetime

import nagiosplugin
import pymongo

class PostgresReplLagCheck(nagiosplugin.Check):

    name = 'postgres replication lag'
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

        try:
            self.ec2_public_hostname = args.pop(0)
        except IndexError:
            print("What's the ec2 public hostname of the server am I supposed to check?!")
            import sys
            sys.exit(3)

    def obtain_data(self):
        db = pymongo.Connection(self.db_server).clio
        coll_name = 'postgres_repl_%s' % datetime.now().strftime('%Y%m')
        field = 'data'
        res = db[coll_name].find_one({'$or': [{'host': self.server},
                                              {'data.slaves': {
                                                  '$elemMatch': {
                                                      'host': self.ec2_public_hostname,
                                                  }
                                              }},
                                             ],
                                     },
                                     sort=[('ts', pymongo.DESCENDING)],
                                     fields=[field, 'host', 'ts'],
                                    )

        assert (datetime.utcnow() - res['ts']).seconds < 60, "stale data! is arke running?"

        def calc_offset(data):
            pieces = data.split('/')
            return (int('ffffffff', 16) * int(pieces[0], 16) + int(pieces[1], 16))


        if self.server == res['host']:
            self.primary = True
            self.recieve_delay = 0
            self.replay_delay = 0
        else:
            self.primary = False
            master_num = calc_offset(res['data']['master'])
            slaves = res['data']['slaves']
            me = None
            for slave in slaves:
                if slave['host'] == self.ec2_public_hostname:
                    me = slave
                    break

            self.receive_delay = master_num - calc_offset(me['r'])
            self.replay_delay = master_num - calc_offset(me['p'])


        self.measures = [
            nagiosplugin.Measure(
                'postgres_receive_delay', self.receive_delay, warning=self.warning, critical=self.critical),
            nagiosplugin.Measure(
                'postgres_replay_delay', self.replay_delay, warning=self.warning, critical=self.critical),
        ]

    def default_message(self):
        if self.primary:
            return 'currently the primary server. not lagging behind self.'
        return 'receive time is %i behind primary, and replay time is %i behind primary' % \
                (self.receive_delay, self.replay_delay)


main = nagiosplugin.Controller(PostgresReplLagCheck)
if __name__ == '__main__':
   main()
