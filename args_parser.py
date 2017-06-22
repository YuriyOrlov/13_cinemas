import argparse
import textwrap
import sys


class ConsoleArgsParser(argparse.ArgumentParser):

    def __init__(self, *args, **kwargs):
        super(ConsoleArgsParser, self).__init__(*args, **kwargs)
        self.prog = "What's in the cinemas?"
        self.formatter_class = argparse.RawDescriptionHelpFormatter
        self.description = textwrap.dedent('''\
                      This program takes data from two web sites and show what happening in
                      movie theaters in Moscow. It shows the number of cinemas and average
                      rating for every movie.\n
                      -----------------------------------------------------------------
                      If you want to stop the program press Ctrl+C.
                      ------------------------------------------------------------------
                      This program had been tested on Python 3.5.2.
                      ''')
        self.add_argument('-get_common_proxies',
                          help='Receive a bunch of proxies and,\
                              save them to a file \
                              File "untested_prx.pkl" \
                              will be created \
                              EXAMPLE: -get_common_proxies yes\
                              (default: %(default)s)',
                          type=str, default=None)
        self.add_argument('-get_anonymous_proxies',
                          help='Receive a bunch of anonymous L3 proxies, then test`em\
                              and save them to a file \
                              File "anon_prx.pkl" will be created \
                              ATTENTION: Could take MUCH TIME (40 - 60 minutes)!\
                              EXAMPLE: -get_anonymous_proxies yes\
                              (default: %(default)s)',
                          type=str, default=None)
        self.add_argument('-get_user_agents',
                          help='Receive a bunch of user agent records\
                              File "user_agents.pkl" will be created \
                              EXAMPLE: -get_user_agents yes\
                              (default: %(default)s)',
                          type=str, default=None)
        self.add_argument('-renew_common_proxies',
                          help='Proxies will be renewed \
                              File "untested_prx.pkl" will be rewritten. \
                              (default: %(default)s)',
                          type=str, default=None)
        self.add_argument('-renew_anonymous_proxies',
                          help='Anonymous proxies will be renewed \
                              File "anon_prx.pkl" will be rewritten. \
                              ATTENTION: Could take MUCH TIME (40 - 60 minutes)!\
                              (default: %(default)s)',
                          type=str, default=None)
        self.add_argument('-ret',
                          help='How many records to return\
                              e.g. -ret 5. If you will enter 0\
                              script will return all records\
                              (default: %(default)s)',
                          type=int, default=10)

    def error(self, message):
        sys.stderr.write('error: {}\n'.format(message))
        self.print_help()
        sys.exit(2)
