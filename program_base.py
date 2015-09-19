# Jason Pang, Lewis Chi
# 997658594, 996905905
# ECS 165A Homework #4
# Christopher Nitta
# 12/9/2014


import argparse
import os
import logging


class ProgramBase:

    def __init__(self):
        self.activate_argument_parser()
        self.argparser.parse_args(namespace=self)
        self.activate_logger()

    def activate_argument_parser(self):
        self.argparser = argparse.ArgumentParser(
            description='Puts NHTS and EIA transportation data into the database and returns '
                        'queries on the dataset.')

        self.argparser.add_argument('--rebuild', dest='rebuild', action='store_true', default=False,
                        help='Specifies whether to wipe '
                             'and rebuild the database ('
                             'default: False)')

        self.argparser.add_argument('--csv-dir', dest='csv_dir', action='store', default='', help='Specifies the directory to find '
                                                                                      'all CSV files in (default: the '
                                                                                      'current directory)')

        self.argparser.add_argument('--verbosity', dest='verbosity', action='store', default='WARNING',
                                    help='Specifies the level of '
                                                                                           'verbosity to show. '
                                                                                           '`DEBUG` shows all '
                                                                                           'messages (default: '
                                                                                           'WARNING). Allowed values: DEBUG, INFO, WARNING, ERROR, CRITICAL')

        self.argparser.add_argument('--flush-after', dest='flush_after', action='store', default='10000',
                                    help='Specifies after how many collected insert statements should the program '
                                         'commit them to avoid overloading the database at once.', type=int)


    def activate_logger(self):
        numeric_level = getattr(logging, self.verbosity.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % self.verbosity)
        logging.basicConfig(format='%(levelname)s (%(asctime)s): %(message)s', level=numeric_level,
                            datefmt='%m/%d/%Y %I:%M:%S %p')

    def run(self):
        raise NotImplementedError