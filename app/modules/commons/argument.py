from datetime import datetime
import argparse

def valid_ids(s):
    try:
        return [int(id) for id in s.split(',')]
    except ValueError:
        message = 'Invalid ids: "{0}".'.format(s)
        raise argparse.ArgumentTypeError(message)


def valid_date(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        message = 'Invalid date: "{0}".'.format(s)
        raise argparse.ArgumentTypeError(message)
