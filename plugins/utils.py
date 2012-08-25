
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def decode_record_timestamp(record):
    for key in ('timestamp', 'ts'):
        if key in record and isinstance(record[key], basestring):
            record[key] = decode_timestamp(record[key])
            break
    return record


def decode_timestamp(data):
    '''
    returns a datetime object in UTC
    '''
    fmt = '%Y-%m-%dT%H:%M:%S'
    tzoffset = None
    if '.' in data:
        fmt += '.%f'

    if '-' in data[data.index('T'):]:
        tzsign = '-'
    elif '+' in data:
        tzsign = '+'
    else:
        tzsign = None

    if tzsign:
        tzidx = data.rindex(tzsign)
        tzstr = data[tzidx:].strip(tzsign)
        data = data[:tzidx]
        if ':' in tzstr:
            tzhours,tzminutes = map(int, (tzsign+_ for _ in tzstr.split(':')))
        elif len(tzstr) == 4:
            tzhours = int(tzsign+tzstr[:2])
            tzminutes = int(tzsign+tzstr[2:])
        else:
            logger.warn('unrecognized timezone format. assuming tz to be UTC')
            tzhours,tzminutes = 0,0
        tzoffset = timedelta(hours=tzhours, minutes=tzminutes)



    dtobj = datetime.strptime(data, fmt)
    if tzoffset is not None:
        dtobj = dtobj - tzoffset
    return dtobj


