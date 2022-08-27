"""
A1. Number of transactions every month
"""
from mrjob.job import MRJob
import time
import re


class ptA1(MRJob):

    """
    Mapper:

    Input file transactions is a .csv file with 7 fields
    Split input line by comma and check if length is 7

    Get timestamp from field index 6
        turn into an integer
        format into human readable date format 'Year-Month' e.g. '2021-11'
        store in variable 'date'

    For every line, yields date as KEY and a '1' as counter
    """


def mapper(self, _, line):
    try:
        fields = line.split(",")
        if (len(fields) == 7):
            # access the fields you want, assuming the format is correct now
            time_epoch = int(fields[6])
            date = time.strftime("%Y-%m", time.gmtime(time_epoch))
            yield(date, 1)
    except:
        pass

    """
    Reducer:
    Receives KEY date as variable 'time' and '1' as variable 'value'
    Creates a variable 'tot' and adds '1' to itself every time to sum up total amount of counter
    For every line, yields date as KEY and a counter
    """

    def reducer(self, time, value):
        tot = 0
        for val in value:
            tot += val
        yield(time, tot)


if __name__ == '__main__':
    ptA1.run()
