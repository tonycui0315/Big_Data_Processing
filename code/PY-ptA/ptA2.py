"""
A2. Average value of transaction in each month
"""
from mrjob.job import MRJob
import time
import re


class ptA2(MRJob):
    """
    Mapper:

    Input file is a .csv file with 7 fields
    Split input line by comma and check if length is 7

    Get timestamp from field index 6
        turn into an integer
        format into human readable date format 'Year-Month' e.g. '2021-11'
        store in variable 'date'

    Get transaction value from field index 3
        turn into an integer
        store in variable 'amount'

    For every line, yields date as KEY and amount
    """

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                time_epoch = int(fields[6])
                date = time.strftime('%Y-%m', time.gmtime(time_epoch))
                amount = int(fields[3])
                yield(date, amount)
        except:
            pass
    """
    Combiner:

    Receives KEY date as variable 'time' and amount as variable 'value'

    Creates variable 'ct' and 'tot'
    For each 'time'
        increment 'ct' by 1, acting as a counter of how many total 'value' there are

    For every line, yields date as KEY and a tuple(ct, tot) as values
    """

    def combiner(self, time, value):
        ct = 0
        tot = 0
        for val in value:
            ct += 1
            tot += val
        yield(time, (ct, tot))

    """
    Reducer:

    Receives KEY time as variable 'time' and a tuple of values(ct and tot) as variable 'values'

    Creates variable 'counter' and 'amount'
    For each 'time'
        increment 'ct' by 1, acting as a counter of how many total 'values' there are

    For every line, yields date as KEY and amount as value
"""

    def reducer(self, time, values):
        counter = 0
        amount = 0
        for val in values:
            counter += val[0]
            amount += val[1]
        avg = amount/counter
        yield(time, avg)


if __name__ == '__main__':
    ptA2.run()
