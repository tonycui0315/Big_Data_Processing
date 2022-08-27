"""
B1. Initial Aggregation
"""
from mrjob.job import MRJob
import time
import re


class ptB2(MRJob):
    """
    Mapper:

    Input file is a .csv file with 7 fields
    Split input line by comma and check if length is 7

    Get to_address from field index 2
        turn into an string
        store in variable 'to_address'

    Get transaction value from field index 3
        turn into an integer
        store in variable 'amount'

    For every line
        if the address is not 'null' and the transaction value is not 0
            yields to_address as KEY and amount as value
    """

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                to_address = str(fields[2])
                amount = int(fields[3])
                if not (to_address == "null" or amount == 0):
                    yield(to_address, amount)
        except:
            pass
    """
    Reducer:

    Receives KEY to_address as variable 'address' and amount as variable 'amount'

    Creates variable 'tot'
    For each amount
        add the amount to tot, to get total amount of transactions per address

    For every line, yields address as KEY and tot as value
    """

    def reducer(self, address, amount):
        tot = 0
        for val in amount:
            tot += val
        yield(address, tot)


if __name__ == '__main__':
    ptB2.run()
