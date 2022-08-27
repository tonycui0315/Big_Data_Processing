from mrjob.job import MRJob
import re

class ptB3(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split("\t")
            address = fields[0].replace('\"','')
            amount = int(fields[1])
            yield(None,(address, amount))
        except:
            pass


    def reducer(self, _, values):
        count = 0
        sort = sorted(values, reverse=True, key=lambda x:x[1])[:10]
        for val in sort:
            count += 1
            yield(count, '{}-{}'.format(val[0], val[1]))

if __name__ == '__main__':
    ptB3.run()
