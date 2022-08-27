from mrjob.job import MRJob
import re

class ptDM1(MRJob):
    def mapper(self, _, line):

        fields = line.split("\t")
        if len(fields) == 2:
            address = fields[0].replace('\"','')

            arr = fields[1]
            smallFields = arr.split(",")
            if len(smallFields) == 2:
                amount = float(smallFields[0].replace('\"',''))
                date = smallFields[1].replace('\"','')
                values = (amount, date)
                # for sorting, always yield all values together in one value, with None as key
        yield(None, (address, values))



    def reducer(self, _, values):
        sort = sorted(values, reverse=True, key=lambda x:x[1][0])[:10]
        for val in sort:
            yield(val[1][0], '{},{}'.format(val[0], val[1][1]))

if __name__ == '__main__':
    ptDM1.run()
