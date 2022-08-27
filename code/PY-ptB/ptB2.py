"""
B2. Joining transactions/contracts and filtering
"""
from mrjob.job import MRJob


class ptB2(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if(len(fields) == 1):
                fields = line.split('\t')
                if(len(fields) == 2):
                    join_key = fields[0].replace("\"", "")
                    join_value = fields[1]
                    yield (join_key, ('T', join_value))

            elif(len(line.split(',')) == 5):
                fields = line.split(',')
                join_key = fields[0]
                yield (join_key, ('C'))
        except:
            pass

    def reducer(self, address, values):
        tAmount = 0
        exist = False
        for val in values:
            if val[0] == 'C':
                exist = True
            else:
                tAmount += int(val[1])
        if(exist and tAmount != 0):
            yield (address, tAmount)


if __name__ == '__main__':
    ptB2.run()
