from mrjob.job import MRJob
from mrjob.job import MRStep
import re


class ptC(MRJob):

    def mapper_1(self, _, line):
        try:
            fields = line.split(",")
            if (len(fields) == 9):
                address = fields[2]
                amount = int(fields[4])
                yield(address, amount)
        except:
            pass

    def reducer_1(self, address, amount):
        tot = sum(amount)
        yield(address, tot)

    def mapper_2(self, address, tot):
        try:
            yield(None, (address, tot))
        except:
            pass

    def reducer_2(self, _, values):
        count = 0
        sort = sorted(values, reverse=True, key=lambda x: x[1])[:10]
        for val in sort:
            count += 1
            yield(count, '{}-{}'.format(val[0], val[1]))

    def steps(self):
        return [MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
                MRStep(mapper=self.mapper_2, reducer=self.reducer_2)]


if __name__ == '__main__':
    ptC.run()
