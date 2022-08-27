from mrjob.job import MRJob
import time
import re


class ptDM1(MRJob):

    def mapper(self, _, line):

        isBetween = False
        fields = line.split(",")

        if (len(fields) == 7):
            fields = line.split(",")

            amount = int(fields[3])
            to_address = str(fields[2])

            date = int(fields[6])
            checkTime = time.gmtime(date)
            printTime = time.strftime("%d %b %Y", checkTime)

            time1 = time.strptime("16 Oct 2017", "%d %b %Y")
            time2 = time.strptime("16 Jan 2018", "%d %b %Y")
            if (time1 <= checkTime and checkTime <= time2):
                isBetween = True
            else:
                isBetween = False

            if not (to_address == "null" or amount == 0):
                if (isBetween == True):
                    yield(printTime, (to_address, amount))

    def reducer(self, date, values):
        tot = 0
        for val in values:
            tot += int(val[1])
            add = val[0]
        totInUSD = tot/230608083925230
        yield(add, ('{},{}'.format(totInUSD, date)))


if __name__ == '__main__':
    ptDM1.JOBCONF = {'mapreduce.job.reduces': '25'}
    ptDM1.run()
