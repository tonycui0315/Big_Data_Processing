# Part B in Spark
import pyspark
from operator import add

sc = pyspark.SparkContext()

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:
             # to_address in transactions.csv
            str(fields[2])
            if int(fields[3]) == 0:
                return False
        elif len(fields) == 5:
            # address in contracts.csv
            str(fields[0])
        else:
            return False
        return True
    except:
        return False

# parse in 'dirty' files
dirty_transactons = sc.textFile('/data/ethereum/transactions')
dirty_contracts = sc.textFile('/data/ethereum/contracts')

# filter out 'bad' lines
clean_transactions = dirty_transactons.filter(is_good_line)
clean_contracts = dirty_contracts.filter(is_good_line)

# Get to_address(KEY) and transaction value(VALUE) from transactions.csv
address_value= clean_transactions.map(lambda l: (l.split(',')[2], int(l.split(',')[3])))
# Summing up total transaction values using add, with address as KEY
adding = address_value.reduceByKey(add)
# Join with contracts.csv with address as KEY, to check if address is user contract or service(smart) contract
final = adding.join(clean_contracts.map(lambda x: (x.split(',')[0], 'contract')))
# use takeOrdered function to get top ten most popular services
top10_popular = final.takeOrdered(10, key = lambda x: -x[1][0])
for record in top10_popular:
    print("{},{}".format(record[0], int(record[1][0])))
