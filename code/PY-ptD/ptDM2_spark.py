import pyspark
import time

sc = pyspark.SparkContext()


def is_good_line_block(line):
    try:
        lines = line.split(',')
        if len(lines) != 9:
            return False
        # Block number
        float(lines[0])
        # Difficulty
        float(lines[3])
        # Timestamp
        float(lines[7])
        return True
    except:
        return False


def is_good_line_transactions(line):
    try:
        lines = line.split(',')
        if len(lines) != 7:
            return False
        # Gas Value
        float(lines[5])
        # Timestamp
        float(lines[6])
        return True
    except:
        return False


def is_good_line_contracts(line):
    try:
        lines = line.split(',')
        if len(lines) != 5:
            return False
        # Block number
        float(lines[3])
        return True
    except:
        return False


# parse in 'dirty' files
dirty_transactons = sc.textFile('/data/ethereum/transactions')
dirty_contracts = sc.textFile('/data/ethereum/contracts')
dirty_blocks = sc.textFile('/data/ethereum/blocks')

# filter out 'bad' lines
clean_transactions = dirty_transactons.filter(is_good_line_transactions)
clean_contracts = dirty_contracts.filter(is_good_line_contracts)
clean_blocks = dirty_blocks.filter(is_good_line_block)


# Job 1: Average Gas price over time
# Get timestamp(KEY) and gas price(VALUE) from transactions.csv
time_value = clean_transactions.map(lambda j: (float(j.split(',')[6]), float(j.split(',')[5])))
# String format timestamp (KEY) and gas price with counter (1)
date_value_counter = time_value.map(lambda (x, y): (time.strftime("%Y-%m", time.gmtime(x)), (y, 1)))
# Yield date and average gas price
date_average = date_value_counter.reduceByKey(lambda (x1, y1), (x2, y2): (x1+x2, y1+y2)).map(lambda k: (k[0], (k[1][0]/k[1][1])))

# Output sorted by average gas price from lowest to highest
avg_out = date_average.sortByKey(ascending=True)
avg_out.saveAsTextFile('Average_Gas_Price')


# Job 2: Difficulty changes over time
# Get block number(KEY) and counter (1) from contracts.csv
blockNum1_counter = clean_contracts.map(lambda l: (l.split(',')[3], 1))
# Get block number(KEY) and  w difficulty(index 3), x gas_used(index 6), z formatted date(timestamp (index 7)) as values from blocks.csv
blockNum2_values = clean_blocks.map(lambda x: (x.split(',')[0], (int(x.split(',')[3]), int(x.split(',')[6]), time.strftime('%Y-%m', time.gmtime(float(x.split(',')[7]))))))

# Joining previous two 'outputs' using block number as KEY, check if block number from blocks.csv exist in contracts.csv
join_results = blockNum2_values.join(blockNum1_counter).map(lambda (id, ((w, x, y), z)): (y, ((w, x), z)))
# Yield using date as KEY with difficulty and block number as values, sorted by difficulty from lowest to highest
final_results = join_results.reduceByKey(lambda ((x1, y1), z1), ((x2, y2), z2): ((x1 + x2, y1 + y2), z1+z2)).map(lambda s: (s[0], (float(s[1][0][0]/s[1][1]), s[1][0][1]/s[1][1]))).sortByKey(ascending=True)
final_results.saveAsTextFile('Complicated')
