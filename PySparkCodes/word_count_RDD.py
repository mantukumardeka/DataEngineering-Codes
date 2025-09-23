
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read text file into RDD
rdd = spark.sparkContext.textFile("/Users/mantukumardeka/Desktop/data/repeated_words.txt")

# Split lines into words, convert to lowercase, and remove empty entries
words = rdd.flatMap(lambda line: line.split(" ")) \
           .map(lambda word: word.strip().lower()) \
           .filter(lambda word: word != "")

# Map each word to (word, 1)
word_pairs = words.map(lambda word: (word, 1))

# Reduce by key to count occurrences
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Collect results
for word, count in word_counts.collect():
    print(f"{word}: {count}")



