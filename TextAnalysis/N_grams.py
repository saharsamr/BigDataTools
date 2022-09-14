from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, \
explode, split, size, array, udf, rank, sort_array, array_contains, \
collect_list, struct, desc, array, lit, max, min, length
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover, NGram


def preprocess(df):

    df = df.withColumn('value', regexp_replace(col('value'), 'Mr. ', 'Mr '))
    df = df.withColumn('value', regexp_replace(col('value'), 'Ms. ', 'Ms '))
    df = df.withColumn('value', regexp_replace(col('value'), 'Mrs. ', 'Mrs '))
    df = df.withColumn('value', regexp_replace(col('value'), "'s", ''))
    df = df.withColumn('value', regexp_replace(col('value'), "`s", ''))

    punctuations = ':;"\'/,_-—`“”'
    for p in punctuations:
        df = df.withColumn('value', regexp_replace(col('value'), p, ' '))

    df = df.withColumn('value', regexp_replace(col('value'), ' +', ' '))
    df = df.withColumn('value', regexp_replace(col('value'), '(\.|!|\?) ', '$1'))
    df = df.withColumn('value', regexp_replace(col('value'), '^\s', ''))
    df = df.withColumn('value', regexp_replace(col('value'), '~[A-Za-z\.\?! ]', ''))
    df = df.withColumn('value', lower(col('value')))
    df = df.withColumn('value', regexp_replace(col('value'), 'chapter \d+', ''))

    return df


if __name__ == "__main__":

    sparkSession = SparkSession.builder.appName("Question1").getOrCreate()
    data = sparkSession.read.text("hdfs://raspberrypi-dml0:9000/rajabi/book-text.txt")

    data = preprocess(data)
    data = data.withColumn('value', explode(split(col('value'), '\.|!|\?')))

    data = data.withColumn('sentence', explode(split(col('value'), '\.|!|\?'))).select('sentence')
    data = data.filter(data.sentence != '')
    data = data.filter(data.sentence != ' ')

    tokenizer = Tokenizer(outputCol="words")
    tokenizer.setInputCol("sentence")
    data = tokenizer.transform(data)

    word_count = data.select('words').withColumn('value', explode('words')).groupby('value').count().orderBy(
        desc('count'))

    word_count.show(50)

    two_gram = NGram(n=2)
    two_gram.setInputCol("words")
    two_gram.setOutputCol("2-grams")
    data = two_gram.transform(data)
    data.show(20)

    three_gram = NGram(n=3)
    three_gram.setInputCol("words")
    three_gram.setOutputCol("3-grams")
    data = three_gram.transform(data)
    data.show(20)

    three_grams_count = data.select('3-grams').withColumn('value', explode('3-grams')).groupby('value').count()\
        .orderBy(desc('count'))

    three_grams_count.show(50)










