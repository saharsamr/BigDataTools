from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, \
    explode, split, size, array, udf, rank, sort_array, array_contains, \
    collect_list, struct, desc, array, lit, max, min, length
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover

import pyarrow as pa
import pickle


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
    data.show(10)
    data = data.withColumn('value', explode(split(col('value'), '\.|!|\?')))
    data.show(10)

    data = data.withColumn('sentence', explode(split(col('value'), '\.|!|\?'))).select('sentence')
    data = data.filter(data.sentence != '')
    data = data.filter(data.sentence != ' ')
    data = data.withColumn('length', size(split(col('sentence'), ' ')))
    data.show(10)

    tokenizer = Tokenizer(outputCol="words")
    tokenizer.setInputCol("sentence")
    data = tokenizer.transform(data)
    data.show(10)

    remover = StopWordsRemover(inputCol="words", outputCol="no-stop-words")
    data = remover.transform(data)

    hashingTF = HashingTF(inputCol="no-stop-words", outputCol="features")
    data = hashingTF.transform(data)
    data.show()

    idf = IDF()
    idf.setInputCol('features')
    model = idf.fit(data)
    model.setOutputCol("idf")
    data = model.transform(data)
    data.show(10)

    data.write.save('hdfs://raspberrypi-dml0:9000/rajabi/data', format='parquet', mode='write')

    hdfs = pa.hdfs.connect()
    with hdfs.open('hdfs://raspberrypi-dml0:9000/rajabi/hashingTF.pkl', 'wb') as f:
        pickle.dump(hashingTF, f)
    with hdfs.open('hdfs://raspberrypi-dml0:9000/rajabi/idf.pkl', 'wb') as f:
        pickle.dump(model, f)





