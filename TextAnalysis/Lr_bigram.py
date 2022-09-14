from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, \
explode, split, size, array, udf, rank, sort_array, array_contains, \
collect_list, struct, desc, array, lit, max, min, length
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover, NGram, VectorAssembler
from pyspark.sql.types import IntegerType


class Preprocess(Transformer):

    def __init__(self):
        super(Preprocess, self).__init__()

    def _transform(self, df):
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


class SplitToSentence(Transformer):

    def __init__(self):
        super(SplitToSentence, self).__init__()

    def _transform(self, df):

        df = df.withColumn('sentence', explode(split(col('value'), '\.|!|\?')))
        return df


class AddBiGramWordLength(Transformer):

    def __init__(self):
        super(AddBiGramWordLength, self).__init__()

    def _transform(self, df):

        df = df.withColumn('n-gram-words', explode(col('n-grams'))).select('n-gram-words')
        df = df.withColumn('word1', split(col('n-gram-words'), ' ').getItem(0))\
            .withColumn('word2', split(col('n-gram-words'), ' ').getItem(1))
        df = df.withColumn('word3', split(col('n-gram-words'), ' ').getItem(2))
        df = df.withColumn('len1', length(col('word1'))).withColumn('len2', length(col('word2')))
        df = df.withColumn('len3', length(col('word3')))
        # df = df.withColumn('more_than_2', (col('len2')-col('len1') > 2) | (col('len2')-col('len1') < -2))
        df = df.withColumn('more_than_2', ((col('len2')-col('len1') > 2) | (col('len2')-col('len1') < -2))
                           & ((col('len3')-col('len2') > 2) | (col('len3')-col('len2') < -2)))
        df = df.withColumn('more_than_2', col('more_than_2').cast(IntegerType()))

        return df


if __name__ == "__main__":

    sparkSession = SparkSession.builder.appName("Question1").getOrCreate()
    data = sparkSession.read.text("hdfs://raspberrypi-dml0:9000/rajabi/book-text.txt")

    preprocessor = Preprocess()
    spliter = SplitToSentence()

    tokenizer = Tokenizer(outputCol="words")
    tokenizer.setInputCol("sentence")

    # n_gram = NGram(n=2)
    n_gram = NGram(n=3)
    n_gram.setInputCol("words")
    n_gram.setOutputCol("n-grams")

    bigram_word_length = AddBiGramWordLength()

    vector_assembler = VectorAssembler(inputCols=['len1', 'len2'], outputCol='features')

    lr = LogisticRegression(featuresCol='features', labelCol='more_than_2', maxIter=10)

    pipeline = Pipeline(stages=[preprocessor, spliter, tokenizer, n_gram, bigram_word_length, vector_assembler, lr])
    model = pipeline.fit(data)

    predictions = model.transform(data).select("more_than_2", "prediction")
    acc = predictions.filter(predictions.prediction == predictions.more_than_2).count() / predictions.count()
    print(f'accuracy is equal to: {acc}')
