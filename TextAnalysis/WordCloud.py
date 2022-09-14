from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, \
    explode, split, size, array, udf, rank, sort_array, array_contains, \
    collect_list, struct, desc, array, lit, max, min, length
from pyspark.sql.types import StringType, MapType, FloatType, IntegerType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover
from wordcloud import WordCloud
import matplotlib.pyplot as plt


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

    remover = StopWordsRemover(inputCol="words", outputCol="no-stop-words")
    data = remover.transform(data)

    hashingTF = HashingTF(inputCol="no-stop-words", outputCol="features")
    data = hashingTF.transform(data)

    idf = IDF()
    idf.setInputCol('features')
    idf_model = idf.fit(data)
    idf_model.setOutputCol("idf")
    data = idf_model.transform(data)

    words_dataframe = data.select(explode('no-stop-words').name('word'))
    words_dataframe = words_dataframe.select('word').distinct().withColumn('word-as-array', array('word'))
    words_dataframe = words_dataframe.withColumn('no-stop-words', col('word-as-array'))

    words_dataframe = hashingTF.transform(words_dataframe)
    words_dataframe = idf_model.transform(words_dataframe)

    get_score = udf(lambda vec: dict(zip(vec.indices.tolist(), vec.values.tolist())),
                    MapType(StringType(), FloatType()))
    words_dataframe = words_dataframe.select('word-as-array',
                                             explode(get_score(col('idf'))).name('hash_index', 'value'))

    word_scores = words_dataframe.sort('value', ascending=False)
    top_50 = [row.asDict()['word-as-array'][0] for row in word_scores.take(50)]

    wordcloud = WordCloud(width=800, height=800,
                          background_color='white',
                          min_font_size=10).generate(' '.join(top_50))

    wordcloud.to_file('WordCloud.png')




