from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, \
explode, split, size, array, udf, rank, sort_array, array_contains, \
collect_list, struct, desc, array, lit, max, min, length, when
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover, NGram, PCA, VectorAssembler
from pyspark.sql.types import StringType, MapType, FloatType, IntegerType
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


if __name__ == "__main__":

    sparkSession = SparkSession.builder.appName("Question3").getOrCreate()
    data = sparkSession.read.option('header', True).csv("hdfs://raspberrypi-dml0:9000/rajabi/recipes.csv")
    data = data.select('RecipeCategory', 'Calories', 'CholesterolContent', 'CarbohydrateContent',
                       'SugarContent', 'ProteinContent', 'RecipeServings', 'HighScore')

    data = data.filter(data.RecipeCategory.isin(['< 15 Mins', '< 30 Mins', '< 60 Mins', '< 4 Hours']))
    # data = data.withColumn('RecipeCategory', when(
    #     data.RecipeCategory.isin(['< 15 Mins', '< 30 Mins', '< 60 Mins', '< 4 Hours']),
    #     data.RecipeCategory).otherwise("NotKnown")
    #                        )

    stringIndexer = StringIndexer(inputCol="RecipeCategory", outputCol="CategoryIndex")
    model = stringIndexer.fit(data)
    data = model.transform(data)

    feature_columns = ['Calories', 'CholesterolContent', 'CarbohydrateContent',
                       'SugarContent', 'ProteinContent', 'RecipeServings', 'HighScore']
    for feature in feature_columns:
        data = data.withColumn(feature, col(feature).cast(FloatType()))

    data = data.withColumn('CategoryIndex', col('CategoryIndex').cast(IntegerType()))

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data = assembler.transform(data)

    pca = PCA(k=4, inputCol="features", outputCol="pcaFeatures")
    pca_model = pca.fit(data)
    data = pca_model.transform(data)

    splits = data.randomSplit([0.8, 0.2], seed=1111)
    train, test = splits[0], splits[1]

    NN = MultilayerPerceptronClassifier(labelCol="CategoryIndex", featuresCol='pcaFeatures', maxIter=100,
                                        layers=[4, 10, 12, 15, 12, 10, 4], blockSize=128)
    model = NN.fit(train)

    result = model.transform(test)
    result = result.withColumn('label', col('CategoryIndex').cast(IntegerType()))
    predictionAndLabels = result.select("prediction", "label")

    evaluator = MulticlassClassificationEvaluator()
    evaluator.evaluate(predictionAndLabels)

