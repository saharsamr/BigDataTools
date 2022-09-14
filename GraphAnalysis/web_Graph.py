from pyspark import SparkContext
from pyspark.sql.functions import col, lower, regexp_replace, \
explode, split, size, array, udf, rank, sort_array, array_contains, \
collect_list, struct, desc, array, lit, max, min, length
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
import networkx as nx
import csv
import random


def RDD_DFS(v, visited, adjacents):

    stack = [v]
    while len(stack):
        n = stack[-1]
        stack.pop()

        if not visited[n]:
            visited[n] = True

        if adjacents.get(n):
            for adj in adjacents.get(n):
                if not visited.get(adj):
                    stack.append(adj)

    return visited


def dataframe_DFS(v, visited):
    stack = [v]
    while len(stack):
        n = stack[-1]
        stack.pop()

        if not visited[n]:
            visited[n] = True
        adj_list = adjacents.get(n)
        if adj_list:
            for adj in adj_list.get('adjacents'):
                if not visited.get(adj):
                    stack.append(adj)

    return visited


if __name__ == "__main__":

    ######################################################## RDD
    sc = SparkContext.getOrCreate()
    data = sc.textFile('hdfs://raspberrypi-dml0:9000/rajabi/web_Graph.txt') \
        .mapPartitions(lambda line: csv.reader(line, delimiter='\t'))

    out_degrees = data.map(lambda row: (row[0], 1)).reduceByKey(lambda x, y: x + y)
    out_degrees.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

    in_degrees = data.map(lambda row: (row[1], 1)).reduceByKey(lambda x, y: x + y)
    in_degrees.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

    sum_ = out_degrees.map(lambda x: (1, x[1])).reduceByKey(lambda x, y: x + y).collect()[0]
    print(sum_[1] / out_degrees.count())
    sum_ = in_degrees.map(lambda x: (1, x[1])).reduceByKey(lambda x, y: x + y).collect()[0]
    print(sum_[1] / in_degrees.count())

    degrees = in_degrees.join(out_degrees)
    degrees = degrees.map(lambda x: (x[0], (x[1][0], x[1][1], x[1][0] + x[1][1])))
    degrees.map(lambda x: (x[1][2], (x[0], x[1][0], x[1][1]))).sortByKey(ascending=False).collect()

    sum_ = degrees.map(lambda x: (1, x[1][2])).reduceByKey(lambda x, y: x + y).collect()[0]
    print(sum_[1] / degrees.count())

    visited = {k: False for k in data.keys().collect()}
    visited.update({k: False for k in data.values().collect()})
    adjacents = data.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).collectAsMap()
    visited = RDD_DFS('0', visited, adjacents)

    for k, v in visited.items():
        if not v:
            print('not connected')
            break

    network = nx.Graph()
    for edge in data.collect():
        network.add_edge(edge[0], edge[1])
    connected_components = [c for c in sorted(list(nx.connected_components(network)), key=len, reverse=True)]
    print([len(c) for c in connected_components][:2])

    for c in connected_components:
        giant = network.subgraph(c)
        break
    print(nx.diameter(giant))

    nodes_with_odd_degree = degrees.filter(lambda x: x[1][2] % 2 == 1)
    print(nodes_with_odd_degree.count())
    print(degrees.filter(lambda x: x[1][0] != x[1][1]).count())

    adjacents = data.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).collectAsMap()
    triangles = []
    for u, adj_list in adjacents.items():

        if adj_list:
            for v in adj_list:
                adj_adj_list = adjacents.get(v)
                if adj_adj_list:
                    for w in adj_adj_list:
                        third_adjs = adjacents.get(w)
                        if u != w and third_adjs and v in third_adjs:
                            triangles.append((u, v, w))

    print(triangles)

    ######################################################## DataFrame
    sparkSession = SparkSession.builder.appName("Question2").getOrCreate()
    data = sparkSession.read.option('delimiter', '\t').csv('hdfs://raspberrypi-dml0:9000/rajabi/web_Graph.txt')
    data = data.withColumnRenamed('_c0', 'node1').withColumnRenamed('_c1', 'node2')

    out_degrees = data.groupby('node1').count()
    out_degrees = out_degrees.withColumnRenamed('count', 'out_degree').withColumnRenamed('node1', 'node')
    out_degrees.orderBy('out_degree', ascending=False).show(10)

    in_degrees = data.groupby('node2').count()
    in_degrees = in_degrees.withColumnRenamed('count', 'in_degree').withColumnRenamed('node2', 'node')
    in_degrees.orderBy('in_degree', ascending=False).show(10)

    degrees = in_degrees.join(out_degrees, 'node')
    degrees = degrees.withColumn('degree', col('in_degree') + col('out_degree'))
    degrees.orderBy('degree', ascending=False).show(10)

    visited = {k[0]: False for k in data.collect()}
    visited.update({k[1]: False for k in data.collect()})

    adjacents = data.groupby('node1').agg(collect_list('node2').alias('adjacents')).toPandas().set_index(
        'node1').T.to_dict()
    visited = dataframe_DFS('0', visited)

    for k, v in visited.items():
        if not v:
            print('not connected')
            break

    network = nx.from_pandas_edgelist(data.toPandas(), source='node1', target='node2')
    connected_components = [c for c in sorted(list(nx.connected_components(network)), key=len, reverse=True)]
    print([len(c) for c in connected_components][:2])

    degrees.filter(degrees.in_degree == degrees.out_degree).count()
    degrees.filter(degrees.degree % 2 == 1).count()

    adjacents = data.groupby('node1').agg(collect_list('node2').alias('adjacents')).toPandas().set_index(
        'node1').T.to_dict()
    triangles = []
    for u, adj_list in adjacents.items():

        if adj_list:
            for v in adj_list.get('adjacents'):
                adj_adj_list = adjacents.get(v)
                if adj_adj_list:
                    for w in adj_adj_list.get('adjacents'):
                        third_adjs = adjacents.get(w)
                        if u != w and third_adjs and v in third_adjs.get('adjacents'):
                            triangles.append((u, v, w))

    print(triangles)

    ######################################################## SQL
    data.createOrReplaceTempView("data")
    out_degrees = sparkSession.sql("select node1 as node, count(*) as out_degree \
                                  from data group by node1 order by out_degree desc")
    out_degrees.show()

    in_degrees = sparkSession.sql("select node2 as node, count(*) as in_degree \
                                  from data group by node2 order by in_degree desc")
    in_degrees.show()

    in_degrees.createOrReplaceTempView('in_degrees')
    out_degrees.createOrReplaceTempView('out_degrees')

    degrees = sparkSession.sql("select in_degrees.node as node, (in_degree + out_degree) as degree from \
                               in_degrees join out_degrees on in_degrees.node = out_degrees.node\
                               order by degree desc")
    degrees.show()

    degrees.createOrReplaceTempView('degrees')
    sparkSession.sql("""
            select count(*) from in_degrees join out_degrees on in_degrees.node = out_degrees.node
            where in_degrees.in_degree != out_degrees.out_degree
    """).show()
    sparkSession.sql("""
            select count(*) from degrees where degrees.degree % 2 = 1
    """).show()

    triangles = sparkSession.sql("""
            with third_pair (node1, node2, node3) as (
                select data1.node1, data1.node2, data2.node2 
                    from data as data1 join data as data2 on 
                    data1.node2 = data2.node1 and data1.node1 != data2.node2
            )
            select third_pair.node1, third_pair.node2, third_pair.node3 from 
            third_pair join data on third_pair.node3 = data.node1 and third_pair.node1 = data.node2
    """)
    triangles.show()









