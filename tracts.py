import pyspark.sql.functions as F

df_all=spark.read.parquet(os.environ['RUN22'])

df=df_all.select("tract","patch")
df.cache().count()
pairs=df.groupBy(["tract","patch"]).count()
p= pairs.groupBy("tract").count() 
p.count()
good=p.filter(p['count']==49).withColumnRenamed("count","#patches").sort("tract")
good.count()
good.show(200)

g=good.select("tract").collect
from numpy import *
a=array([gg[0] for gg in g])



bad=p.filter(p['count']!=49).withColumnRenamed("count","#patches").sort("tract")
ibad.show(200)
pairs.join(bad,"tract").groupBy("tract").agg(F.count("patch"),F.array_sort(F.collect_list("patch"))).sort("tract").show(200,truncate=False)




