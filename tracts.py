import pyspark.sql.functions as F

df_all=spark.read.parquet(os.environ['RUN22'])

df=df_all.select("tract","patch","ra","dec")

df.cache().count()

pairs=df.groupBy(["tract","patch"]).count()
p= pairs.groupBy("tract").count().withColumnRenamed("count","npatch")
p.count()
good=p.filter(p['npatch']==49).sort("tract")
good.count()
good.show(200)

g=good.select("tract").collect()
from numpy import *
a=array([gg[0] for gg in g])


bad=p.filter(p['npatch']!=49).sort("tract")
bad.show(200)
pairs.join(bad,"tract").groupBy("tract").agg(F.count("patch"),F.array_sort(F.collect_list("patch"))).sort("tract").show(200,truncate=False)

#geometry
geo=df.groupBy("tract").agg(F.avg("ra"),F.min("ra"),F.max("ra"),F.min("dec"),F.max("dec"),F.avg("dec"))

#join with npatch
dfj=geo.join(p,"tract")

#ckeck bads
p=dfj.toPandas()
plt.plot(p["avg(ra)"],p["avg(dec)"],'o')
bad=(p.npatch!=49)
plt.plot(p[bad]["avg(ra)"],p[bad]["avg(dec)"],'ro')
