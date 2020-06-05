df=df.select("tract","patch")
pairs=df.groupBy(["tract","patch"]).count()
p= pairs.groupBy("tract").count() 
p.count()
good=p.filter(p['count']==49).withColumnRenamed("count","#patches").sort("tract")
good.count()
good.show(200)
bad=p.filter(p['count']!=49).withColumnRenamed("count","#patches").sort("tract")
ibad.show(200)
pairs.join(bad,"tract").sort("patch").groupBy("tract").agg(F.collect_list("patch")).show(200,truncate=False)




