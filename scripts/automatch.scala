
//max (plane) radius in arcmin
val rcut=10.0

val nside=256

:load cross-tools.scala


val timer=new Timer
val start=timer.time



//SOURCE
val input=spark.read.parquet("deg.parquet")
var source=input

//TARGET
var target=input.withColumnRenamed("id","id2").drop("degtrue")


/*--------------------------------------------------------*/

//add theta-phi and healpixels
source=source.withColumn("theta_s",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi_s",F.radians("ra"))
source=source.withColumn("ipix",Ang2pix($"theta_s",$"phi_s")).drop("ra","dec")

println("*** caching source: "+source.columns.mkString(", "))
val Ns=source.cache.count
println(f"input size=${Ns/1e6}%3.2f M")

timer.step
timer.print("source cache")

//ADD DUPLICATES
val dfn=source.withColumn("neighbours",pix_neighbours($"ipix"))

val dups=new Array[org.apache.spark.sql.DataFrame](9)

for (i <- 0 to 7) {
  println(i)
  val df1=dfn.drop("ipix").withColumn("ipix",$"neighbours"(i))
  val dfclean1=df1.filter(not(df1("ipix")===F.lit(-1))).drop("neighbours")
  dups(i)=dfclean1
}
dups(8)=source

val dup=dups.reduceLeft(_.union(_))

println("*** caching source+duplicates: "+dup.columns.mkString(", "))
val Ndup=dup.cache().count
println(f"source+duplicates size=${Ndup/1e6}%3.2f M")

timer.step
timer.print("duplicates cache")

/************************/
//TARGET=cosmodc2

//add healpixels
target=target.withColumn("theta_t",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi_t",F.radians("ra"))
target=target.withColumn("ipix",Ang2pix($"theta_t",$"phi_t")).drop("ra","dec")

println("*** caching target: "+target.columns.mkString(", "))
val Nt=target.cache().count
println(f"target size=${Nt/1e6}%3.2f M")

timer.step
timer.print("target cache")

///////////////////////////////////////////
//PAIRS
//join by ipix: tous les candidats paires
var matched=dup.join(target,"ipix").drop("ipix")

//filter same id
matched=matched.filter('id=!='id2)

//add distance column
//matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("d",F.sqrt($"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")).drop("dx","dy","dz").withColumn("r",F.degrees($"d")*60).drop("d")
matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("d",F.asin(F.sqrt($"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")/2)*2).withColumn("r",F.degrees($"d")*60).drop("dx","dy","dz,d")

//cut at rcut
matched=matched.filter($"r"<rcut)

println("==> joining on ipix: "+matched.columns.mkString(", "))
val nmatch=matched.cache.count()
println(f"#pair-associations=${nmatch/1e6}%3.2f M")

//release mem
dup.unpersist
target.unpersist

timer.step
timer.print("join")

//only non 0 degrees
val deg=matched.groupBy("id").count
println(deg.cache.count)

timer.step
timer.print("groupBy")

val fulltime=(timer.time-start)*1e-9/60
println(f"TOT TIME=${fulltime}")

val df=source.join(deg,"id")
df.withColumn("diff",$"count"-$"count").describe("diff").show


val sumDegIn=source.agg(F.sum("degtrue")).first.getLong(0)
val sumDegOut=deg.agg(F.sum("count")).first.getLong(0)
//attention meandegree=sumDegOut.toFloat/Ns
println(s"Sum degree=$sumDegIn vs. $sumDegOut")
require(sumDegOut==sumDegIn)
