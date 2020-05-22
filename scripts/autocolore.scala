import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

//args from --conf spark.driver.args="10"
//compute nside from sep=args(0)
val args = sc.getConf.get("spark.driver.args").split("\\s+")
val sepcut=args(0).toDouble
val zmin=1.0
val zmax=args(1).toDouble

println(s"sep=$sepcut arcmin -> nside=$nside")
println(s"readshift shell [$zmin,$zmax]")

//data source
val df_all=spark.read.format("fits").option("hdu",1).load("/global/cscratch1/sd/plaszczy/LSST10Y").select($"RA",$"DEC",$"Z_COSMO"+$"DZ_RSD" as "z")

val input=df_all.filter($"z".between(zmin,zmax)).drop("z")


val timer=new Timer
val start=timer.time



//SOURCE
//add id
var source=input.withColumn("id",F.monotonicallyIncreasingId).drop("z")

//TARGET
var target=source.withColumnRenamed("id","id2")


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
matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("d",F.asin(F.sqrt($"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")/2)*2).withColumn("r",F.degrees($"d")*60).drop("dx","dy","dz","d")

//flat sky
//matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("d",F.sqrt($"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")).withColumn("r",F.degrees($"d")*60).drop("dx","dy","dz","d")

//hypot
//matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("d",F.asin(F.hypot($"dx",F.hypot($"dy",$"dz"))/2)*2).withColumn("r",F.degrees($"d")*60).drop("dx","dy","dz","d")


//cut at sepcut
matched=matched.filter($"r"<sepcut).drop("r").persist(StorageLevel.MEMORY_AND_DISK)

println("==> joining on ipix: "+matched.columns.mkString(", "))
val nmatch=matched.count()
println(f"#pair-associations=${nmatch/1e6}%3.2f M")
matched.printSchema


timer.step
timer.print("join")

val nodes=System.getenv("SLURM_JOB_NUM_NODES")

//only non 0 degrees
/*
//release mem
dup.unpersist
target.unpersist

val deg=matched.groupBy("id").count
println(deg.cache.count)

timer.step
timer.print("groupBy")

val sumDeg=deg.agg(F.sum("count")).first.getLong(0)
val meanDeg=sumDeg.toDouble/Ns
println(s"Degree: sum=$sumDeg avg=$meanDeg")

println("|||sep,nside,zmin,zmax,Ns,nmatch,sumdeg,nodes,meandeg,tmin")
println(f"||$sepcut,$nside,$zmin,$zmax,$Ns,$nmatch,$sumDeg,$meanDeg%5.3f,$nodes,$fulltime%.2f")
 */
val fulltime=(timer.time-start)*1e-9/60
println(s"TOT TIME=${fulltime} mins")


println("|||sep,nside,zmin,zmax,Ns,nmatch,nodes,tmin")
println(f"||$sepcut,$nside,$zmin,$zmax,$Ns,$nmatch,$nodes,$fulltime%.2f")

System.exit(0)
