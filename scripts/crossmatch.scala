
//:load scripts/cross-tools.scala

val timer=new Timer
val start=timer.time


//min radius arcsec
val rcut=1.0

//common cuts
val magcut=25.3


//SOURCE
var source=spark.read.parquet(System.getenv("RUN2"))
val col_s=Array("objectId","ra","dec","mag_i_cModel","psf_fwhm_i","magerr_i_cModel","cModelFlux_i","cModelFluxErr_i","clean","snr_i_cModel","blendedness","extendedness")
val sourceId=col_s(0)
source=source.select(col_s.head,col_s.tail: _*).na.drop
//cut
source=source.filter($"mag_i_cModel"<magcut)
source.printSchema

//TARGET
var target=spark.read.parquet(System.getenv("COSMODC2"))
// select columns
val col_t=Array("galaxy_id","ra","dec","mag_i")
target=target.select(col_t.head,col_t.tail: _*).na.drop
//cut
target=target.filter($"mag_i"<magcut)
target.printSchema

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

//add distance column
matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("d",F.sqrt($"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")).drop("dx","dy","dz").withColumn("r",F.degrees($"d")*3600).drop("d")

//no need to associate above rcut
matched=matched.filter($"r"<rcut)

println("==> joining on ipix: "+matched.columns.mkString(", "))
val nmatch=matched.cache.count()
println(f"#pair-associations=${nmatch/1e6}%3.2f M")

//release mem
dup.unpersist
target.unpersist

timer.step
timer.print("join")


// pair RDD
//create a map to retrieve position
val idx=matched.columns.map(s=>(s,matched.columns.indexOf(s))).toMap

/*
//build paiRDD based on key objectId
val rdd=matched.rdd.map(r=>(r.getLong(idx(sourceId)),r))
val ir:Int=idx("r")
val ass_rdd=rdd.reduceByKey((r1,r2)=> if (r1.getDouble(ir)<r2.getDouble(ir)) r1 else r2)
var ass=spark.createDataFrame(ass_rdd.map(x=>x._2),matched.schema)

//filter 
println("*** reducebykey id_source")
val nc=ass.cache.count

//count #cands
val cands=matched.groupBy(sourceId).count
print(cands.cache.count)

matched.unpersist
val df_ass=ass.join(cands,sourceId)
print(df_ass.cache.count)
 */

//reduce by min(r) and accumlate counts
val ir:Int=idx("r")
val rdd=matched.rdd.map{ r=>(r.getLong(idx(sourceId)),(r,1L)) } 
val ass_rdd=rdd.reduceByKey { case ( (r1,c1),(r2,c2)) => (if (r1.getDouble(ir)<r2.getDouble(ir)) r1 else r2 ,c1+c2) }
val df_ass=spark.createDataFrame(ass_rdd.map{ case (k,(r,c))=> Row.fromSeq(r.toSeq++Seq(c)) },matched.schema.add(StructField("nass",LongType,true)))

val nc=df_ass.cache.count

//stat on # associations: unnecessary
//ass.groupBy("nass").count.withColumn("frac",$"count"/nc).sort("nass").show


// ncand=1
var df1=df_ass.filter($"nass"===F.lit(1)).drop("nass")

println("*** caching df1: "+df1.columns.mkString(", "))
val n1=df1.cache.count
df1.printSchema

println(f"||i<${magcut}|| ${Ns/1e6}%3.2f || ${nc/1e6}%3.2f (${nc.toFloat/Ns*100}%.1f%%) || ${n1/1e6}%3.2f (${n1.toFloat/nc*100}%.1f%%)||")

timer.step
timer.print("completed")

val fulltime=(timer.time-start)*1e-9/60
println(f"TOT TIME=${fulltime}")


/*
//assoc statitics
var df_src=source.filter( ($"clean"===1) && ($"extendedness"===1))
var df_ass=ass.filter( ($"clean"===1) && ($"extendedness"===1))

var N=df_src.count
var Na=df_ass.count
var N1=df_ass.filter($"nass"===F.lit(1)).count

println(f"|| clean+extendedness || ${N/1e6}%3.2f || ${Na/1e6}%3.2f (${Na.toFloat/N*100}%.1f%%) || ${N1/1e6}%3.2f (${N1.toFloat/Na*100}%.1f%%)||")

df_src=df_src.filter($"snr_i_cModel">5)
df_ass=df_ass.filter($"snr_i_cModel">5)
N=df_src.count
Na=df_ass.count
N1=df_ass.filter($"nass"===F.lit(1)).count
println(f"|| SNR>5 || ${N/1e6}%3.2f || ${Na/1e6}%3.2f (${Na.toFloat/N*100}%.1f%%) || ${N1/1e6}%3.2f (${N1.toFloat/Na*100}%.1f%%)||")

df_src=df_src.filter($"snr_i_cModel">10)
df_ass=df_ass.filter($"snr_i_cModel">10)
N=df_src.count
Na=df_ass.count
N1=df_ass.filter($"nass"===F.lit(1)).count
println(f"|| SNR>10 || ${N/1e6}%3.2f || ${Na/1e6}%3.2f (${Na.toFloat/N*100}%.1f%%) || ${N1/1e6}%3.2f (${N1.toFloat/Na*100}%.1f%%)||")
 */
