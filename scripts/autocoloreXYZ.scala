import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._

import java.util.Locale
import scala.math.{sin,toRadians,log}

// spark3D implicits
import com.astrolabsoftware.spark3d._

Locale.setDefault(Locale.US)

//args from --conf spark.driver.args="10"
//compute nside from sep=args(0)
val args = sc.getConf.get("spark.driver.args").split("\\s+")
val sepcut:Double=args(0).toDouble
val thetacut:Double=toRadians(sepcut/60)
val rcut:Double=2*sin(thetacut/2)
val r2cut:Double=rcut*rcut

val sep0:Double = 2.5
val r0=2*sin(toRadians(sep0/60)/2)
val r02=r0*r0
val lr0=log(r0)
val binSize:Double = 0.24236578127764186

val zmin:Double=1.0
val zmax:Double=args(1).toDouble

val numPart:Int=args(2).toInt

val nodes=System.getenv("SLURM_JOB_NUM_NODES")


println(s"sep=$sepcut arcmin -> nside=$nside")
println(s"readshift shell [$zmin,$zmax]")

//input
/*
//fits+cut
val df_all=spark.read.format("fits").option("hdu",1).load(System.getenv("FITSSOURCE")).select($"RA",$"DEC",$"Z_COSMO"+$"DZ_RSD" as "z")

val input=df_all.filter($"z".between(zmin,zmax)).drop("z")
 */
//parquet
val input=spark.read.parquet(System.getenv("INPUT")).drop("ipix","z")

val timer=new Timer
val start=timer.time

//add id+ipix
val source=input.withColumn("id",F.monotonicallyIncreasingId)
  .withColumn("theta_s",F.radians(F.lit(90)-F.col("DEC")))
  .withColumn("phi_s",F.radians("RA"))
  .withColumn("ipix",Ang2pix($"theta_s",$"phi_s"))
  .drop("RA","DEC")
  .withColumn("x_s",F.sin($"theta_s")*F.cos($"phi_s"))
  .withColumn("y_s",F.sin($"theta_s")*F.sin($"phi_s"))
  .withColumn("z_s",F.cos($"theta_s"))
  .drop("theta_s","phi_s")
//  .repartitionByCol("ipix",preLabeled=false, numPartitions=12*nside*nside)
//  .coalesce(numPart)
  .cache()


println("*** caching source: "+source.columns.mkString(", "))
val Ns=source.count
println(f"input size=${Ns/1e6}%3.2f M")
timer.step
timer.print("source cache")

val np1=source.rdd.getNumPartitions
println("source partitions="+np1)

//DUPLICATES
val dfn=source.withColumn("neighbours",pix_neighbours($"ipix"))

val dups=new Array[org.apache.spark.sql.DataFrame](9)

for (i <- 0 to 7) {
  println(i)
  val df1=dfn.drop("ipix").withColumn("ipix",$"neighbours"(i))
  val dfclean1=df1.filter(not(df1("ipix")===F.lit(-1))).drop("neighbours")
  dups(i)=dfclean1
}
val cols=dups(0).columns
dups(8)=source.select(cols.head,cols.tail:_*)

val dup=dups.reduceLeft(_.union(_))
  .withColumnRenamed("id","id2")
  .withColumnRenamed("x_s","x_t")
  .withColumnRenamed("y_s","y_t")
  .withColumnRenamed("z_s","z_t")
  .cache

println("*** caching duplicates: "+dup.columns.mkString(", "))
val Ndup=dup.count
println(f"duplicates size=${Ndup/1e6}%3.2f M")

timer.step
timer.print("dup cache")
val np2=dup.rdd.getNumPartitions
println("dup partitions="+np2)

///////////////////////////////////////////
//PAIRS
//join by ipix: tous les candidats paires
val pairs=source.join(dup,"ipix").drop("ipix").filter('id=!='id2)

//cut on cart distance+bin
val edges=pairs
  .withColumn("dx",$"x_s"-$"x_t")
  .withColumn("dy",$"y_s"-$"y_t")
  .withColumn("dz",$"z_s"-$"z_t")
  .withColumn("r2",$"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")
  .filter(F.col("r2").between(r02,r2cut))
  .withColumn("logr",F.log($"r2")/2.0)
  .withColumn("ibin",(($"logr"-lr0)/binSize).cast(IntegerType))
  .drop("dx","dy","dz","x_t","x_s","y_s","y_t","z_s","z_t","r2","logr","id","id2")
//  .persist(StorageLevel.MEMORY_AND_DISK)

edges.printSchema

println("==> joining on ipix: "+edges.columns.mkString(", "))
val nedges=edges.count()
println(f"#edges=${nedges/1e6}%3.2f M")

val tjoin=timer.step
timer.print("join")

/*
val deg=edges.groupBy("id").count
dup.unpersist

println("waiting for deg...")
println(deg.cache.count)

val tjoin=timer.step
timer.print("join+groupBy")

val sumDeg=deg.agg(F.sum("count")).first.getLong(0)
val meanDeg=sumDeg.toDouble/Ns
println(s"Degree: sum=$sumDeg avg=$meanDeg")
val nedges=sumDeg
 */


//binning
val binned=edges.groupBy("ibin").count().sort("ibin")
binned.show

timer.step
timer.print("binning")


val fulltime=(timer.time-start)*1e-9/60
println(s"TOT TIME=${fulltime} mins")

println("<>sep,nside,zmin,zmax,Ns,nedges,nodes,np1,np2,tjoin,tmin")
println(f"@$sepcut,$nside,$zmin,$zmax,$Ns,$nedges,$nodes,$np1,$np2,$tjoin%.1f,$fulltime%.2f")

System.exit(0)
