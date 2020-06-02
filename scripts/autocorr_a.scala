//spark-shell --jars jhealpix.jar -I hpgrid.scala -I Timer.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._

import java.util.Locale
import scala.math.{sin,toRadians,log,pow,floor}

// spark3D implicits
import com.astrolabsoftware.spark3d._

Locale.setDefault(Locale.US)

//args from --conf spark.driver.args="sepcut zmax numpart"
val args = sc.getConf.get("spark.driver.args").split("\\s+")

val binSize:Double = args(0).toDouble
val nside1:Int= args(1).toInt

val Nbins:Int= args(2).toInt
val nside2:Int= args(3).toInt

//en arcmin
val t=List.tabulate(Nbins+1)(i=>2.5+i*binSize)
val bins=t.sliding(2).toList
val tmin=t.head
val tmax=t.last

//in rads
val r=t.map(x=>toRadians(x/60))

val dr=r(1)-r(0)
val rmin=r.head
val rmax=r.last
val r2min=rmin*rmin
val r2max=rmax*rmax


//val zmin:Double=1.0

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

println("reducing data")

val finegrid = HealpixGrid(new HealpixBase(nside1, NESTED), new ExtPointing)
val Ang2Pix1024=spark.udf.register("Ang2Pix1024",(theta:Double,phi:Double)=>finegrid.index(theta,phi))
val Pix2Ang1024=spark.udf.register("Pix2Ang1024",(ipix:Long)=> finegrid.pix2ang(ipix))


import spark.implicits._
/////////////////////////////////////////
//1 reduce data
val pixmap=input.withColumn("theta",F.radians(F.lit(90)-F.col("DEC"))).withColumn("phi",F.radians("RA")).withColumn("bigpix",Ang2Pix1024($"theta",$"phi")).groupBy("bigpix").count()

val newinput=pixmap.withColumnRenamed("count","w")
  .withColumn("ptg",Pix2Ang1024($"bigpix"))
  .select($"ptg"(0) as "theta_s",$"ptg"(1) as "phi_s",$"w")

//println("Reduced data size="+newinput.count)
// automatic
//val i=floor(-log(thetacut)/log(2.0)).toInt
//val nside2=pow(2,i-1).toInt

val grid = HealpixGrid(new HealpixBase(nside2, NESTED), new ExtPointing)
def Ang2pix=spark.udf.register("Ang2pix",(theta:Double,phi:Double)=>grid.index(theta,phi))
def pix_neighbours=spark.udf.register("pix_neighbours",(ipix:Long)=>grid.neighbours(ipix))


val source=newinput.withColumn("id",F.monotonicallyIncreasingId)
  .withColumn("ipix",Ang2pix($"theta_s",$"phi_s"))
  .withColumn("x_s",F.sin($"theta_s")*F.cos($"phi_s"))
  .withColumn("y_s",F.sin($"theta_s")*F.sin($"phi_s"))
  .withColumn("z_s",F.cos($"theta_s"))
  .drop("theta_s","phi_s")
  .cache()


println("*** caching source: "+source.columns.mkString(", "))
val Ns=source.count
println(f"Source size=${Ns/1e6}%3.2f M")
val tsource=timer.step
timer.print("source cache")

val np1=source.rdd.getNumPartitions
println("source partitions="+np1)

//////////////////////////////////////////////
//2 build duplicates
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
  .withColumnRenamed("w","w2")
  .withColumnRenamed("id","id2")
  .withColumnRenamed("x_s","x_t")
  .withColumnRenamed("y_s","y_t")
  .withColumnRenamed("z_s","z_t")
  .cache

println("*** caching duplicates: "+dup.columns.mkString(", "))
val Ndup=dup.count
println(f"duplicates size=${Ndup/1e6}%3.2f M")

val tdup=timer.step
timer.print("dup cache")
val np2=dup.rdd.getNumPartitions
println("dup partitions="+np2)

///////////////////////////////////////////
//3 build PAIRS with cuts
val pairs=source.join(dup,"ipix").drop("ipix").filter('id=!='id2).drop("id","id2")

//cut on cart distance+bin
val edges=pairs
  .withColumn("dx",$"x_s"-$"x_t")
  .withColumn("dy",$"y_s"-$"y_t")
  .withColumn("dz",$"z_s"-$"z_t")
  .withColumn("r2",$"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")
  .filter(F.col("r2").between(r2min,r2max))
  .drop("dx","dy","dz","x_t","x_s","y_s","y_t","z_s","z_t")
  .withColumn("r",F.sqrt($"r2"))
  .withColumn("ibin",(($"r"-rmin)/dr).cast(IntegerType))
  .drop("r2","r")
  .withColumn("prod",$"w"*$"w2")
  .drop("w","w2")
//  .persist(StorageLevel.MEMORY_AND_DISK)

edges.printSchema

println("==> joining using nside2="+nside2+edges.columns.mkString(", "))
val nedges=edges.count()
println(f"#edges=${nedges/1e6}%3.2f M")

val tjoin=timer.step
timer.print("join")

///////////////////////////////////
//4 binning
//val bins=edges.groupBy("ibin").agg(F.sum($"w"*$"w2") as "Nbin")
val binned=edges.rdd.map(r=>(r.getInt(0),r.getLong(1))).reduceByKey(_+_).toDF("ibin","Nbin")

binned.show

val tbin=timer.step
timer.print("binning")

val fulltime=(timer.time-start)*1e-9/60
println(s"TOT TIME=${fulltime} mins")


val nodes=System.getenv("SLURM_JOB_NUM_NODES")


println("step,tmin,tmax,nside1,nside2,Ns,nedges,tmin")
println(f"@$binSize,tmin,tmax,$nside1,$nside2,$Ns,$nedges,$fulltime%.2f")
println(f"@#nodes=$nodes parts=($np1,$np2): source=${tsource.toInt}s dups=${tdup.toInt}s join=${tjoin.toInt}s bins=${tbin.toInt}, tot=$fulltime%.2f mins")


System.exit(0)
