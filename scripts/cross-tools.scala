import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row

import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.NESTED

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

//define nside!


//Timer
class Timer (var t0:Double=System.nanoTime().toDouble,   var dt:Double=0)  {

  def time:Double=System.nanoTime().toDouble

  def step:Double={
    val t1 = System.nanoTime().toDouble
    dt=(t1-t0)/1e9
    t0=t1
    dt
  }
  def print(msg:String):Unit={
    val sep="----------------------------------------"
    println("\n"+msg+": "+dt+" s\n"+sep)
  }
}

//Healpix
class ExtPointing extends Pointing with java.io.Serializable
//val nside=262144
val nside=131072
//val nside=32


print(s"Using NSIDE=$nside")


val hp = new HealpixBase(nside, NESTED)

case class HealpixGrid(hp : HealpixBase, ptg : ExtPointing) {
  def index(theta : Double, phi : Double) : Long = {
    ptg.theta = theta
    ptg.phi = phi
    hp.ang2pix(ptg)
  }
  def neighbours(ipix:Long):Array[Long] =  {
    hp.neighbours(ipix)
  }

}

val grid = HealpixGrid(hp, new ExtPointing)
val Ang2pix=spark.udf.register("Ang2pix",(theta:Double,phi:Double)=>grid.index(theta,phi))

val pix_neighbours=spark.udf.register("pix_neighbours",(ipix:Long)=>grid.neighbours(ipix))

/************************/

def single_match(df_src:DataFrame,df_target:DataFrame,rcut:Double=1.0,ra_src:String="ra",dec_src:String="dec",ra_t:String="ra",dec_t:String="dec"):DataFrame = {
/*  cross match2 catalogs havinf ra/dec's coordinates with r<rcut. Uses Healpix (check nside if rcut change)
    - do not put in cache
    - df_src witll be duplicated
    - df_src first colummn should be a unique id

 */
  val sourceId=df_src.columns(0)

  val timer=new Timer
  val start_time=timer.time

  //add theta-phi and healpixels
  var df=df_src.withColumn("theta_s",F.radians(F.lit(90)-F.col(dec_src))).withColumn("phi_s",F.radians(ra_src))
  val source=df.withColumn("ipix",Ang2pix($"theta_s",$"phi_s")).drop(ra_src,dec_src)

  println("*** caching source: "+df.columns.mkString(", "))
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
  //TARGET
  //add healpixels
  df=df_target.withColumn("theta_t",F.radians(F.lit(90)-F.col(dec_t))).withColumn("phi_t",F.radians(ra_t))
  val target=df.withColumn("ipix",Ang2pix($"theta_t",$"phi_t")).drop(ra_t,dec_t)

  println("*** caching target: "+target.columns.mkString(", "))
  val Nt=target.cache().count
  println(f"target size=${Nt/1e6}%3.2f M")

  timer.step
  timer.print("target cache")

///////////////////////////////////////////
//PAIRS
//join by ipix: tous les candidats paires
  var matched=dup.join(target,"ipix").drop("ipix")

  //release mem
  dup.unpersist
  target.unpersist

  //add distance column
  matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("d",F.sqrt($"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")).drop("dx","dy","dz").withColumn("r",F.degrees($"d")*3600).drop("d")

  //no need to associate above rcut
  matched=matched.filter($"r"<rcut)


  println("==> joining on ipix: "+matched.columns.mkString(", "))
  val nmatch=matched.cache.count()
  println(f"#pair-associations=${nmatch/1e6}%3.2f M")

  timer.step
  timer.print("join")


  // pair RDD
  //create a map to retrieve position
  val idx=matched.columns.map(s=>(s,matched.columns.indexOf(s))).toMap


  //reduce by min(r) and accumlate counts
  val ir:Int=idx("r")
  val rdd=matched.rdd.map{ r=>(r.getLong(idx(sourceId)),(r,1L)) }
  val ass_rdd=rdd.reduceByKey { case ( (r1,c1),(r2,c2)) => (if (r1.getDouble(ir)<r2.getDouble(ir)) r1 else r2 ,c1+c2) }
  val ass=spark.createDataFrame(ass_rdd.map{ case (k,(r,c))=> Row.fromSeq(r.toSeq++Seq(c)) },matched.schema.add(StructField("nass",LongType,true)))

  println("*** reducebykey id_source")
  val nc=ass.cache.count

  timer.step
  timer.print("reducebykey")


  matched.unpersist

//stat on # associations: unnecessary
//ass.groupBy("nass").count.withColumn("frac",$"count"/nc).sort("nass").show

  // ncand=1
  var df1=ass.filter($"nass"===F.lit(1)).drop("nass")

  println("*** caching df1: "+df1.columns.mkString(", "))
  val n1=df1.cache.count
  df1.printSchema

  println(f"|| ${Ns/1e6}%3.2f || ${nc/1e6}%3.2f (${nc.toFloat/Ns*100}%.1f%%) || ${n1/1e6}%3.2f (${n1.toFloat/nc*100}%.1f%%)||")

  timer.step
  timer.print("completed")

  val tot_time=(timer.time-start_time)*1e-9/60
  println(f"TOT TIME=${tot_time}%.1f mins")

  df1
}
