import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.types._
import java.io._


def df_minmax(df:DataFrame,col:String)=df.select(F.min(col),F.max(col)).first()

def df_savetxt(df:DataFrame,file:String="df.txt")={
  val A=df.collect
  val append=false
  val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,append)))
  for (r <- A) {
    val s=r.toSeq.mkString("\t")
    writer.write(s+"\n")
  }

  writer.close
  println(file+ " written")
}


def df_hist(df_in:DataFrame,col:String,bounds: Option[(Double,Double)]=None, Nbins:Int=50,doStat:Boolean=true,fn:String="df.txt")={

  //drop nans if any
  val df=df_in.select(col).na.drop()

  if (doStat) df.describe(col).map(r=>(r.getString(0),r.getString(1))).collect.foreach(x=>println(x._1+"="+x._2))

  val (zmin,zmax) = bounds match {
    case Some(b) => b
    case None=>  val r=df_minmax(df_in,col); (r.toSeq(0).asInstanceOf[Double],r.toSeq(1).asInstanceOf[Double])
  }

  val dff=df.filter(df(col).between(zmin,zmax))
  val dz=(zmax-zmin)/(Nbins)
  
  val zbin=dff.select(F.col(col),(((dff(col)-lit(zmin+dz/2)))/dz).cast(IntegerType).as("bin"))

  val h=zbin.groupBy("bin").count.sort("bin")
  val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("loc"),$"count").drop("bin")
  
  df_savetxt(p,fn)

  p

}

