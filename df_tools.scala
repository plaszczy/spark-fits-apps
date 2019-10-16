import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.types._
import java.io._


def df_minmax(df:DataFrame,col:String)=df.select(F.min(col),F.max(col)).first()

def df_hist(df_in:DataFrame,col:String,bounds:Array[Double],Nbins:Int=50)={

  //drop nans if any
  val df=df_in.select(col).na.drop()
  val zmin:Double=bounds(0)
  val zmax:Double=bounds(1)
  val dff=df.filter(df(col).between(zmin,zmax))
  val dz=(zmax-zmin)/(Nbins)
  
  val zbin=dff.select(F.col(col),(((dff(col)-lit(zmin+dz/2)))/dz).cast(IntegerType).as("bin"))

  val h=zbin.groupBy("bin").count.sort("bin")
  val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("loc"),$"count").drop("bin")
  
  p

}

def savetxt(df:DataFrame,file:String="df.txt")={
  val A=df.collect
  val append=false
  val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,append)))
  for (r <- A) {
    val s=r.toSeq.mkString("\t")
    //for (s <- buf)  writer.write(s+"\t")
    writer.write(s+"\n")
  }

  writer.close
  println(file+ " written")
}
