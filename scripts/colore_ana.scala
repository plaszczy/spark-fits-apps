import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val f="/home/plaszczy/fits/galbench_srcs_s1_0.fits"

var ana="load(HDU)"

val gal=spark.read.format("com.sparkfits").option("hdu",1).load(f).select($"RA",$"Dec",($"Z_COSMO"+$"DZ_RSD").as("z"))

gal.printSchema
ana="PZ + show(5)"
val galrec=gal.withColumn("zrec",(gal("z")+lit(0.03)*(lit(1)+gal("z"))*randn()).cast(FloatType))
galrec.show

//cache
val N=galrec.cache.count
println("Ntot="+N)

ana="minmax"
val minmax=gal.select(min("z"),max("z")).first()
val zmin=minmax(0).toString.toDouble
val zmax=minmax(1).toString.toDouble
val Nbins=100
val dz=(zmax-zmin)/Nbins

ana="histo"
val zbin=galrec.select($"z",((gal("z")-lit(zmin-dz/2)/dz)).cast(IntegerType).as("bin"))
val h=zbin.groupBy("bin").count.orderBy(asc("bin"))
val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("zbin"),$"count").drop("bin")
//save to csv
p.write.csv("histo.csv")
p.write.json("histo.json")

