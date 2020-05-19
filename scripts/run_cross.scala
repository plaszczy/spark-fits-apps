
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row

// Logger info
import org.apache.log4j.Level
import org.apache.log4j.Logger


// Set to Level.WARN is you want verbosity
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)






//COMMON CUTS
val magcut=23.35

//GALCAT cosmodc2
var gal=spark.read.parquet(System.getenv("COSMODC2"))
val cols=Array("galaxy_id","ra","dec","mag_i","mag_r")

gal=gal.select(cols.head,cols.tail: _*).na.drop
//filter
gal=gal.filter($"mag_i"<magcut)


//STARS
var stars=spark.read.parquet("refcat_v3_dc2_r2p1i.parquet")
val cols=Array("id","ra","dec","ra_smeared","dec_smeared","i","i_smeared","r","r_smeared","isresolved","isagn")
stars=stars.select(cols.head,cols.tail: _*).na.drop
stars=stars.filter($"isresolved"===false)
stars=stars.filter($"i_smeared"<magcut)

stars=stars.filter($"ra".between(58.2,65.3))
stars=stars.filter($"dec".between(-40.7,-33.6))
stars=star.filter($"i_smeared">16)


//OBJECT CAT
var obj=spark.read.parquet(System.getenv("RUN2"))
val cols=Array("objectId","ra","dec","psf_fwhm_i","mag_i_cModel","magerr_i_cModel","cModelFlux_i","cModelFluxErr_i","mag_r_cModel","magerr_r_cModel","clean","snr_i_cModel","snr_r_cModel","blendedness","extendedness","psFlux_i","psFlux_flag_i","psFluxErr_i","psFlux_r","psFlux_flag_r","psFluxErr_r")

obj=obj.select(cols.head,cols.tail: _*).na.drop
//filter
obj=obj.filter($"mag_i_cModel"<magcut)

obj=obj.filter($"isresolved"===false)
obj=obj.filter($"ra".between(58.2,65.3))
obj=obj.filter($"dec".between(-40.7,-33.6))
obj=star.filter($"mag_i_cModel">16)


///:load scripts/cross-tools.scala
val df1=single_match(obj,gal)

//df1.write.parquet("df3.parquet")
