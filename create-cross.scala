/////////////////////////////////////
//stars X run2
//stars
val ref=spark.read.parquet("/lsst/DC2/refcat_v3_dc2_r2p1i.parquet")

//agn=ref.filter(ref.isagn==True)
//gal=ref.filter((ref.isagn==False)&(ref.isresolved==True))

val stars=ref.filter($"isresolved"===false).select("id","ra","dec","i_smeared","r_smeared")

//run2 4stars
val magcut=23.0
var obj=spark.read.parquet(System.getenv("RUN22"))
//cut stars (pas sur cmodel)
val run2=obj.filter(($"psFlux_flag_i"===false)&&($"psFlux_flag_r"===false)).filter(($"good"===true)&&($"clean"===true)).select("objectId","ra","dec","mag_i","mag_r","magerr_i","magerr_r","extendedness","psFlux_i","psFluxErr_i","psFlux_r","psFluxErr_r").na.drop.filter($"mag_i"<magcut)

//mag_i est  bien la meme chose que:
//.withColumn("mag_i_psf",-2.5*F.log10($"psFlux_i")+31.4)


:load scripts/cross-tools.scala

val df1=single_match(stars,run2)
df1.write.mode("overwrite").parquet("/lsst/DC2/run22xstars.parquet")

///////////////////////////////////////////////////

//run2xCosmoDC2

// run2
val magcut=25.3
var run2=spark.read.parquet(System.getenv("RUN22"))

//filter gal
run2=run2.filter(($"good"===true)&&($"clean"===true)).filter($"mag_i_cModel"<magcut).filter($"snr_i_cModel">1).filter($"extendedness">0.5)


run2=run2.select("objectId","ra","dec","psf_fwhm_i","mag_i_cModel","magerr_i_cModel","snr_i_cModel","blendedness").na.drop

//cosmodc2 (no ultrafaint)
//var gal=spark.read.parquet(System.getenv("COSMODC2"))
var gal=spark.read.parquet("/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image_nofaint.parquet")

//loose cut
gal=gal.filter($"mag_i"<26)
// select columns
gal=gal.select("galaxy_id","ra","dec","mag_i")


:load scripts/cross-tools.scala

val df1=single_match(run2,gal)
df1.write.mode("overwrite").parquet("/lsst/DC2/run22xCdc2.parquet")
