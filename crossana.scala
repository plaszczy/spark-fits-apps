
:load df_tools.scala

//dphi
df_hist(df1.withColumn("dphi",F.degrees($"phi_s"-$"phi_t")*3600),"dphi",Some(-5.0,5.0),Nbins=1000,fn="dphi.txt")

df_hist(df1.withColumn("dphipull",F.degrees($"phi_s"-$"phi_t")*3600/$"sigr"),"dphipull",Some(-15.0,15.0),Nbins=1000,fn="phipull.txt")

//dtheta
df_hist(df1.withColumn("dtet",F.degrees($"theta_s"-$"theta_t")*3600),"dtet",Some(-5.0,5.0),Nbins=1000,fn="dtet.txt")

df_hist(df1.withColumn("dtetpull",F.degrees($"theta_s"-$"theta_t")*3600/$"sigr"),"dtetpull",Some(-15.0,15.0),Nbins=1000,fn="tetpull.txt")

//r
df_hist(df1,"r",Some(0,7),Nbins=1000,fn="r.txt")
df_hist(df1.withColumn("rnorm",$"r"/$"sigr"),"rnorm",Some(0,15),Nbins=1000,fn="pullr.txt")
df_hist(df1,"cumr",Some(0,1),Nbins=1000,fn="cumr.txt")

//mags
df_hist(df1.withColumn("dmag",$"mag_i_cModel"-$"mag_i"),"dmag",Some(-15,10),Nbins=1000,fn="dmag.txt")
df_hist(df1.withColumn("pmag",($"mag_i_cModel"-$"mag_i")/$"magerr_i_cModel"),"pmag",Some(-300,150),Nbins=1000,fn="pullmag.txt")

//flux
val df2=df1.withColumn("dflux",$"cModelFlux_i"-$"flux_i")
df_hist(df2,"dflux",Some(-50000,50000),Nbins=1000,fn="dflux.txt")
df_hist(df2.withColumn("pullf",$"dflux"/$"cModelFluxErr_i"),"pullf",Some(-50,50),Nbins=1000,fn="pullflux.txt")



