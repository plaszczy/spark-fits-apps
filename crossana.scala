
:load df_tools.scala

//dphi
df_hist(df1.withColumn("dphi",F.degrees($"phi_s"-$"phi_t")*3600),"dphi",Some(-5.0,5.0),Nbins=1000,fn="dphi.txt")

df_hist(df1.withColumn("dphipull",F.degrees($"phi_s"-$"phi_t")*3600/$"sigr"),"dphipull",Some(-15.0,15.0),Nbins=1000,fn="phipull.txt")

//dtheta
df_hist(df1.withColumn("dtet",F.degrees($"theta_s"-$"theta_t")*3600),"dtet",Some(-5.0,5.0),Nbins=1000,fn="dtet.txt")

df_hist(df1.withColumn("dtetpull",F.degrees($"theta_s"-$"theta_t")*3600/$"sigr"),"dtetpull",Some(-15.0,15.0),Nbins=1000,fn="tetpull.txt")



//mags

df_hist(df1.withColumn("dmag",$"mag_i_cModel"-$"mag_i"),"dmag",fn="dmag.txt",Nbins=1000)
df_hist(df1.withColumn("pmag",($"mag_i_cModel"-$"mag_i")/$"magerr_i_cModel"),"pmag",Some(-300,150),Nbins=1000,fn="pullmag.txt")


//flux
val df2=df1.withColumn("flux_i",F.pow(10.0,-($"mag_i"-31.4)/2.5)).withColumn("dflux",$"cModelFlux_i"-$"flux_i",fn="dflux.txt")

df_hist(df2,"dflux",Some(-50000,50000),Nbins=1000)

df_hist(df2.withColumn("pullf",$"dflux"/$"cModelFluxErr_i"),"pullf",Some(-50,50),Nbins=1000,fn="pullflux.txt")



