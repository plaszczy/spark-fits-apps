
#hitso mags
for (b,c) in zip(bands,colors):
    h,step=df_hist(df,"mag_{}".format(b),bounds=(15,40))
    #plt.bar(h['loc'],h['count'],step,label=b,color='white',edgecolor=c)
    plt.plot(h['loc'],h['count'],color=c,label=b)
    
plt.legend()

#magnification
df_lens=df.withColumn("mag_lens-mag_true",df.mag_u-df.mag_true_u)
df_histplot(df_lens,"mag_lens-mag_true",bounds=(-0.4,0.5),Nbins=80,doStat=True)


#strong lensing?
df_lens.filter(df_lens["mag_lens-mag_true"]<-2.5).show()

-------------+---------+----------+---------+----------+---------+---------+---------+---------+---------+---------+--------+-----------------+
|      halo_id|       ra|       dec| redshift|mag_true_u|    mag_u|    mag_g|    mag_r|    mag_i|    mag_z|    mag_y|    ipix|mag_lens-mag_true|
+-------------+---------+----------+---------+----------+---------+---------+---------+---------+---------+---------+--------+-----------------+
|3584900064124|50.133892| -31.16941| 2.884473| 37.799507|35.140182|31.563677|29.273592|28.362547| 28.11977| 27.92492|38188148|       -2.6593246|
|4950100064124|50.129242| -31.16934|2.8848314| 28.337717|25.433786|23.608252|23.211761|23.272312|23.374165|23.314266|38188148|       -2.9039307|

haloid=3584900064124,4950100064124

ipix=38188148

716  (600+-90 pour area=2.9 arcmin^2)


*4349300064121
