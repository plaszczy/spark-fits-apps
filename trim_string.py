import pyspark.sql.functions as F

df=spark.read.format("fits").option("hdu",1).load("refcat_v3_dc2_r2p1i.fits")


band=['u','g','r','i','z','y']

conv=['ra','dec','sigma_ra','sigma_dec','ra_smeared','dec_smeared']
for b in band:
    conv+=["{}".format(b),"{}_smeared".format(b),"sigma_{}".format(b),"{}_rms".format(b)]

#float conv
for c in conv:
    df=df.withColumnRenamed(c,"s")
    df=df.withColumn(c,F.substring_index(df.s, ',', 1).astype('float')).drop("s")

#bool conv
convbool=["isagn","isresolved"]
for c in convbool:
    df=df.withColumnRenamed(c,"s")
    df=df.withColumn(c,F.substring_index(df.s, ',', 1).astype('Boolean')).drop("s")

convint=["id"]
for c in convint:
    df=df.withColumnRenamed(c,"s")
    df=df.withColumn(c,F.substring_index(df.s, ',', 1).astype('Long')).drop("s")

df=df.drop("properMotionDec","parallax","radialVelocity",'properMotionRa')
