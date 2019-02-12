#!/usr/bin/env python
# coding: utf-8

# # Building systematics maps for 3x2pt with Spark
# 
# <br>Kernel: desc-pyspark
# <br>Owner: **S Plaszczynski** 
# <br>Last Verified to Run: **2019-01-10**
# 
# The goal of this notebook is to show how to build (simply) Healpix maps from the DC2 
# DPDD output inorder to test for possible 3x2pt systematics.
# It is illustrated on the current run1.2p production.
# It also shows how Spark can be used for data analysis (for more details see: https://arxiv.org/abs/1807.03078)
# Note that the full power of Spark will reveal when more data will be available.
# 
# The advantages of using Spark are:
# - one can put the relevant variables in cache
# - computation automatically optimised (lazy evaluation)
# - the analysis will scale when more data will be available
# - Spark is available at NERSC (as this notebook shows). jupyter-dev is limited to running on 4 threads ie 8GB mem. For (much) more memory use the interactive or batch mode, see https://github.com/LSSTDESC/desc-spark
# 

# # reading the data

# In[1]:


from pyspark.sql import SparkSession

# Initialise our Spark session
spark = SparkSession.builder.getOrCreate()
print("spark session started")

#usefull tool to benchmark
from time import time
class Timer:
    """
    a simple class for printing time (s) since last call
    """
    def __init__(self):
        self.t0=time()
    
    def start(self):
        self.t0=time()
        
    def stop(self):
        t1=time()
        print("{:2.1f}s".format(t1-self.t0))

timer=Timer()


# In[2]:


timer.start()
#df_all=spark.read.parquet("/global/cscratch1/sd/plaszczy/Run1.2p/object_catalog/full_catalog.parquet")
df_all=spark.read.parquet("/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog/dpdd_object_run1.2p.parquet")

df_all.printSchema()
timer.stop()


# select interesting columns (for this example we will only use the i band)

# In[3]:


# build selection by appending to string
cols=["ra","dec","good","clean","extendedness","blendedness","mag_i_cModel","magerr_i_cModel","snr_i_cModel","psf_fwhm_i","Ixx_i","Iyy_i","Ixy_i","IxxPSF_i","IyyPSF_i","IxyPSF_i"]
print(cols)
#use these columns
df=df_all.select(cols)


# Apply some quality cuts

# In[4]:


df=df.filter( (df.good==True)&(df.clean==True)&(df.extendedness>0.9)&(df.blendedness < 10**(-0.375))&(df.mag_i_cModel< 24.5)&(df.snr_i_cModel>10))


# Add a column of healpixels (mapReduce way)

# In[5]:


import pandas as pd
import numpy as np
import healpy as hp
from pyspark.sql.functions import pandas_udf, PandasUDFType

nside=2048
#create the ang2pix user-defined-function. 
#we use pandas_udf because they are more efficient
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))

#add a column of healpix indices
df=df.withColumn("ipix",Ang2Pix("ra","dec"))
#groupby indices and count the number of elements in each group
#df_map=df.groupBy("ipix").count()


# Drop all Nans and put in cache

# In[8]:


timer.start()
df=df.na.drop().cache()
print("sample has {}M objects".format(df.count()/1e6))
timer.stop()


# ## Mean counts

# In[7]:


timer.start()
#groupby indices and count the number of elements in each group
df_map=df.groupBy("ipix").count()
area=hp.nside2pixarea(nside, degrees=True)*3600
df_map=df_map.withColumn("dens",df_map['count']/area).drop("count")
#statistics per pixel
df_map.describe(['dens']).show() 
#back to python world
map_p=df_map.toPandas()
#now data is reduced create the healpy map
map_c = np.zeros(hp.nside2npix(nside))
map_c[map_p['ipix'].values]=map_p['dens'].values
#map_c[map_c==0]=hp.UNSEEN
timer.stop()


# In[19]:


get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
plt.set_cmap('jet')
hp.gnomview(map_c,rot=[55,-29.8],reso=hp.nside2resol(nside,arcmin=True),max=80,title='counts')


# ## Sky sigma

# In[20]:


var="magerr_i_cModel"
var_sys="avg("+var+")"
df_map=df.groupBy("ipix").mean(var)
df_map.describe([var_sys]).show() 
dfp=df_map.toPandas()
map_s = np.zeros(hp.nside2npix(nside))
map_s[dfp['ipix'].values]=dfp[var_sys].values
hp.gnomview(map_s,rot=[55,-29.8],reso=hp.nside2resol(nside,arcmin=True),title=var_sys)


# In[21]:


var='snr_i_cModel'
var_sys="avg("+var+")"
df_map=df.groupBy("ipix").mean(var)
df_map.describe([var_sys]).show() 
dfp=df_map.toPandas()
map_s = np.zeros(hp.nside2npix(nside))
map_s[dfp['ipix'].values]=dfp[var_sys].values
hp.gnomview(map_s,rot=[55,-29.8],reso=hp.nside2resol(nside,arcmin=True),min=10,max=500,title=var_sys)


# ## Mean seeing

# In[22]:


var="psf_fwhm_i"
var_sys="avg("+var+")"
df_map=df.groupBy("ipix").mean(var)
df_map.describe([var_sys]).show() 
dfp=df_map.toPandas()
map_s = np.zeros(hp.nside2npix(nside))
map_s[dfp['ipix'].values]=dfp[var_sys].values
hp.gnomview(map_s,rot=[55,-29.8],reso=hp.nside2resol(nside,arcmin=True),min=0.45,max=1.,title=var_sys)


# ## Ellipticities 
# 
# 
# compute distorsion (thanks to Javier). Note that we don't have redshifts
# 
# ### Signal

# In[61]:


from pyspark.sql import functions as F
Q11="IxxPSF_i"
Q22="IyyPSF_i"
Q12="IxyPSF_i"

# pre-compute denominator
df_shear=df.withColumn("denom",F.col(Q11)+F.col(Q22))
#read and img parts of shear
df_shear=df_shear.withColumn("R_E",(F.col(Q11)-F.col(Q22))/F.col('denom')).        withColumn("I_E",(2*F.col(Q12))/F.col('denom'))
# convert to amplitude and phase
df_shear=df_shear.withColumn("amp_E",F.hypot(F.col("R_E"),F.col("I_E"))).    withColumn("phase_E",F.atan2(F.col("R_E"),F.col("I_E")))
df_shear.select("R_E","I_E","amp_E","phase_E").show(5)


# In[63]:


var="amp_E"
var_sys="avg("+var+")"
df_map=df_shear.groupBy("ipix").mean(var)
df_map.describe([var_sys]).show() 
dfp=df_map.toPandas()
map_e = np.zeros(hp.nside2npix(nside))
map_e[dfp['ipix'].values]=dfp[var_sys].values
hp.gnomview(map_e,rot=[55,-29.8],reso=hp.nside2resol(nside,arcmin=True),title=var_sys)


# In[60]:


var="phase_E"
var_sys="avg("+var+")"
df_map=df_shear.groupBy("ipix").mean(var)
df_map.describe([var_sys]).show() 
dfp=df_map.toPandas()
map_e = np.zeros(hp.nside2npix(nside))
map_e[dfp['ipix'].values]=dfp[var_sys].values
hp.gnomview(map_e,rot=[55,-29.8],reso=hp.nside2resol(nside,arcmin=True),title=var_sys)


# # Missing quantities

# - redshift 
# - airmass
# - HSM _e1/e2 (ext_shapeHSM_HsmShapeRegauss_e1 and ext_shapeHSM_HsmShapeRegauss_e2 availbale in GCR as native_quantities but not DPDD)
# - ?

# In[ ]:




