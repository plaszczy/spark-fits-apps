#!/bin/bash

#init
source /global/homes/p/plaszczy/Spark/spark-fits-apps/cori/init_spark.sh
#


## SBT Version
SBT_VERSION=2.11.8
SBT_VERSION_SPARK=2.11

## Package version
VERSION=0.1.0

# External JARS
SF=lib/spark-fits_2.11-0.6.0.jar
HP=lib/jhealpix.jar
export fitsdir="/global/cscratch1/sd/plaszczy/LSST10Y"

# Parameters
nside=512
loop=10


shifter spark-submit\
        --jars ${SF},${HP} \
        --class com.apps.healpix.HealpixProjection \
        target/scala-${SBT_VERSION_SPARK}/healpixprojection_${SBT_VERSION_SPARK}-${VERSION}.jar \
        $fitsdir $nside 0 $loop


stop-all.sh
