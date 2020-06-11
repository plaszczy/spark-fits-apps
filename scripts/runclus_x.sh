#!/bin/bash

declare -i nargs
nargs=$#

if [ $nargs -lt 3 ]; then
echo "##################################################################################"
echo "usage: "
echo "./${0##*/} tmin tmax Nbins "
echo "##################################################################################"
exit
fi

#optional

if [ -z "${ncores_tot}" ] ; then
echo "sparkopts!"
exit
fi
part=$((${ncores_tot}))


export INPUT="/lsst/tomo10M.parquet"
export SLURM_JOB_NUM_NODES=${n_executors}

spark-shell $SPARKOPTS --jars $JARS --conf spark.driver.args="$1 $2 $3" -I hpgrid.scala -I Timer.scala -i corr_x.scala
