#!/bin/bash

declare -i nargs
nargs=$#

if [ $nargs -lt 1 ]; then
echo "##################################################################################"
echo "usage: "
echo "./run_sparkdeg.sh sep "
echo "##################################################################################"
exit
fi

#optional

sep=$1
zmax=1.025

if [ -z "${ncores_tot}" ] ; then
echo "sparkopts!"
exit
fi
part=$((${ncores_tot}*3))


export INPUT="/lsst/tomo10M.parquet"
export SLURM_JOB_NUM_NODES=${n_executors}

spark-shell $SPARKOPTS --jars $JARS --conf spark.driver.args="${sep} ${zmax} ${part}" -I hpgrid.scala -I Timer.scala -i autocorr_a.scala
