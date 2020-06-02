#!/bin/bash

declare -i nargs
nargs=$#

if [ $nargs -lt 4 ]; then
echo "##################################################################################"
echo "usage: "
echo "./run_sparkdeg.sh binWidth nside1 Nbins nside2"
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

spark-shell $SPARKOPTS --jars $JARS --conf spark.driver.args="$1 $2 $3 $4 $part" -I hpgrid.scala -I Timer.scala -i autocorr_a.scala
