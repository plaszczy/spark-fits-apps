#!/bin/bash

declare -i nargs
nargs=$#

if [ $nargs -lt 2 ]; then
echo "##################################################################################"
echo "usage: "
echo "./run_sparkdeg.sh sep zmax "
echo "##################################################################################"
exit
fi

#optional

sep=$1
zmax=$2

if [ -z "${ncores_tot}" ] ; then
echo "sparkopts!"
exit
fi
part=$((${ncores_tot}*3))


export FITSSOURCE="/lsst/LSST1Y"
export SLURM_JOB_NUM_NODES=${n_executors}

spark-shell $SPARKOPTS --jars $JARS --conf spark.driver.args="${sep} ${zmax} ${part}" -I hputils.scala -I Timer.scala -i autocoloreXYZ.scala
