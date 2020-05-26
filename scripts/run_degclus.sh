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

ncores=${ncores_tot}

export FITSSOURCE="/lsst/LSST1Y"
export SLURM_JOB_NUM_NODES=${n_executors}

spark-shell $SPARKOPTS --jars $JARS --conf spark.driver.args="${sep} ${zmax} ${ncores}" -I hputils.scala -I Timer.scala -i autocolore.scala
