#!/bin/bash
#SBATCH -p debug
#SBATCH -N 6
#SBATCH -t 00:30:00
#SBATCH -J sparkFITS
#SBATCH -C haswell
#SBATCH --image=nersc/spark-2.3.0:v1
#SBATCH -A m1727

#module load spark
#module load sbt
#start-all.sh

## SBT Version
SBT_VERSION=2.11.8
SBT_VERSION_SPARK=2.11

## Package version
VERSION=0.1.0

# Package it
sbt ++${SBT_VERSION} package

# External JARS
SF=lib/spark-fits_2.11-0.6.0.jar
HP=lib/jhealpix.jar
export fitsdir="/global/cscratch1/sd/plaszczy/LSST10Y"

echo "working on $fitsdir"

#cluster: 1 machine(executor= 32 cores de 2 GB=64B)
local="--master $SPARKURL"
n_executors=$(($NODES-1))
executor_cores=32
executor_mem=50
driver_mem=10

total_mem=$(($driver_mem+$n_executors*$executor_mem))
ncores_tot=$(($n_executors*$executor_cores))

echo "#executors=$n_executors"
echo "#cores used=$ncores_tot"
echo "mem used= ${total_mem} GB"
echo "mem for cache $(echo $n_executors*$executor_mem*0.6|bc) GB"

opts=" $local --driver-memory ${driver_mem}g --total-executor-cores ${ncores_tot} --executor-cores ${executor_cores} --executor-memory ${executor_mem}g "

# Parameters
nside=512
loop=1


# 110G, 550, 1100 GB
for replication in 0 ; do
  shifter spark-submit $opts \
    --jars ${SF},${HP} \
    --class com.apps.healpix.HealpixProjection \
    target/scala-${SBT_VERSION_SPARK}/healpixprojection_${SBT_VERSION_SPARK}-${VERSION}.jar \
    $fitsdir $nside $replication $loop
  wait
done

#stop-all.sh
