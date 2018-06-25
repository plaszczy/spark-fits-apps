#!/bin/bash


nargs=$#
if [ $nargs -ne 2 ] ; then
echo "usage pyrun.sh script.py nloop"
exit 1
fi

# Parameters (put your file)
export fitsdir="hdfs://134.158.75.222:8020//lsst/LSST10Y"
echo "working on $fitsdir"

#cluster: 1 machine(executor= 18 cores de 2 GB=36GB)
local="--master $master spark://134.158.75.222:7077 "
n_executors=8
executor_cores=17
executor_mem=30
driver_mem=4


total_mem=$(($driver_mem+$n_executors*$executor_mem))
ncores_tot=$(($n_executors*$executor_cores))

echo "#executors=$n_executors"
echo "#cores used=$ncores_tot"
echo "mem used= ${total_mem} GB"
echo "mem for cache $(echo $n_executors*$executor_mem*0.6|bc) GB"
export opts="$local --driver-memory ${driver_mem}g --total-executor-cores ${ncores_tot} --executor-cores ${executor_cores} --executor-memory ${executor_mem}g  --jars lib/spark-fits_2.11-0.4.0.jar"

nloop=$2
echo "running $nloop times"

for (( i=1 ; i<=$nloop; i++ )); do 
cmd="spark-submit $opts $1"
echo $cmd
eval $cmd
done
