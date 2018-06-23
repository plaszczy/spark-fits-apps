#!/bin/bash

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
opts="$local --driver-memory ${driver_mem}g --total-executor-cores ${ncores_tot} --executor-cores ${executor_cores} --executor-memory ${executor_mem}g "

rm scala_perf.txt
rm python_perf.txt
touch scala_perf.txt
for i in {1..100}; do 
#cmd="spark-shell $opts --jars lib/spark-fits_2.11-0.4.0.jar < scripts/colore_ana.scala"
#eval $cmd
cmd="spark-submit $opts --jars lib/spark-fits_2.11-0.4.0.jar scripts/colore_ana.py"
eval $cmd
done
