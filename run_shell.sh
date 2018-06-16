#!/bin/bash
# Copyright 2018 Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

nargs=$#
if ! [ $nargs -eq 1 ] ; then
echo "missing shell [pyspark/spark-shell]"
exit
fi
shell=$1
# Parameters (put your file)
export fitsdir="hdfs://134.158.75.222:8020//lsst/LSST10Y"
echo "working on $fitsdir"

#cluster: 1 machine(executor= 18 cores de 2 GB=36GB)
master="spark://134.158.75.222:7077 "
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
opts="--master $master --driver-memory ${driver_mem}g --total-executor-cores ${ncores_tot} --executor-cores ${executor_cores} --executor-memory ${executor_mem}g "




# Run it!
cmd="$shell $opts --jars lib/spark-fits_2.11-0.4.0.jar"
echo $cmd
eval $cmd
