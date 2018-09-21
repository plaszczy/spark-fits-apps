#!/bin/bash

#init
source /global/homes/p/plaszczy/Spark/spark-fits-apps/cori/init_spark.sh
#
echo "I am here: $PWD"
#colore python benches
which spark-submit
rm python_perf.txt
for i in {1..10}; do
spark-submit scripts/colore_ana.py
done

#colore scala bench
which spark-shell
rm scala_perf.txt
touch scala_perf.txt
for i in {1..10}; do
spark-shell < scripts/colore_ana.scala
done

#close
stop-all.sh
