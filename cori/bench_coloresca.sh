#!/bin/bash

#init
source /global/homes/p/plaszczy/Spark/spark-fits-apps/cori/init_spark.sh
#

#colore scala bench
which spark-shell
rm scala_perf.txt
touch scala_perf.txt
for i in {1..10}; do
shifter spark-shell $SPARKOPTS < scripts/colore_ana.scala
done

#close
stop-all.sh
