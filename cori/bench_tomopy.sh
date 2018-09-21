#!/bin/bash

#init
source /global/homes/p/plaszczy/Spark/spark-fits-apps/cori/init_spark.sh
#
echo "I am here: $PWD"
#colore python benches
which spark-submit
rm python_perf.txt
for i in {1..10}; do
shifter spark-submit $SPARKOPTS scripts/colore_ana.py
done


#close
stop-all.sh
