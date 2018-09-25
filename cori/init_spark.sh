

#list of jars: currently SparkFITS
SF=/global/homes/p/plaszczy/Spark/spark-fits-apps/lib/spark-fits_2.11-0.6.0.jar

JARS=$SF
export EXEC_CLASSPATH=$JARS

module load spark/2.3.0
module load sbt
start-all.sh 

export PYSPARK_DRIVER_PYTHON=ipython

export fitsdir="/global/cscratch1/sd/plaszczy/LSST10Y"

echo "SparkFITS=$SF"
echo "FITS input dir=$fitsdir"


if [ -z "$NODES" ] ; then
echo "number of NODES cannot be found: did you use toSpark to log-in?"
return
fi

master="--master $SPARKURL"
n_executors=$(($NODES-1))

#config UPSUD (6 NODES)
#executor_cores=27
#executor_mem=48
#driver_mem=4

#config NERSC (from Lisa)
executor_cores=32
executor_mem=100
driver_mem=10


total_mem=$(($driver_mem+$n_executors*$executor_mem))
ncores_tot=$(($n_executors*$executor_cores))

echo "#executors=$n_executors"
echo "#cores used=$ncores_tot"
echo "mem used= ${total_mem} GB"
echo "mem for cache $(echo $n_executors*$executor_mem*0.6|bc) GB"


export SPARKOPTS="$master --driver-memory ${driver_mem}g --total-executor-cores ${ncores_tot} --executor-cores ${executor_cores} --executor-memory ${executor_mem}g --jars $SF"

#printenv SPARKOPTS


#aliases for interactive case
#alias spark-shell="shifter spark-shell $SPARKOPTS"
#alias pyspark="shifter pyspark $SPARKOPTS"
#alias spark-submit="shifter spark-submit $SPARKOPTS"
