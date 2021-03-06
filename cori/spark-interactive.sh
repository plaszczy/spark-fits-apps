

nargs=$#
if ! [ $nargs -eq 2 ]; then
echo "##################################################################################"
echo "usage: toSpark N(#nodes) t(mins)"
echo "# each cori node has 32 cores and 100GB RAM (from which 60% will be used for cache)"
echo "# if you ask for less than 30 mins you will be running on the debug queue"
echo "##################################################################################"
return
fi
declare -i NODES
NODES=$1

declare -i t
t=$2

if [ $t -gt 30 ]; then
queue=interactive
else
queue=debug
fi

echo "running on $NODES nodes for $t mins on queue $queue"

export NODES

#IMG=nersc/spark-2.3.0:v1
IMG=registry.services.nersc.gov/plaszczy/spark_desc:v2.3.0

echo "IMG=$IMG"

if ! [ -d /global/cscratch1/sd/$USER ] ; then 
echo "creating NodeCache in /global/cscratch1/sd/$USER" 
mkdir /global/cscratch1/sd/$USER
fi

salloc -N $NODES -t $t -C haswell -q $queue --image=$IMG --volume="/global/cscratch1/sd/$USER/tmpfiles:/tmp:perNodeCache=size=200G"
