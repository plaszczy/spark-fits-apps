

nargs=$#
if ! [ $nargs -eq 3 ]; then
echo "usage: spark-batch exec-file N(#nodes) time(mins)"
return
fi

exe=$1

declare -i NODES
NODES=$2

declare -i t
t=$3

if [ $t -gt 30 ]; then
queue=regular
else
queue=debug
fi

echo "running $exe on $NODES nodes for $t mins on queue $queue"

export NODES

IMG=registry.services.nersc.gov/plaszczy/spark_desc:v2.3.0

if ! [ -d /global/cscratch1/sd/$USER ] ; then 
echo "creating NodeCache in /global/cscratch1/sd/$USER"
mkdir /global/cscratch1/sd/$USER
fi

jobname=$(basename $exe | awk -F"." '{NF--; printf("%s",$1)}')

cmd="sbatch -N $NODES -t $t -C haswell -q $queue -L SCRATCH -J $jobname --image=$IMG --volume=\"/global/cscratch1/sd/$USER/tmpfiles:/tmp:perNodeCache=size=200G\" $exe"
echo $cmd
ask "-> submit?"
if [ $? -eq 0 ] ; then
eval $cmd
fi

