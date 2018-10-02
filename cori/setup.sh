#SPARK

alias spark-interactive="source $PWD/cori/spark-interactive.sh"
alias spark-batch="source $PWD/cori/spark-batch.sh"

alias init_spark="source $PWD/cori/init_spark.sh"


ask ()
{
    echo -n "$@" '[y/n] ';
    read ans;
    case "$ans" in
        y* | Y*)
            return 0
        ;;
        *)
            return 1
        ;;
    esac
}
