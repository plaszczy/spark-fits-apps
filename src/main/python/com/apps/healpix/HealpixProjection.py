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
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import StorageLevel

import argparse

def quiet_logs(sc, log_level="ERROR"):
    """
    Set the level of log in Spark.

    Parameters
    ----------
    sc : SparkContext
        The SparkContext for the session
    log_level : String [optional]
        Level of log wanted: INFO, WARN, ERROR, OFF, etc.

    """
    ## Get the logger
    logger = sc._jvm.org.apache.log4j

    ## Set the level
    level = getattr(logger.Level, log_level, "INFO")

    logger.LogManager.getLogger("org"). setLevel(level)
    logger.LogManager.getLogger("akka").setLevel(level)

def replicatedataset(spark, df, inputpath, numit, ind=0):
    """
    Replicate the same dataset numIt times.

    Parameters
    ----------
    sparksession : SparkSession
        The SparkSession of the App
    df : DataFrame
        The initial DataFrame
    inputpath : String
        The path to the dataset to add
    numit : Int
        Number of replication
    ind : Int
        Counter used for the recursion

    Returns
    ----------
    df2 : DataFrame
        A DataFrame made of df + df2 x numit.

    """
    if ind == numit:
        return df
    else:
        df2 = spark.read\
            .format("com.sparkfits")\
            .option("hdu", 1)\
            .load(inputpath)\
            .union(df)
        return replicatedataset(spark, df2, inputpath, numit, ind+1)


def addargs(parser):
    """ Parse command line arguments for readfits """

    ## Arguments
    parser.add_argument(
        '-inputpath', dest='inputpath',
        required=True,
        help='Path to a FITS file or a directory containing FITS files')

    parser.add_argument(
        '-nside', dest='nside',
        required=True,
        type=int,
        help='NSIDE for the output map')

    parser.add_argument(
        '-replication', dest='replication',
        default=0,
        type=int,
        help='Number of replication of the input dataset')

    parser.add_argument(
        '-loop', dest='loop',
        default=1,
        type=int,
        help='Number of iterations')

    parser.add_argument(
        '-log_level', dest='log_level',
        default="ERROR",
        help='Level of log for Spark. Default is ERROR.')


if __name__ == "__main__":
    """
    Read the data from a FITS file using Spark,
    and show the first rows and the schema.
    """
    parser = argparse.ArgumentParser(
        description='Distribute the data of a FITS file.')
    addargs(parser)
    args = parser.parse_args(None)

    spark = SparkSession\
        .builder\
        .getOrCreate()

    ## Set logs to be quiet
    quiet_logs(spark.sparkContext, log_level=args.log_level)

    df = spark.read\
        .format("com.sparkfits")\
        .option("hdu", 1)\
        .load(args.inputpath)

    ## Next action call will cache the data
    df = replicatedataset(spark, df, args.inputpath, args.replication)

    df_tot = df.select(col("RA"), col("Dec"), col("Z_COSMO").alias("z"))\
        .persist(StorageLevel.MEMORY_ONLY_SER)

    ## Loop over iterations
    for loop in range(args.loop):
        result = df_tot.count()
        print(result)
