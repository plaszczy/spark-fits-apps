/*
 * Copyright 2018 Julien Peloton
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.apps.healpix

// Scala utils
import scala.util.Try

// Spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

// Import the implicit to allow interaction with FITS
import com.sparkfits.fits._

// Healpix library
import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.RING

// Logger info
import org.apache.log4j.Level
import org.apache.log4j.Logger

// Internal imports
import com.apps.healpix.Utils._
import com.apps.healpix._

case class JobContext(session: SparkSession, grid: HealpixGrid, df_index: Dataset[Point3D])

object HealpixProjection {

  def time[R](text: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()

    var dt:Double = (t1 - t0).asInstanceOf[Double] / 1000000000.0

    val unit = "S"

    println("\n" + text + "> Elapsed time:" + " " + dt + " " + unit)

    result
  }

  /**
    * Replicate the same dataset numIt times.
    *
    * @param df : (DataFrame)
    *   The initial DataFrame
    * @param catalogFilename : (String)
    *   The catalog filename to replicate
    * @param numIt : (Int)
    *   The number of replication
    * @param ind : (Int)
    *   Internal index used for the recursion. Initialised to 0.
    * @return (DataFrame) initial DataFrame plus the replications.
    */
  def replicateDataSet(session: SparkSession, df: DataFrame,
      catalogFilename: String, numIt: Int, ind: Int = 0): DataFrame = {
    if (ind == numIt) {
      df
    } else {
      val df2 = session.readfits
        .option("datatype", "table")
        .option("HDU", 1)
        .load(catalogFilename)
        .union(df)
      replicateDataSet(session, df2, catalogFilename, numIt, ind + 1)
    }
  }

  def initialize(catalogFilename : String, nside : Int, replication: Int=0) = {
    val usage =
      """
    Usage: HealpixProjection <catalogFilename> <nside> <replication>=0 <loop>=1

    // Distribute the catalog.fits on a healpix grid at nside=512
    HealpixProjection catalog.fits 512

    // Replicate 10 times the catalog.fits and distribute the data
    // on a healpix grid at nside=512
    HealpixProjection catalog.fits 512 10

    // Launch 10 times the same job
    HealpixProjection catalog.fits 512 0 10

    """

    // Set to Level.WARN is you want verbosity
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Initialise your SparkSession
    val session = SparkSession
      .builder()
      .getOrCreate()

    import session.implicits._

    // Initialise the Pointing object
    var ptg = new ExtPointing

    // Initialise HealpixBase functionalities
    val hp = new HealpixBase(nside, RING)

    // Instantiate the grid
    val grid = HealpixGrid(nside, hp, ptg)

    // Data
    val df = session.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .load(catalogFilename)

    val df_tot = replicateDataSet(session, df, catalogFilename, replication)

    // Select Ra, Dec, Z
    val df_index = df_tot.select($"RA", $"Dec", ($"Z_COSMO").as("z"))
      .as[Point3D]

    df_index.persist(StorageLevel.MEMORY_ONLY_SER)

    JobContext(session, grid, df_index)
  }

  def job1(jc: JobContext) = {
    import jc.session.implicits._

    // Redshift boundaries
    val redList = List(0.1, 0.2, 0.3, 0.4, 0.5)

    // Make shells
    val shells = redList.slice(0, redList.size-1).zip(redList.slice(1, redList.size))

    // Loop over shells, make an histogram, and save results.
    for (pos <- shells) {
      val start = pos._1
      val stop = pos._2
      jc.df_index.filter(x => x.z >= start && x.z < stop) // filter in redshift space
        .map(x => (jc.grid.index(dec2theta(x.dec), ra2phi(x.ra) ), 1) ) // index
        .groupBy("_1").agg(sum($"_2")) // group by pixel index and make an histogram
        .coalesce(1).rdd.saveAsTextFile(s"output_redshift_${start}_${stop}/")
    }
  }

  def job2(jc: JobContext) = {
    import jc.session.implicits._

    // Redshift boundaries
    // val redList = List(0.1, 0.2, 0.3, 0.4, 0.5)

    // Make shells
    // val shells = redList.slice(0, redList.size-1).zip(redList.slice(1, redList.size))

    var ptg = new ExtPointing
    ptg.phi = 0.0
    ptg.theta = 0.0
    val radius = 0.02

    val selectedPixels = jc.grid.hp.queryDiscInclusive(ptg, radius, 4).toArray
    print(selectedPixels.length)

    val sp = jc.session.sparkContext.broadcast(selectedPixels)

    var counts = Array[(Int, Int)]()

    val firstShell = 0.1
    val lastShell = 0.5
    val shells:Int = 10
    val d:Double = (lastShell - firstShell) / shells

    // Loop over shells, make an histogram, and save results.
    for (shell <- 0 to shells) {
      val start = firstShell + shell*d
      val stop = start + d
      var c = jc.df_index.filter(x => x.z >= start && x.z < stop) // filter in redshift space
        .map(x => (jc.grid.index(dec2theta(x.dec), ra2phi(x.ra) ), x) ) // index
        .filter(x => sp.value.contains(x._1)) // select pixels touching the selected disk
        .map(x => x._2) // consider all points inside the disk
        .count
        // .filter()
      counts :+= (shell, c.toInt)
      print(s"shell=$start count=${c.toInt}\n")
    }

    counts
  }

  def job3(jc: JobContext) = {
    import jc.session.implicits._

    val firstShell = 0.1
    val lastShell = 0.5
    val shells:Int = 10
    val d:Double = (lastShell - firstShell) / shells

    var ptg = new ExtPointing
    ptg.phi = 0.0
    ptg.theta = 0.0
    val radius = 0.02

    val selectedPixels = jc.grid.hp.queryDiscInclusive(ptg, radius, 4).toArray
    print(selectedPixels.length)

    val sp = jc.session.sparkContext.broadcast(selectedPixels)

    // start by creating (shell_index, pixel, point)
    val result = jc.df_index
      .map(x => ((shells * (x.z - firstShell) / (lastShell - firstShell)).toInt, jc.grid.index(dec2theta(x.dec), ra2phi(x.ra) ), x) )
      //.select($"_1".alias("shell_index"), $"_2".alias("pixel"), $"_3".alias("point"))
      .filter(x => sp.value.contains($"_2")) // select pixels touching the selected disk
      .groupBy($"_2").agg(count($"_3"))
    print(s"result=$result \n")
  }

  /**
    * Routine to just check the throughput
    */
  def ioBenchmark(jc: JobContext, loop: Int = 1) = {
    import jc.session.implicits._

    for (i <- 1 to loop) {
      val result = jc.df_index.count()
      print(s"result=$result \n")
    }
  }

  /**
    * Routine to just check the throughput
    */
  def ioBenchmarkWithGroupBy(jc: JobContext, loop: Int = 1) = {
    import jc.session.implicits._

    for (i <- 1 to loop) {
      val result = jc.df_index
        .map(x => (jc.grid.index(dec2theta(x.dec), ra2phi(x.ra) ), 1) ) // index
        .groupBy("_1").agg(sum($"_2")) // group by pixel index and make an histogram
        .count()
      print(s"result=$result \n")
    }
  }

  def main(args : Array[String]): Unit = {

    val catalogFilename : String = args(0)
    val nside : Int = args(1).toInt
    val replication : Int = Try{args(2).toInt}.getOrElse(0)
    val loop : Int = Try{args(3).toInt}.getOrElse(1)

    val jc = time("Intialize", initialize(catalogFilename, nside, replication))

    // time("job1", job1(jc))
    // val result = time("job2", job2(jc))
    // val result = time("job3", job3(jc))
    val result = ioBenchmark(jc, loop)
  }
}
