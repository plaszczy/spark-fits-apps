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

// Import SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

object HealpixProjection {

  val usage =
    """
    Usage: HealpixProjection <catalogFilename> <nside>

    // Distribute the catalog.fits on a healpix grid at nside=512
    HealpixProjection catalog.fits 512

    """

  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Initialise your SparkSession
  val spark = SparkSession
    .builder()
    .getOrCreate()

  import spark.implicits._

  def main(args : Array[String]) = {

    val catalogFilename : String = args(0)
    val nside : Int = args(1).toInt

    // Initialise the Pointing object
    var ptg = new ExtPointing

    // Initialise HealpixBase functionalities
    val hp = new HealpixBase(nside, RING)

    // Instantiate the grid
    val grid = HealpixGrid(nside, hp, ptg)

    // Data
    val df = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .load(catalogFilename)

    df.show()

    val df_index = df.select($"RA", $"Dec")
      .as[Point2D]
      .map(x => (grid.index(dec2theta(x.dec), ra2phi(x.ra) ), 1) )
      .groupBy("_1")
      .agg(sum($"_2"))

    df_index.show()
    // println(df_index.select($"_1").count())
    // println(df_index.select($"_1").distinct.count())
    df_index.coalesce(1).rdd.saveAsTextFile("output/")
  }
}
