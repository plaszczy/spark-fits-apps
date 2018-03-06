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

import scala.util.Try

object Utils {

  /**
    * Case class for galaxy position (RA, Dec)
    *
    * @param ra : (Double)
    *   RA coordinate in degree
    * @param dec : (Double)
    *   Dec coordinate in degree
    *
    */
  case class Point2D(ra: Double, dec: Double)

  /**
    * Case class for galaxy position (RA, Dec, z)
    *
    * @param ra : (Double)
    *   RA coordinate in degree
    * @param dec : (Double)
    *   Dec coordinate in degree
    * @param z : (Double)
    *   Redshift coordinate in degree
    */
  case class Point3D(ra: Double, dec: Double, z : Double)

  /**
    * Convert declination into theta
    *
    * @param dec : (Double)
    *   declination coordinate in degree
    * @return theta coordinate in radian
    *
    */
  def dec2theta(dec : Double) : Double = {
    Math.PI / 2.0 - Math.PI / 180.0 * dec
  }

  /**
    * Convert right ascension into phi
    *
    * @param ra : (Double)
    *   RA coordinate in degree
    * @return phi coordinate in radian
    *
    */
  def ra2phi(ra : Double) : Double = {
    Math.PI / 180.0 * ra
  }

  /**
    * Convert an array of String into a Point3D.
    * If the redshift is not present, returns Point(ra, dec, 0.0).
    * TODO: Need to catch other exceptions on ra and dec!
    *
    * @param x : (Array[String])
    *   array of String ["RA", "Dec", "z"]
    * @return Point(ra, dec, z) or Point(ra, dec, 0.0) if z undefined.
    *
    */
  def convertToPoint2D(x : Array[String]) : Point2D = {
    Point2D(x(0).toDouble, x(1).toDouble)
  }
}
