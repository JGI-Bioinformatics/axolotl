package org.jgi.axolotl.udfs;

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.immutable.Map;
import scala.math.sqrt;

class L2Normalize extends UDF1[Map[String, Double], Map[String, Double]] {

    override def call(vector: Map[String, Double]): Map[String, Double] = {

        // in python:
        // divider = sqrt(sum([val**2 for val in feature.values()]))
        // return {key: val/divider for key, val in feature.items()}

        val divider: Double = sqrt(vector.values.map(value => value * value).sum)
        val resultMap: Map[String, Double] = vector.map { case (key, value) => key -> (value.toDouble / divider) }
        resultMap
        
    }
}