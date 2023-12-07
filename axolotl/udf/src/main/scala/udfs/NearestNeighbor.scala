package org.jgi.axolotl.udfs;

import org.apache.spark.sql.api.java.UDF2;
import scala.collection.immutable.Map;
import scala.math.sqrt;


class NearestNeighbor extends UDF2[Map[Long, Map[String, Double]], Map[Long, Map[String, Double]], Map[Long, Tuple2[Long, Double]]] {

    override def call(query: Map[Long, Map[String, Double]], target: Map[Long, Map[String, Double]]): Map[Long, Tuple2[Long, Double]] = {

        query.map { case (query_id, query_feature) => query_id -> {
            val best_hit = target.map {
                case (target_id, target_feature) => target_id -> calcDist(query_feature, target_feature)
            }.minBy(_._2)
            best_hit
        }}

    }

    def calcDist(feature_1: Map[String, Double], feature_2: Map[String, Double]): Double = {

        val allKeys = (feature_1.keys.toSet union feature_2.keys.toSet).toList

        val squaredDifferences = allKeys.map { key =>
          val diff = feature_2.getOrElse(key, 0.00) - feature_1.getOrElse(key, 0.00)
          diff * diff
        }
        sqrt(squaredDifferences.sum)

    }
}
