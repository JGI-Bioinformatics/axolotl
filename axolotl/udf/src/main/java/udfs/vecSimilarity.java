package org.jgi.axolotl.udfs;

import org.jgi.axolotl.libs.*;
import org.apache.spark.sql.api.java.UDF3;

public class vecSimilarity implements UDF3<double[], double[], String, Double> {

    /**
     * Calculates the cosine similarity for two given vectors.
     * @return cosine similarity between the two vectors
     * @return 0 if one or two vectors are empty
     */
    public Double call(double[] A, double[] B, String metric) throws Exception {
        if(metric == "cosine") {
            return cosineSimilarity(A, B);
        } else if(metric == "euclidean") {
            return euclideanSimilarity(A, B);
        } else {
            return 0.0;
        }
    }
    public double cosineSimilarity(double[] A, double[] B) {
        if (A == null || B == null || A.length == 0 || B.length == 0 || A.length != B.length) {
            return 0.0;
        }
        double sumProduct = 0;
        double sumASq = 0;
        double sumBSq = 0;
        for (int i = 0; i < A.length; i++) {
            sumProduct += A[i]*B[i];
            sumASq += A[i] * A[i];
            sumBSq += B[i] * B[i];
        }
        if (sumASq == 0 && sumBSq == 0) {
            return 0.0;
        }
        return sumProduct / (Math.sqrt(sumASq) * Math.sqrt(sumBSq));
    }

    public double euclideanSimilarity(double[] A, double[] B) {
        if (A == null || B == null || A.length == 0 || B.length == 0 || A.length != B.length) {
            return 0.0;
        }
        double distSum = 0;
        for (int i = 0; i < A.length; i++) {
            double d = A[i] - B[i];
            distSum += d * d;
        }
        double es = 1- Math.sqrt(distSum);
        return es;
    }
}