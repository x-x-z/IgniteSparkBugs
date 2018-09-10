package org.xxz.ignite_spark_bugs;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author Denis Kostin
 */

public enum SparkSingleton {
    INSTANCE;

    private SparkSession sparkSession;

    SparkSingleton() {
        SparkConf sparkConf = new SparkConf(true)
                .setMaster("local[*]")
                .setAppName(getClass().getName());

        this.sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .appName(getClass().getName())
                .getOrCreate();
    }

    public SparkSession get() {
        return sparkSession;
    }

}
