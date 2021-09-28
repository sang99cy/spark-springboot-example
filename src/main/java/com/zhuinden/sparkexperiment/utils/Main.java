package com.zhuinden.sparkexperiment.utils;

import com.zhuinden.sparkexperiment.model.Product;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Tuple;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) {
       /* Logger.getLogger("org").setLevel(Level.INFO);
        List<Double> inputData = new ArrayList<Double>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);*/

        SparkConf sparkConf = new SparkConf()
                .setAppName("startingSpark")
                .setSparkHome("E:/MOI TRUONG BIGDATA/spark-3.1.2-bin-hadoop3.2")
                .setMaster("local[*]");
        SparkContext sparkContext = new SparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder()
                .sparkContext(sparkContext)
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        Dataset<Row> csv = sparkSession.read().format("csv").
                load("F:\\AUTOML\\spark-springboot-example\\files\\shopee.csv");
        List<Row> rows = csv.collectAsList();
        List<String> old_products = new ArrayList<String>();
        for(int i = 0; i < rows.size(); i++){
            Row product = rows.get(i);
            old_products.add(product.toString());
        }
        System.out.println(old_products.size());
        //extract data
        old_products.forEach( a -> {
            System.out.println(a);
        });

    }
}
