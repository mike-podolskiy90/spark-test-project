package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Main {
  public static void main(String[] args) {
    Main runner = new Main();
    runner.run();
  }

  @SuppressWarnings("SqlNoDataSourceInspection")
  public void run() {
    try (SparkSession spark = SparkSession.builder().appName("Simple Spark Runner").enableHiveSupport().getOrCreate()) {
      spark.sql("USE dev");
      spark.sparkContext().conf().set("hive.exec.compress.output", "true");

      Dataset<Row> sqlResult = spark.sql("SELECT gbifid, datasetKey FROM occurrence LIMIT 10");
      List<Row> sqlResultList = sqlResult.collectAsList();

      sqlResultList.forEach(p -> System.out.println("gbifid: " + p.getAs("gbifid") + ", datasetKey: " + p.getAs("datasetKey")));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}