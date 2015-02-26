package net.egemsoft.rrd;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by cenk on 06/02/15.
 */
public class SparkConfig implements Serializable {
  final static Logger logger = Logger.getLogger(SparkConfig.class);

  private String cassandraHost;
  private String sparkMaster;
  private String appName;
  private final static String connectionTimeOut = "10000000000";
  private final static String readTimeOut = "10000000000";
  private final static String allowMultipleContexts = "true";


  public SparkConfig(String sparkMaster, String cassandraHost, String appName) {
    this.sparkMaster = sparkMaster;
    this.cassandraHost = cassandraHost;
    String uuid = UUID.randomUUID().toString();
    this.appName = uuid + '-' + appName;
  }


  public SparkConf getSparkConf() {
    logger.info(String.format("App name= %s", appName));

    return new SparkConf(true).setMaster(sparkMaster).setAppName(appName)
        .set("spark.cassandra.connection.host", getCassandraHost())
        .set("spark.cassandra.connection.timeout_ms", connectionTimeOut)
        .set("spark.cassandra.read.timeout_ms", readTimeOut)
        .set("spark.driver.allowMultipleContexts", allowMultipleContexts)
        .set("spark.cores.max", "4");
  }

  public SparkContext getSparkContext() {

    return new SparkContext(getSparkConf());

  }

  public JavaSparkContext getJavaSparkContext() {
    return JavaSparkContext.fromSparkContext(getSparkContext());

  }

  protected String getCassandraHost() {
    return this.cassandraHost;
  }

}
