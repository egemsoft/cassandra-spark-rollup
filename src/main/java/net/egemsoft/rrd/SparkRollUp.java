package net.egemsoft.rrd;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.rdd.ReadConf;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by cenk on 04/02/15.
 */
public class SparkRollUp implements Serializable {
  final static Logger logger = Logger.getLogger(SparkRollUp.class);

  private String cassandraKeyspace;
  private String cassandraTable;
  private int rollup;
  private Long periodStart;
  private Long periodStop;
  private JavaRDD<CassandraRow> javaRDD;
  private String sparkMaster;
  private String cassandraHost;
  private int destinationRollup;
  private int ttl;
  private String appName;

  public SparkRollUp(Map arguments) {

    this.sparkMaster = (String) arguments.get("spmaster");
    this.cassandraHost = (String) arguments.get("cashost");
    this.cassandraKeyspace = (String) arguments.get("caskeyspace");
    this.cassandraTable = (String) arguments.get("castable");
    this.rollup = Integer.parseInt((String) arguments.get("rollup"));
    this.periodStart = Long.parseLong((String) arguments.get("start"));
    this.periodStop = Long.parseLong((String) arguments.get("end"));
    this.destinationRollup = Integer.parseInt((String) arguments.get("destrollup"));
    this.ttl = Integer.parseInt((String) arguments.get("ttl"));


    logger.info(String.format("%s %s %s %s %s %s %s %s %s",
        sparkMaster, cassandraHost, cassandraKeyspace, cassandraTable,
        rollup, periodStart, periodStop, destinationRollup, ttl));
  }

  public String getCassandraKeyspace() {
    return this.cassandraKeyspace;
  }

  public String getCassandraTable() {
    return this.cassandraTable;
  }

  public int getRollup() {
    return this.rollup;
  }


  public Map<String, Long> getTimePeriod() {
    Map<String, Long> timePeriod = new HashMap<String, Long>();
    timePeriod.put("start", this.periodStart);
    timePeriod.put("stop", this.periodStop);
    appName = this.periodStart.toString() + '-' + this.cassandraKeyspace;
    return timePeriod;
  }


  private void doSelect() {

  }

  public String getDestinationTable() {
    return getCassandraKeyspace() + '.' + getCassandraTable();
  }

  public void run() {
    doSelect();

    final Map<String, Long> timePeriod = getTimePeriod();
    logger.info("Do Select Starts");
    SparkConfig sparkConfig = new SparkConfig(sparkMaster, cassandraHost, appName);
    SparkContext sparkContext = new SparkContext(sparkConfig.getSparkConf());
    ReadConf readConf = new ReadConf(10000000, 10000, ConsistencyLevel.LOCAL_ONE);
    SparkContextJavaFunctions sparkFunctions = CassandraJavaUtil.javaFunctions(sparkContext);

    Long start = timePeriod.get("start");
    Long end = timePeriod.get("stop");

    CassandraJavaRDD<CassandraRow> cassandraJavaRDD = sparkFunctions.cassandraTable(getCassandraKeyspace(), getCassandraTable())
        .select("path", "data", "rollup")
        .where("time>=? AND time<? ", start, end)
        .withReadConf(readConf);

    javaRDD = cassandraJavaRDD.filter(new Function<CassandraRow, Boolean>() {
      @Override
      public Boolean call(CassandraRow cassandraRow) throws Exception {
        return getRollup() == cassandraRow.getInt("rollup");
      }
    });

    logger.info("Do Select Finishes");

    logger.info("Aggregate Algorithm");
    AggregateAlgorithm aggregateAlgorithm = new AggregateAlgorithm();
    aggregateAlgorithm.calculateAverage(javaRDD);
    JavaPairRDD<String, Double> javaPair = aggregateAlgorithm.getResults();

    logger.info("Persist");
    PersistToCassandra persistToCassandra = new PersistToCassandra(cassandraHost, sparkMaster, appName, getDestinationTable());
    persistToCassandra.persist(javaPair, getTimePeriod().get("start"), destinationRollup, ttl);
    sparkContext.stop();
  }
}
