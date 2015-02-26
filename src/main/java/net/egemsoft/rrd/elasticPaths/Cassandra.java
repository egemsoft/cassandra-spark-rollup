package net.egemsoft.rrd.elasticPaths;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.rdd.ReadConf;
import net.egemsoft.rrd.SparkConfig;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by cenk on 09/02/15.
 */
public class Cassandra implements Serializable {
  final static Logger logger = Logger.getLogger(Cassandra.class);

  private String sparkMaster;
  private String cassandraHost;
  private String cassandraKeyspace;
  private String cassandraTable;
  private long start;
  private long end;
  private String appName;

  public Cassandra(Map map) {
    this.sparkMaster = (String) map.get("spmaster");
    this.cassandraHost = (String) map.get("cashost");
    this.cassandraKeyspace = (String) map.get("caskeyspace");
    this.cassandraTable = (String) map.get("castable");
    this.start = Integer.parseInt(String.valueOf(map.get("start")));
    this.end = Integer.parseInt(String.valueOf(map.get("end")));
    this.appName = "ElasticPath" + '-' + this.cassandraKeyspace;
  }

  public void distinct(final Parser parser, Action action) {
    SparkConfig sparkConfig = new SparkConfig(sparkMaster, cassandraHost, appName);

    SparkContext sparkContext = new SparkContext(sparkConfig.getSparkConf());
    SparkContextJavaFunctions sparkFunctions = CassandraJavaUtil.javaFunctions(sparkContext);
    ReadConf readConf = new ReadConf(10000000, 10000, ConsistencyLevel.LOCAL_ONE);
    //long end = Math.round(new Date().getTime() / 1000);
    //long start = end - (60 * 60 * 2);
    logger.info(String.format("Start Time: %s, End Time: %s", start, end));

    System.out.println(String.format("Start Time: %s, End Time: %s", start, end));
    CassandraJavaRDD<CassandraRow> cassandraJavaRDD = sparkFunctions.cassandraTable(cassandraKeyspace, cassandraTable)
        .select("path")
        .where("time>=? AND time<? ", start, end)
        .withReadConf(readConf);
    logger.info("Group By");
    JavaPairRDD<String, Iterable<CassandraRow>> groupedData = cassandraJavaRDD.groupBy(new Function<CassandraRow, String>() {
      @Override
      public String call(CassandraRow row) throws Exception {
        return row.getString("path");
      }
    });

    int counter = 0;
    ArrayList<Map> resultMap = new ArrayList<Map>();
    for (Tuple2<String, Iterable<CassandraRow>> cassandraRow : groupedData.collect()) {

      resultMap.addAll(parser.parse(cassandraRow._1()));

      counter++;
    }
    logger.info(counter);
    action.processArrayList(resultMap);
  }

}

