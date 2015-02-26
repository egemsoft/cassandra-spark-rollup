package net.egemsoft.rrd;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import net.egemsoft.rrd.utils.cassandra.Cql;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by cenk on 06/02/15.
 */
public class PersistToCassandra implements Serializable {
  final static Logger logger = Logger.getLogger(PersistToCassandra.class);

  private SparkConf sparkConf;
  private String tableName;

  public PersistToCassandra(String cassandraHost, String sparkMaster, String appName, String tableName) {

    SparkConfig sparkConfig = new SparkConfig(sparkMaster, cassandraHost, appName);
    sparkConf = sparkConfig.getSparkConf();
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public SparkConf getSparkConf() {
    return sparkConf;
  }

  private CassandraConnector getCassandraConnector() {
    return CassandraConnector.apply(getSparkConf());
  }

  protected void executeCql(String cql) {
    CassandraConnector cassandraConnector = getCassandraConnector();
    try (Session session = cassandraConnector.openSession()) {
      session.executeAsync(cql);
    } catch (Exception e) {
      e.printStackTrace();
    }
    cassandraConnector.closestLiveHost();
  }

  protected void createTableIfNotExists() {
    String cql = "\n" +
        "CREATE TABLE IF NOT EXISTS ipam.test (\n" +
        "  period int,\n" +
        "  rollup int,\n" +
        "  tenant text,\n" +
        "  path text,\n" +
        "  time bigint,\n" +
        "  data list<double>,\n" +
        "  PRIMARY KEY ((tenant, period, rollup, path), time)\n" +
        ") WITH\n" +
        "  bloom_filter_fp_chance=0.010000 AND\n" +
        "  caching='KEYS_ONLY' AND\n" +
        "  comment='' AND\n" +
        "  dclocal_read_repair_chance=0.000000 AND\n" +
        "  gc_grace_seconds=864000 AND\n" +
        "  index_interval=128 AND\n" +
        "  read_repair_chance=0.100000 AND\n" +
        "  replicate_on_write='true' AND\n" +
        "  populate_io_cache_on_flush='false' AND\n" +
        "  default_time_to_live=0 AND\n" +
        "  speculative_retry='NONE' AND\n" +
        "  memtable_flush_period_in_ms=0 AND\n" +
        "  compaction={'class': 'SizeTieredCompactionStrategy'} AND\n" +
        "  compression={'sstable_compression': 'LZ4Compressor'};\n";
    logger.info("Create table cql: " + cql);
    executeCql(cql);

  }


  public int getPeriod(int destinationRollUp, int ttl) {
    return Integer.parseInt(String.valueOf(ttl / destinationRollUp));
  }

  public long getTime(Long timePeriod) {
    return timePeriod;
  }

  public void persist(JavaPairRDD<String, Double> cassandraRows, Long startTime, int destinationRollup, int ttl) {
    List<Tuple2<String, Double>> cassandraData = cassandraRows.collect();
    logger.info("Persist to cassandra");
    for (Tuple2<String, Double> cassandraRow : cassandraData) {
      try{
        String cql = Cql.insertCql(cassandraRow, getTableName(), getPeriod(destinationRollup, ttl), destinationRollup, getTime(startTime), ttl);
        executeCql(cql);
      }catch (Exception e){
        String cql = Cql.cassandraRowToCql(cassandraRow, getTableName(), getPeriod(destinationRollup, ttl), destinationRollup, getTime(startTime), ttl);
        executeCql(cql);
      }


    }

  }

//  public void persistRDD() {
//    //TODO
//
//    List<Double> data = new ArrayList<Double>();
//    data.add(1.0);
//    data.add(2.0);
//    data.add(3.0);
//    List<Metrics> metrics = Arrays.asList(
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data),
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data),
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data),
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data),
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data)
//    );
//
//
//    JavaRDD<Metrics> saveRdd = getJavaSparkContext().parallelize(metrics);
//
//    javaFunctions(saveRdd).writerBuilder("ipam", "test", mapToRow(Metrics.class)).saveToCassandra();
//
//  }
//
//  public void persistRDD(JavaRDD javaRDD) {
//    //TODO
//
//    List<Double> data = new ArrayList<Double>();
//    data.add(1.0);
//    data.add(2.0);
//    data.add(3.0);
//    List<Metrics> metrics = Arrays.asList(
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data),
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data),
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data),
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data),
//        new Metrics("", 1000, 1000, "sample.path", new Date().getTime(), data)
//    );
//
//
//    JavaRDD<Metrics> saveRdd = getJavaSparkContext().parallelize(metrics);
//
//    javaFunctions(saveRdd).writerBuilder("ipam", "test", mapToRow(Metrics.class)).saveToCassandra();
//
//  }
}
