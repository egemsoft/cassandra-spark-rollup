package net.egemsoft.rrd.elasticPaths;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by cenk on 12/02/15.
 */
public class ElasticWithSpark implements Serializable, Elastic {
  private String indexName;


  public ElasticWithSpark(String indexName) {
    this.indexName = indexName;
  }

  public void bulkInsert(ArrayList<Map> mapList) {
//
//    SparkConf sparkConf = new SparkConf().setAppName("Elastic").setMaster("ipam-ulus-db-2");
//    sparkConf.set("es.index.auto.create", "true");
//    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//
//    Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
//    Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");
//
//    JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
//    .saveToEs(javaRDD, "spark/docs");
  }
}
