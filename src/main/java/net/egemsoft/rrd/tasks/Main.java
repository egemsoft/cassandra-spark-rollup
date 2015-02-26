package net.egemsoft.rrd.tasks;

import net.egemsoft.rrd.SparkRollUp;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cenk on 11/02/15.
 */
public class Main {

  public static void main(String[] args) {
    CassandraDataCreator cassandraDataCreator = new CassandraDataCreator();
    cassandraDataCreator.createData();

    CassandraToElasticSearch cassandraToElasticSearch = new CassandraToElasticSearch();
    cassandraToElasticSearch.postPath();

    Map map = new HashMap();
    map.put("spmaster", "local");
    map.put("cashost", "ipam-ulus-db-1");
    map.put("caskeyspace", "test");
    map.put("castable", "metric");
    map.put("start", "1");
    map.put("end", "1000000000");
    map.put("rollup", "1000");
    map.put("destrollup", "2000");
    map.put("ttl", "1000000");

    SparkRollUp program = new SparkRollUp(map);
    program.run();
    System.out.println("Finish");
  }
}
