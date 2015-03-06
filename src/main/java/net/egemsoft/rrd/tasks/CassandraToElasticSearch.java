package net.egemsoft.rrd.tasks;

import net.egemsoft.rrd.elasticPaths.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cenk on 11/02/15.
 */
public class CassandraToElasticSearch {

  public void postPath() {
    Map map = new HashMap();
    map.put("spmaster", "local");
    map.put("cashost", "ipam-ulus-db-2");
    map.put("caskeyspace", "test");
    map.put("castable", "metric");

    ElasticPath elasticPath = new ElasticPath("test_path");
    Action action = new ElasticAction(elasticPath);
    Parser parser = new PathParser();


    Cassandra cassandra = new Cassandra(map);
    cassandra.distinct(parser, action);
  }
}
