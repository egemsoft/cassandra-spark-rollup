package net.egemsoft.rrd.tasks;

import net.egemsoft.rrd.elasticPaths.ElasticPath;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cenk on 11/02/15.
 */
public class ElasticPathBulkInsert {
  @Test
  public void postToElastic() {

    ArrayList<Map> mapList = new ArrayList<>();

    Integer path = 0;
    while (path < 100000) {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("tenant", "");
      map.put("path", path.toString());
      map.put("leaf", "true");
      map.put("depth", "8");
      mapList.add(map);
      path++;
    }


    ElasticPath elasticPath = new ElasticPath("test_path");
    elasticPath.bulkInsert(mapList);

  }
}
