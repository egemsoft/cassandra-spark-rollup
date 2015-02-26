package net.egemsoft.rrd.elasticPaths;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cenk on 09/02/15.
 */
public class PathParser implements Parser, Serializable {

  @Override
  public ArrayList<Map> parse(String path) {
    ArrayList<Map> maps = new ArrayList<Map>();
    String[] values = path.split("\\.");
    int pathDepth = values.length - 1;

    Map<String, Object> elasticData = new HashMap<String, Object>();
    elasticData.put("path", path);
    elasticData.put("leaf", true);
    elasticData.put("depth", pathDepth + 1);
    elasticData.put("tenant", "");

    maps.add(elasticData);
    for (int i = 0; i < pathDepth; i++) {
      int position = nthOccurrence(path, '.', i);
      System.out.println(position);
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("path", path.substring(0, position));
      map.put("leaf", false);
      map.put("depth", i + 1);
      map.put("tenant", "");
      maps.add(map);
    }

    return maps;
  }

  public static int nthOccurrence(String str, char c, int n) {
    int pos = str.indexOf(c, 0);
    while (n-- > 0 && pos != -1)
      pos = str.indexOf(c, pos + 1);
    return pos;
  }
}
