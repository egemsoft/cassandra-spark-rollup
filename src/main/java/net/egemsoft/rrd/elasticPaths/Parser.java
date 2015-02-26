package net.egemsoft.rrd.elasticPaths;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by cenk on 09/02/15.
 */
public interface Parser extends Serializable {
  public ArrayList<Map> parse(String path);
}
