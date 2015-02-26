package net.egemsoft.rrd.elasticPaths;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by cenk on 12/02/15.
 */
public interface Elastic {

  public void bulkInsert(ArrayList<Map> mapList);
}
