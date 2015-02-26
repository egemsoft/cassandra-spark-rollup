package net.egemsoft.rrd.elasticPaths;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by cenk on 09/02/15.
 */
public interface Action {

  public void processArrayList(ArrayList<Map> paths);

  public void processList(List list);
}
