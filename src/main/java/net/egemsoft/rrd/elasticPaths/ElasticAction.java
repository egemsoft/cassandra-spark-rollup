package net.egemsoft.rrd.elasticPaths;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by cenk on 11/02/15.
 */
public class ElasticAction implements Action {
  private Elastic elasticPath;

  public ElasticAction(Elastic elasticPath) {
    this.elasticPath = elasticPath;
  }

  @Override
  public void processArrayList(ArrayList<Map> paths) {

    elasticPath.bulkInsert(paths);
  }

  @Override
  public void processList(List list) {

  }
}
