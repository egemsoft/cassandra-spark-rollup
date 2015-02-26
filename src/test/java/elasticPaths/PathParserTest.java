package elasticPaths;

import net.egemsoft.rrd.elasticPaths.PathParser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by cenk on 09/02/15.
 */
public class PathParserTest {

  @Test
  public void testParse() {
    String path = "a.b.c.d.e.f";
    PathParser pathParser = new PathParser();
    ArrayList<Map> paths = pathParser.parse(path);
    System.out.println(paths.toString());
  }
}
