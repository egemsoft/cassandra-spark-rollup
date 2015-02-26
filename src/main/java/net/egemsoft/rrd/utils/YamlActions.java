package net.egemsoft.rrd.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.Map;

/**
 * Created by cenk on 06/02/15.
 */
public class YamlActions implements Serializable {

  public static Map getYamlMap(String input) throws FileNotFoundException {
    InputStream yamlFile = new FileInputStream(new File(input));
    Yaml yaml = new Yaml();
    return (Map) yaml.load(yamlFile);
  }
}
