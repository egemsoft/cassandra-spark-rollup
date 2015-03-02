package net.egemsoft.rrd.elasticPaths;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Map;

/**
 * Created by cenk on 09/02/15.
 */
public class ElasticPath implements Serializable, Elastic {

  private String indexName;


  public ElasticPath(String indexName) {
    this.indexName = indexName;
  }

  public XContentBuilder getPathMapping() throws IOException {
    XContentBuilder data = XContentFactory.jsonBuilder().startObject()
        .startObject("tenant")
        .field("type", "string")
        .field("analyzer", "not_analyzed")
        .endObject()
        .startObject("path")
        .field("type", "string")
        .field("analyzer", "not_analyzed")
        .startObject("leaf")
        .field("type", "boolean")
        .endObject()
        .startObject("depth")
        .field("type", "long")
        .endObject().endObject();
    return data;
  }

  public boolean checkIndexExists() {
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "ttsearch").build();
    Client client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress("ttsearch", 9300));

    return client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
  }

  public void setMapping(boolean indexExists) throws IOException {
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "ttsearch").build();
    Client client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress("ttsearch", 9300));
    System.out.println(getPathMapping().toString());
    if (!indexExists) {


      client.prepareIndex(indexName, "path")
          .setSource(getPathMapping())
          .execute().actionGet();

    } else {

//      client.admin().indices().preparePutMapping(indexName).setType("path")
//          .setSource(mapping)
//          .execute().actionGet(new TimeValue(60, TimeUnit.SECONDS));
    }

  }

  public void bulkInsert(ArrayList<Map> mapList) {
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "ttsearch").build();
    Client client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress("ttsearch1", 9300));

    BulkRequestBuilder bulkBuilder = client.prepareBulk();
    int counter = 1;
    ListIterator<Map> iterator = mapList.listIterator();
    while (iterator.hasNext()) {
      System.out.println(counter);
      Map map = iterator.next();

      if (counter % 5000 == 0 || !iterator.hasNext()) {
        BulkResponse bulkResponse = bulkBuilder.execute().actionGet();
        if (bulkResponse.hasFailures()) {
          System.out.println("Error");
          System.out.println(bulkResponse.buildFailureMessage());
        }
        System.out.println(bulkResponse.toString());
        bulkBuilder = client.prepareBulk();
      }
      System.out.println((String) map.get("path"));

      bulkBuilder.add(client.prepareIndex(indexName, "path", (String) map.get("path"))
          .setSource(map));
      counter++;
    }


    client.close();
  }

}
