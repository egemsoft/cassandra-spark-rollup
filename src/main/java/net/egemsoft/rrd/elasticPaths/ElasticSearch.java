package net.egemsoft.rrd.elasticPaths;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by cenk on 09/02/15.
 */
public class ElasticSearch implements Serializable {
  final static Logger logger = Logger.getLogger(ElasticSearch.class);


  private Client client;

  public ElasticSearch() {

  }

  private void setClient() {

  }

  public void bulkInsert() throws IOException {

    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
            .setSource(jsonBuilder()
                    .startObject().field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject()
            )
    );

    bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
            .setSource(jsonBuilder().startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "another post")
                    .endObject()
            )
    );

    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      // process failures by iterating through each bulk response item
    }

    client.close();


  }

}
