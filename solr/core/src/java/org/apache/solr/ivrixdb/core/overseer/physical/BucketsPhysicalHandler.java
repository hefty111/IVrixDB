/*
 * IVrixDB software is licensed under the IVrixDB Software License Agreement
 * (the "License"); you may not use this file or the IVrixDB except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     https://github.com/hefty111/IVrixDB/blob/master/LICENSE.pdf
 *
 * Unless required by applicable law or agreed to in writing, IVrixDB software is provided
 * and distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions
 * and limitations under the License. See the NOTICE file distributed with the IVrixDB Software
 * for additional information regarding copyright ownership.
 */

package org.apache.solr.ivrixdb.core.overseer.physical;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.*;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.SolrCollectionMetadata;
import org.apache.solr.ivrixdb.core.overseer.utilities.SolrStateZkUtil;
import org.apache.solr.ivrixdb.utilities.Constants;
import org.slf4j.Logger;

/**
 * This object handles the physical side of IVrix Buckets
 * (i.e, the Solr-state side.) Each IVrix Node possesses a
 * single BucketsPhysicalHandler. It can create, delete, attach,
 * and detach buckets, and prepare WARM buckets for COLD rollover.
 * It does so through the Solr Overseer. Generally, each function
 * of this handler will send multiple requests to the Solr Overseer.
 * As a rule of thumb, when one of those requests fail, the function
 * will still try to execute the others, unless that error is harmful
 * and will break the system.
 *
 * To know the Solr state makeup of an IVrix Bucket, please refer to {@link IVrixBucket}.
 *
 * @author Ivri Faitelson
 */
public class BucketsPhysicalHandler {
  private static final Logger log = IVrixOverseer.log;
  private static final Integer NUMBER_OF_SHARDS = 1;
  private static final String SHARD_NAME = "shard1";
  private static final String SHARDS_LIST_STRING = "shard1";

  private HttpSolrClient solrClient;

  /**
   * Initializes the handler
   */
  public void init(String baseURL) {
    this.solrClient = new HttpSolrClient.Builder(baseURL).build();
  }

  /**
   * Shuts down the handler
   */
  public void shutdown() throws IOException {
    solrClient.close();
  }

  /**
   * Creates an IVrix Bucket with one indexer replica and
   * multiple replication replicas determined by the replication factor
   * @throws IOException If failed to create the indexer (leader) Replica
   */
  public void createBucket(String bucketName, String indexerNodeName,
                           List<String> replicationNodes, int replicationFactor) throws IOException {
    executeRequestIgnoringExceptions(createEmptyCollectionRequest(bucketName, SHARDS_LIST_STRING, NUMBER_OF_SHARDS, replicationFactor));

    executeRequest(createAddReplicaRequest(Replica.Type.TLOG, indexerNodeName, bucketName, SHARD_NAME));
    for (String replicationNodeName : replicationNodes) {
      executeRequestIgnoringExceptions(createAddReplicaRequest(Replica.Type.TLOG, replicationNodeName, bucketName, SHARD_NAME));
    }
  }

  /**
   * Deletes an IVrix Bucket
   */
  public void deleteBucket(String bucketName) {
    executeRequestIgnoringExceptions(createDeleteCollectionRequest(bucketName));
  }

  /**
   * Prepares a WARM bucket for COLD rollover by deleting all replication replicas
   */
  public void prepareWarmBucketForColdRollover(IVrixBucket bucket) throws IOException {
    String bucketName = bucket.getName();
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollection(bucketName);
    SolrCollectionMetadata collectionMetadata = bucket.getSolrMetadata();
    List<CollectionsAPIRequest> replicaRequests = new LinkedList<>();

    for (Slice shardState : collectionState.getSlices()) {
      String indexerReplicaName = collectionMetadata.getSlice(shardState.getName()).getLeader().getName();

      for (Replica replicaState : shardState.getReplicas()) {
        if (!replicaState.getName().equals(indexerReplicaName)) {
          // TODO: this request is bugged after bucket has been indexed into (without restarting solr.)
          //       this bug was fixed by changing a low-level Lucene index lock type from "native" to "single" (in solrconfig.xml).
          //       This solution should be reviewed VERY CAREFULLY, as it may pose dangers down the line.
          replicaRequests.add(createDeleteReplicaRequest(bucketName, shardState, replicaState));
        }
      }
    }
    // TODO: This request fixes the issue of detaching buckets after indexing.
    //       It would be a good idea to figure a different solution.
    replicaRequests.add(createReloadCollectionRequest(bucketName));

    executeRequestsIgnoringExceptions(replicaRequests);

    // TODO: This request fixes the issue of attaching a bucket that was affected by node death
    //       In a way that it still has removed replicas in shard leader election ("terms" zk node).
    //       It would be a good idea to figure a different solution.
    SolrStateZkUtil.cleanReplicaLeaderElectionFromRemovedReplicas(bucket);
  }

  /**
   * Attaches an IVrix Bucket.
   * Ignores replicas that are already attached. That is, that they already exist
   * in the Solr state (that includes dead node / unhealthy replica states).
   */
  public void attachBucket(IVrixBucket bucket) {
    String bucketName = bucket.getName();
    SolrCollectionMetadata collectionMetadata = bucket.getSolrMetadata();
    Collection<String> liveSolrNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    List<CollectionsAPIRequest> replicaRequests = new LinkedList<>();

    for (Slice shardMetadata : collectionMetadata.getSlices()) {
      for (Replica replicaMetadata : shardMetadata.getReplicas()) {

        boolean residesOnLiveNode = liveSolrNodes.contains(replicaMetadata.getNodeName());
        boolean isAttached = SolrCollectionMetadata.isReplicaActuallyAttached(
            bucketName, shardMetadata.getName(), replicaMetadata.getCoreName()
        );

        if (!isAttached && residesOnLiveNode) {
          replicaRequests.add(createAttachReplicaRequest(bucketName, shardMetadata, replicaMetadata));
        }
      }
    }

    executeRequestsIgnoringExceptions(replicaRequests);
  }

  /**
   * Detaches an IVrix Bucket.
   * Ignores replicas that are already detached (non-existent in Solr state).
   * Ignores replicas that are from dead nodes.
   */
  public void detachBucket(IVrixBucket bucket) {
    String bucketName = bucket.getName();
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollection(bucketName);
    Collection<String> liveSolrNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    List<CollectionsAPIRequest> replicaRequests = new LinkedList<>();

    for (Slice shardState : collectionState.getSlices()) {
      for (Replica replicaState : shardState.getReplicas()) {

        if (liveSolrNodes.contains(replicaState.getNodeName())) {
          replicaRequests.add(createDetachReplicaRequest(bucketName, shardState, replicaState));
        }
      }
    }

    executeRequestsIgnoringExceptions(replicaRequests);
  }





  private void executeRequestsIgnoringExceptions(Collection<CollectionsAPIRequest> requests) {
    for (CollectionsAPIRequest request : requests) {
      executeRequestIgnoringExceptions(request);
    }
  }

  private void executeRequestIgnoringExceptions(CollectionsAPIRequest request) {
    try {
      solrClient.request(request);
    } catch (Exception e) {
      log.warn("Received error from Collections API. Resuming work, since this error won't hurt the operation. " +
               "However, these errors ideally should not occur, and therefore must be logged: ", e);
      e.printStackTrace();
    }
  }

  private void executeRequest(CollectionsAPIRequest request) throws IOException {
    try {
      solrClient.request(request);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }


  private CollectionsAPIRequest createEmptyCollectionRequest(String collectionName, String shardListString,
                                                             int numberOfShards, int replicationFactor) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put("action", "CREATE");
    paramsMap.put("waitForFinalState", "true");
    paramsMap.put("collection.configName", Constants.IVrix.IVRIX_BUCKET_CONFIG_SET);
    paramsMap.put("createNodeSet", "EMPTY");
    paramsMap.put("name", collectionName);
    paramsMap.put("shards", shardListString);
    paramsMap.put("numShards", String.valueOf(numberOfShards));
    paramsMap.put("maxShardsPerNode", String.valueOf(numberOfShards));
    paramsMap.put("replicationFactor", String.valueOf(replicationFactor));
    return new CollectionsAPIRequest(paramsMap);
  }

  private CollectionsAPIRequest createDeleteCollectionRequest(String collectionName) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put("action", "DELETE");
    paramsMap.put("name", collectionName);
    return new CollectionsAPIRequest(paramsMap);
  }

  private CollectionsAPIRequest createReloadCollectionRequest(String collectionName) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put("action", "RELOAD");
    paramsMap.put("name", collectionName);
    return new CollectionsAPIRequest(paramsMap);
  }


  private CollectionsAPIRequest createAddReplicaRequest(Replica.Type type, String nodeName,
                                                        String collectionName, String shardName) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put("action", "ADDREPLICA");
    paramsMap.put("waitForFinalState", "true");
    paramsMap.put("collection.configName", Constants.IVrix.IVRIX_BUCKET_CONFIG_SET);
    paramsMap.put("type", type.toString().toLowerCase());
    paramsMap.put("collection", collectionName);
    paramsMap.put("shard", shardName);
    paramsMap.put("createNodeSet", nodeName);
    return new CollectionsAPIRequest(paramsMap);
  }

  private CollectionsAPIRequest createDeleteReplicaRequest(String collectionName, Slice shardState, Replica replicaState) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put("action", "DELETEREPLICA");
    paramsMap.put("collection", collectionName);
    paramsMap.put("shard", shardState.getName());
    paramsMap.put("replica", replicaState.getName());
    paramsMap.put("deleteInstanceDir", "true");
    paramsMap.put("deleteDataDir", "true");
    paramsMap.put("deleteIndex", "true");
    return new CollectionsAPIRequest(paramsMap);
  }


  private CollectionsAPIRequest createAttachReplicaRequest(String collectionName, Slice shardState, Replica replicaState) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put("action", "ADDREPLICA");
    paramsMap.put("waitForFinalState", "true");
    paramsMap.put("collection", collectionName);
    paramsMap.put("shard", shardState.getName());
    paramsMap.put("property.dataDir", "../" + replicaState.getCoreName() + "/data");
    paramsMap.put("property.name", replicaState.getCoreName());
    paramsMap.put("createNodeSet", replicaState.getNodeName());
    paramsMap.put("collection.configName", Constants.IVrix.IVRIX_BUCKET_CONFIG_SET);
    return new CollectionsAPIRequest(paramsMap);
  }


  private CollectionsAPIRequest createDetachReplicaRequest(String collectionName, Slice shardState, Replica replicaState) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put("action", "DELETEREPLICA");
    paramsMap.put("collection", collectionName);
    paramsMap.put("shard", shardState.getName());
    paramsMap.put("replica", replicaState.getName());
    paramsMap.put("deleteInstanceDir", "false");
    paramsMap.put("deleteDataDir", "false");
    paramsMap.put("deleteIndex", "false");
    return new CollectionsAPIRequest(paramsMap);
  }


  private static class CollectionsAPIRequest extends SolrRequest<CollectionsAPIResponse> {
    private final Map<String, String> params;
    public CollectionsAPIRequest(Map<String, String> params) {
      super(METHOD.GET, "/admin/collections");
      this.params = params;
    }
    @Override
    public SolrParams getParams() {
      return new MapSolrParams(this.params);
    }
    @Override
    protected CollectionsAPIResponse createResponse(SolrClient client) {
      return new CollectionsAPIResponse();
    }
  }
  private static class CollectionsAPIResponse extends SolrResponseBase {}
}
