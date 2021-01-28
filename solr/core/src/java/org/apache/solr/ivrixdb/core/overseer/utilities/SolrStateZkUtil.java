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

package org.apache.solr.ivrixdb.core.overseer.utilities;

import java.io.IOException;
import java.util.*;

import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Utils;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket;
import org.apache.zookeeper.KeeperException;

/**
 * This class is a collection of functions that directly modify the
 * Solr State in Zookeeper. These are usually done to correct
 * issues in the Solr State.
 *
 * @author Ivri Faitelson
 */
public class SolrStateZkUtil {
  private static final String SOLR_COLLECTIONS_PATH = "/collections";
  private static final String COLLECTION_TERMS_PATH = "/terms";

  /**
   * Cleans up the leader election of replicas in a bucket in order to
   * fix the issue of attaching a bucket that was affected by node death
   * in a way that it still has removed replicas in shard leader election.
   */
  public static void cleanReplicaLeaderElectionFromRemovedReplicas(IVrixBucket bucket) throws IOException {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollection(bucket.getName());
    for (Slice shard : collectionState.getSlices()) {
      cleanReplicaLeaderElectionFromRemovedReplicas(collectionState, shard);
    }
  }

  private static void cleanReplicaLeaderElectionFromRemovedReplicas(DocCollection collectionState, Slice shardState) throws IOException {
    String collectionName = collectionState.getName();
    String shardName = shardState.getName();

    Map<String, Object> collectionTerms = getCollectionTerms(collectionName, shardName);

    List<String> bucketReplicaNames = new LinkedList<>();
    for (Replica replica : collectionState.getReplicas()) {
      bucketReplicaNames.add(replica.getName());
    }
    for (String key : collectionTerms.keySet()) {
      if (!bucketReplicaNames.contains(key)) {
        collectionTerms.remove(key);
      }
    }

    setCollectionTerms(collectionName, shardName, collectionTerms);
  }



  private static Map<String, Object> getCollectionTerms(String collectionName, String shardName) throws IOException {
    String collectionTermsPath = SOLR_COLLECTIONS_PATH + "/" + collectionName + COLLECTION_TERMS_PATH + "/" + shardName;
    Map<String, Object> collectionTerms;
    try {
      byte[] rawData = IVrixLocalNode.getZkClient().getData(collectionTermsPath, null, null, true);
      collectionTerms = deserializeBlob(rawData);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
    return collectionTerms;
  }

  private static void setCollectionTerms(String collectionName, String shardName, Map<String, Object> collectionTerms) throws IOException {
    String collectionTermsPath = SOLR_COLLECTIONS_PATH + "/" + collectionName + COLLECTION_TERMS_PATH + "/" + shardName;
    try {
      IVrixLocalNode.getZkClient().setData(collectionTermsPath, serializeBlob(collectionTerms), true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }



  private static byte[] serializeBlob(Map<String, Object> metadata) {
    return Utils.toJSON(new ZkNodeProps(metadata));
  }

  private static Map<String, Object> deserializeBlob(byte[] data) {
    return ZkNodeProps.load(data).getProperties();
  }
}
