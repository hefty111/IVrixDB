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

import java.io.*;
import java.util.*;

import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.Utils;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket;
import org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketType;
import org.apache.zookeeper.*;

/**
 * This class is a collection of functions that update IVrixDB's persistent
 * state/metadata, which includes IVrix Indexes and IVrix Buckets.
 *
 * @author Ivri Faitelson
 */
public class IVrixStateZkUtil {
  public static final String BUCKET_TYPE_KEY = "bucketType";
  public static final String IS_DETACHED_KEY = "isDetached";
  public static final String TIME_BOUNDS_KEY = "timeBounds";
  public static final String INDEXER_NODE_NAME_KEY = "indexerNodeName";
  public static final String SOLR_COLLECTION_METADATA_KEY = "solrCollectionMetadata";
  private static final String PERSISTENT_STATE_PATH = "/ivrix_buckets";

  /**
   * Ensures the Zookeeper path for the persistent state exists
   */
  public static void ensurePersistentStatePathExists() throws IOException {
    try {
      IVrixLocalNode.getZkClient().makePath(PERSISTENT_STATE_PATH, false, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }



  /**
   * Creates a path for an IVrix Index
   */
  public static void createIVrixIndexPath(String indexName) throws IOException {
    String ivrixIndexPath = PERSISTENT_STATE_PATH + "/" + indexName;
    try {
      IVrixLocalNode.getZkClient().makePath(ivrixIndexPath, false, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Deletes a path (and all subsequent sub-paths) of an IVrix Index
   */
  public static void deleteIVrixIndexPath(String indexName) throws IOException {
    String ivrixIndexPath = PERSISTENT_STATE_PATH + "/" + indexName;
    try {
      IVrixLocalNode.getZkClient().clean(ivrixIndexPath);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Deletes a path (and all subsequent sub-paths) of an IVrix Bucket
   */
  public static void deleteIVrixBucketPath(IVrixBucket bucket) throws IOException {
    String ivrixBucketPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName();
    try {
      IVrixLocalNode.getZkClient().clean(ivrixBucketPath);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }



  /**
   * @return A list of the names of all persistent IVrix Indexes
   */
  public static List<String> retrieveAllPersistedIndexes() throws IOException {
    List<String> bucketNames;
    try {
      bucketNames = IVrixLocalNode.getZkClient().getChildren(PERSISTENT_STATE_PATH, null, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
    return bucketNames;
  }

  /**
   * @return A list of the names of all persistent IVrix Buckets for an IVrix Index
   */
  public static List<String> retrieveAllPersistedBucketNames(String indexName) throws IOException {
    List<String> bucketNames;
    try {
      bucketNames = IVrixLocalNode.getZkClient().getChildren(PERSISTENT_STATE_PATH + "/" + indexName, null, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
    return bucketNames;
  }



  /**
   * Creates a ZK node for an IVrix Bucket and stores its main metadata as a blob.
   * If the node already exists, it will update the ZK node with the current main metadata.
   */
  public static void createBucketBlobNode(IVrixBucket bucket) throws IOException {
    Map<String, Object> metadataBlob = bucket.getMainMetadataBlob();

    SolrZkClient zkClient = IVrixLocalNode.getZkClient();
    String collectionMetadataPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName();
    byte[] serializedBlob = serializeBlob(metadataBlob);
    try {
      zkClient.create(collectionMetadataPath, serializedBlob, CreateMode.PERSISTENT, true);
    } catch (InterruptedException | KeeperException e) {
      updateBucketBlobNode(bucket);
    }
  }

  /**
   * updates the ZK node of an IVrix Bucket with the main metadata
   */
  public static void updateBucketBlobNode(IVrixBucket bucket) throws IOException {
    Map<String, Object> metadataBlob = bucket.getMainMetadataBlob();

    SolrZkClient zkClient = IVrixLocalNode.getZkClient();
    String collectionMetadataPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName();
    byte[] serializedBlob = serializeBlob(metadataBlob);
    try {
      zkClient.setData(collectionMetadataPath, serializedBlob, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return The main metadata of an IVrix Bucket
   */
  public static Map<String, Object> retrieveBucketBlob(String indexName, String bucketName) throws IOException {
    String propertyPath = PERSISTENT_STATE_PATH + "/" + indexName + "/" + bucketName;
    Map<String, Object> blob;
    try {
      byte[] rawData = IVrixLocalNode.getZkClient().getData(propertyPath, null, null, true);
      blob = deserializeBlob(rawData);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
    return blob;
  }



  /**
   * Creates a bucket type sub-ZK-node for an IVrix Bucket and stores the bucket's type in there.
   * If the node already exists, it will update the ZK node with the current bucket type.
   */
  public static void createBucketTypePropertyNode(IVrixBucket bucket) throws IOException {
    createBucketPropertyNode(bucket, BUCKET_TYPE_KEY, bucket.getBucketType().name());
  }

  /**
   * Updates a bucket type sub-ZK-node for an IVrix Bucket with the current bucket type
   */
  public static void updateBucketTypePropertyNode(IVrixBucket bucket) throws IOException {
    updateBucketPropertyNode(bucket, BUCKET_TYPE_KEY, bucket.getBucketType().name());
  }

  /**
   * @return The bucket type of an IVrix Bucket
   */
  public static BucketType retrieveBucketTypePropertyValue(String indexName, String bucketName) throws IOException {
    String bucketTypeSting = (String) retrieveBucketProperty(indexName, bucketName, BUCKET_TYPE_KEY);
    return BucketType.valueOf(bucketTypeSting);
  }



  /**
   * Creates an is-detached sub-ZK-node for an IVrix Bucket and stores the attachment state in there.
   * If the node already exists, it will update the ZK node with the current attachment state.
   */
  public static void createIsDetachedPropertyNode(IVrixBucket bucket) throws IOException {
    createBucketPropertyNode(bucket, IS_DETACHED_KEY, bucket.isDetached());
  }

  /**
   * Updates an is-detached sub-ZK-node for an IVrix Bucket with the current attachment state
   */
  public static void updateIsDetachedPropertyNode(IVrixBucket bucket) throws IOException {
    updateBucketPropertyNode(bucket, IS_DETACHED_KEY, bucket.isDetached());
  }

  /**
   * @return The attachment (is-detached) state of an IVrix Bucket
   */
  public static boolean retrieveIsDetachedPropertyValue(String indexName, String bucketName) throws IOException {
    return (boolean) retrieveBucketProperty(indexName, bucketName, IS_DETACHED_KEY);
  }



  private static void createBucketPropertyNode(IVrixBucket bucket, String propertyName, Object propertyValue) throws IOException {
    String indexName = bucket.getIndexName();
    String bucketName = bucket.getName();

    try {
      byte[] rawData = serialize(propertyValue);
      String propertyPath = PERSISTENT_STATE_PATH + "/" + indexName + "/" + bucketName + "/" + propertyName;
      IVrixLocalNode.getZkClient().create(propertyPath, rawData, CreateMode.PERSISTENT, true);
    } catch (InterruptedException | KeeperException e) {
      updateBucketPropertyNode(bucket, propertyName, propertyValue);
    }
  }

  private static void updateBucketPropertyNode(IVrixBucket bucket, String propertyName, Object propertyValue) throws IOException {
    String indexName = bucket.getIndexName();
    String bucketName = bucket.getName();

    try {
      byte[] rawData = serialize(propertyValue);
      String propertyPath = PERSISTENT_STATE_PATH + "/" + indexName + "/" + bucketName + "/" + propertyName;
      IVrixLocalNode.getZkClient().setData(propertyPath, rawData, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  private static Object retrieveBucketProperty(String indexName, String bucketName, String propertyName) throws IOException {
    String propertyPath = PERSISTENT_STATE_PATH + "/" + indexName + "/" + bucketName + "/" + propertyName;
    Object propertyValue;
    try {
      byte[] rawData = IVrixLocalNode.getZkClient().getData(propertyPath, null, null, true);
      propertyValue = deserialize(rawData);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }

    return propertyValue;
  }



  private static byte[] serializeBlob(Map<String, Object> metadata) {
    return Utils.toJSON(new ZkNodeProps(metadata));
  }

  private static Map<String, Object> deserializeBlob(byte[] data) {
    return ZkNodeProps.load(data).getProperties();
  }

  private static byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream outputByteStream = new ByteArrayOutputStream();
    ObjectOutputStream outputObjectStream = new ObjectOutputStream(outputByteStream);
    outputObjectStream.writeObject(obj);
    return outputByteStream.toByteArray();
  }

  private static Object deserialize(byte[] data) throws IOException {
    ByteArrayInputStream inputByteStream = new ByteArrayInputStream(data);
    ObjectInputStream inputObjectStream = new ObjectInputStream(inputByteStream);
    try {
      return inputObjectStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}
