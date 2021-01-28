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

package org.apache.solr.ivrixdb.core.overseer.request;

import java.util.*;

import org.apache.solr.common.util.Utils;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.TimeBounds;

import static org.apache.solr.ivrixdb.core.overseer.request.IVrixOverseerOperationParams.OperationParam.*;

/**
 * This object represents an IVrix Overseer Operation request
 * and its parameters. IT IS NOT the actual operation, nor the
 * thread of the operation. Rather, it is an encapsulation of
 * the request for an operation. It is used by creating the operation
 * type with its corresponding parameters, and then sent over to the
 * IVrixOverseerClient, which sends it to the IVrixOverseer.
 *
 * To do that, this object is capable of serializing/de-serializing itself
 * in the form of a JSON string.
 *
 * @author Ivri Faitelson
 */
public class IVrixOverseerOperationParams {
  /**
   * Defines a parameter for an Operation Type
   */
  public enum OperationParam {
    OPERATION_TYPE_PARAM("operationType"),
    INDEX_NAME_PARAM("indexName"),
    Bucket_NAME_PARAM("bucketName"),
    TIME_BOUNDS_PARAM("timeBounds"),
    SEARCH_JOB_ID_PARAM("searchJobID"),
    REQUESTING_NODE_NAME_PARAM("requestingNodeName"),
    REPLICATION_FACTOR_PARAM("replicationFactor");

    public final String paramString;
    OperationParam(String paramString) {
      this.paramString = paramString;
    }
  }

  /**
   * Defines an Operation Type and what Operation Parameters it requires
   */
  public enum OperationType {
    GET_ALL_IVRIX_INDEXES(),
    CREATE_IVRIX_INDEX(INDEX_NAME_PARAM),
    DELETE_IVRIX_INDEX(INDEX_NAME_PARAM),
    FREE_HOT_BUCKETS_IN_NODE(REQUESTING_NODE_NAME_PARAM),
    CREATE_BUCKET(REQUESTING_NODE_NAME_PARAM, INDEX_NAME_PARAM, REPLICATION_FACTOR_PARAM),
    UPDATE_BUCKET_METADATA(INDEX_NAME_PARAM, Bucket_NAME_PARAM, TIME_BOUNDS_PARAM),
    ROLL_HOT_BUCKET_TO_WARM(INDEX_NAME_PARAM, Bucket_NAME_PARAM),
    GET_BUCKETS_WITHIN_BOUNDS(INDEX_NAME_PARAM, TIME_BOUNDS_PARAM),
    HOLD_BUCKET_FOR_SEARCH(INDEX_NAME_PARAM, Bucket_NAME_PARAM, SEARCH_JOB_ID_PARAM, REQUESTING_NODE_NAME_PARAM),
    RELEASE_BUCKET_HOLD(INDEX_NAME_PARAM, Bucket_NAME_PARAM, SEARCH_JOB_ID_PARAM, REQUESTING_NODE_NAME_PARAM);

    public final OperationParam[] supportedParams;
    OperationType(OperationParam... params) {
      this.supportedParams = params;
    }

    /**
     * @return Whether the Operation Type has/can-retrieve the requested Operation Parameter
     */
    public boolean canAccessParam(OperationParam param) {
      boolean isPermittedAccess = false;
      for (OperationParam supportedParam : supportedParams) {
        if (supportedParam == param) {
          isPermittedAccess = true;
          break;
        }
      }
      return isPermittedAccess;
    }
  }

  private final OperationType type;
  protected final Map<String, Object> rawParams;

  /**
   * @return The value of the index name parameter
   * @throws IllegalAccessException If the Operation's Operation Type does not have the requested Operation Parameter
   */
  public String getIndexName() throws IllegalAccessException {
    checkIfAllowedAccessToParam(INDEX_NAME_PARAM);
    return (String)rawParams.get(INDEX_NAME_PARAM.paramString);
  }

  /**
   * @return The value of the bucket name parameter
   * @throws IllegalAccessException If the Operation's Operation Type does not have the requested Operation Parameter
   */
  public String getBucketName() throws IllegalAccessException {
    checkIfAllowedAccessToParam(Bucket_NAME_PARAM);
    return (String)rawParams.get(Bucket_NAME_PARAM.paramString);
  }

  /**
   * @return The value of the time bounds parameter
   * @throws IllegalAccessException If the Operation's Operation Type does not have the requested Operation Parameter
   */
  public TimeBounds getTimeBounds() throws IllegalAccessException {
    checkIfAllowedAccessToParam(TIME_BOUNDS_PARAM);
    return TimeBounds.fromMap((Map<String, Object>)rawParams.get(TIME_BOUNDS_PARAM.paramString));
  }

  /**
   * @return The value of the Search Job ID parameter
   * @throws IllegalAccessException If the Operation's Operation Type does not have the requested Operation Parameter
   */
  public String getSearchJobId() throws IllegalAccessException {
    checkIfAllowedAccessToParam(SEARCH_JOB_ID_PARAM);
    return (String)rawParams.get(SEARCH_JOB_ID_PARAM.paramString);
  }

  /**
   * @return The value of the requesting node name parameter
   * @throws IllegalAccessException If the Operation's Operation Type does not have the requested Operation Parameter
   */
  public String getRequestingNodeName() throws IllegalAccessException {
    checkIfAllowedAccessToParam(REQUESTING_NODE_NAME_PARAM);
    return (String)rawParams.get(REQUESTING_NODE_NAME_PARAM.paramString);
  }

  /**
   * @return The value of the replication factor parameter
   * @throws IllegalAccessException If the Operation's Operation Type does not have the requested Operation Parameter
   */
  public Integer getReplicationFactor() throws IllegalAccessException {
    checkIfAllowedAccessToParam(REPLICATION_FACTOR_PARAM);
    return Integer.valueOf((String)rawParams.get(REPLICATION_FACTOR_PARAM.paramString));
  }

  /**
   * @return The Operation's type
   */
  public OperationType getOperationType() {
    return type;
  }

  private void checkIfAllowedAccessToParam(OperationParam param) throws IllegalAccessException {
    boolean isPermittedAccess = type.canAccessParam(param);
    if (!isPermittedAccess) {
      throw new IllegalAccessException("this operation type does not have this param!");
    }
  }





  private IVrixOverseerOperationParams(OperationType type, Map<String, Object> rawParams) {
    this.type = type;
    this.rawParams = rawParams;
  }

  /**
   * @return An Operation from a JSON string that contains Operation properties
   */
  public static IVrixOverseerOperationParams fromJSONString(String paramsString) {
    Map<String, Object> rawParams = (Map<String, Object>)Utils.fromJSONString(paramsString);
    OperationType operationType = OperationType.valueOf((String)rawParams.get(OPERATION_TYPE_PARAM.paramString));
    return new IVrixOverseerOperationParams(operationType, rawParams);
  }

  /**
   * @return A JSON String representation that contains Operation properties from the requested Operation
   */
  public static String toJSONString(IVrixOverseerOperationParams params) {
    return Utils.toJSONString(params.rawParams);
  }

  /**
   * @return A JSON String representation of the Operation without the Operation Type
   */
  @Override
  public String toString() {
    HashMap<String, Object> paramsOnlyMap = new HashMap<>(rawParams);
    paramsOnlyMap.remove(OPERATION_TYPE_PARAM.paramString);
    return Utils.toJSONString(paramsOnlyMap)
        .replaceAll("\n", "")
        .replaceAll(" ", "");
  }


  
  private IVrixOverseerOperationParams(OperationType type) {
    this.type = type;
    this.rawParams = new HashMap<>();
    rawParams.put(OPERATION_TYPE_PARAM.paramString, type.toString());
  }

  private IVrixOverseerOperationParams(OperationType type, String indexName) {
    this(type);
    rawParams.put(INDEX_NAME_PARAM.paramString, indexName);
  }

  private IVrixOverseerOperationParams(OperationType type, String indexName, String bucketName) {
    this(type, indexName);
    rawParams.put(Bucket_NAME_PARAM.paramString, bucketName);

  }

  private IVrixOverseerOperationParams(OperationType type, String indexName, String bucketName,
                                       String searchJobID, String requestingNodeName) {
    this(type, indexName, bucketName);
    rawParams.put(SEARCH_JOB_ID_PARAM.paramString, searchJobID);
    rawParams.put(REQUESTING_NODE_NAME_PARAM.paramString, requestingNodeName);
  }

  /**
   * @return A CreateISolrIndex Operation with the requested parameters.
   *          This operation creates an IVrix Index.
   */
  public static IVrixOverseerOperationParams CreateIVrixIndex(String indexName) {
    return new IVrixOverseerOperationParams(OperationType.CREATE_IVRIX_INDEX, indexName);
  }

  /**
   * @return A DeleteISolrIndex Operation with the requested parameters.
   *          This operation deletes an IVrix Index.
   */
  public static IVrixOverseerOperationParams DeleteIVrixIndex(String indexName) {
    return new IVrixOverseerOperationParams(OperationType.DELETE_IVRIX_INDEX, indexName);
  }

  /**
   * @return A GetAllISolrIndexes Operation with the requested parameters.
   *          This operation retrieves all IVrix Indexes that exist.
   */
  public static IVrixOverseerOperationParams GetAllIVrixIndexes() {
    return new IVrixOverseerOperationParams(OperationType.GET_ALL_IVRIX_INDEXES);
  }

  /**
   * @return A HoldBucketForSearch Operation with the requested parameters.
   *          This operation holds an IVrix Bucket, so that it does not get lost during search.
   *          Potentially induces attachment and detachment commands.
   */
  public static IVrixOverseerOperationParams HoldBucketForSearch(String indexName, String bucketName, String searchJobID) {
    return new IVrixOverseerOperationParams(
        OperationType.HOLD_BUCKET_FOR_SEARCH, indexName, bucketName, searchJobID, IVrixLocalNode.getName()
    );
  }

  /**
   * @return A ReleaseBucketHold Operation with the requested parameters.
   *          This operation releases a hold on an IVrix Bucket.
   */
  public static IVrixOverseerOperationParams ReleaseBucketHold(String indexName, String bucketName, String searchJobID) {
    return new IVrixOverseerOperationParams(
        OperationType.RELEASE_BUCKET_HOLD, indexName, bucketName, searchJobID, IVrixLocalNode.getName()
    );
  }

  /**
   * @return A GetBucketsForSearchJob Operation with the requested parameters.
   *          This operation retrieves an organized list of bucket names for a search.
   *          see {@link org.apache.solr.ivrixdb.core.overseer.response.OrganizedBucketsForSearch}
   */
  public static IVrixOverseerOperationParams GetBucketsForSearchJob(String indexName, TimeBounds timeBounds) {
    IVrixOverseerOperationParams operationParams = new IVrixOverseerOperationParams(
        OperationType.GET_BUCKETS_WITHIN_BOUNDS, indexName
    );
    operationParams.rawParams.put(TIME_BOUNDS_PARAM.paramString, timeBounds.toMap());
    return operationParams;
  }

  /**
   * @return A CreateBucket Operation with the requested parameters.
   *          This operation creates an IVrix Bucket for an IVrix Index.
   *          Potentially induces index rollover commands.
   */
  public static IVrixOverseerOperationParams CreateBucket(String indexName, int replicationFactor) {
    IVrixOverseerOperationParams operationParams =  new IVrixOverseerOperationParams(
        OperationType.CREATE_BUCKET, indexName
    );
    operationParams.rawParams.put(REPLICATION_FACTOR_PARAM.paramString, String.valueOf(replicationFactor));
    operationParams.rawParams.put(REQUESTING_NODE_NAME_PARAM.paramString, IVrixLocalNode.getName());
    return operationParams;
  }

  /**
   * @return A UpdateBucketMetadata Operation with the requested parameters.
   *          This operation updates the persistent metadata of an IVrix Bucket (like time bounds, for instance.)
   */
  public static IVrixOverseerOperationParams UpdateBucketMetadata(String indexName, String bucketName, TimeBounds timeBounds) {
    IVrixOverseerOperationParams operationParams =  new IVrixOverseerOperationParams(
        OperationType.UPDATE_BUCKET_METADATA, indexName, bucketName
    );
    operationParams.rawParams.put(TIME_BOUNDS_PARAM.paramString, timeBounds.toMap());
    return operationParams;
  }

  /**
   * @return A RollHotBucketToWarm Operation with the requested parameters
   *          This operation rolls a HOT bucket to WARM.
   */
  public static IVrixOverseerOperationParams RollHotBucketToWarm(String indexName, String bucketName) {
    return new IVrixOverseerOperationParams(OperationType.ROLL_HOT_BUCKET_TO_WARM, indexName, bucketName);
  }

  /**
   * @return A FreeHotBucketsInNode Operation with the requested parameters.
   *          This operation rolls all HOT buckets in an IVrix Node to WARM.
   */
  public static IVrixOverseerOperationParams FreeHotBucketsInNode() {
    IVrixOverseerOperationParams operationParams = new IVrixOverseerOperationParams(OperationType.FREE_HOT_BUCKETS_IN_NODE);
    operationParams.rawParams.put(REQUESTING_NODE_NAME_PARAM.paramString, IVrixLocalNode.getName());
    return operationParams;
  }
}