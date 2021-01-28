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

package org.apache.solr.ivrixdb.core.overseer.state.metadata;

import java.util.*;

import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.Utils;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;

/**
 * This object extends the Solr Doc Collection object
 * that represents a Solr Collection in the Solr state.
 * This was done in order to be able to copy, serialize,
 * and de-serialize the object for IVrixDB's state.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: refactoring idea -- move "isReplicaActuallyAttached" to a more appropriate location
 */
public class SolrCollectionMetadata extends DocCollection {
  private SolrCollectionMetadata(String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router) {
    super(name, slices, props, router);
  }

  /**
   * @return Whether a Replica of a given Core name for a given collection and shard names is actually attached in the Solr state,
   *          meaning it is exists and the properties are there. It does not mean that the Replica sits on a live node,
   *          or that it is healthy.
   */
  public static boolean isReplicaActuallyAttached(String collectionName, String shardName, String replicaCoreName) {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollectionOrNull(collectionName);
    if (collectionState == null) {
      return false;
    }

    boolean isActuallyAttached = false;
    for (Replica attachedReplica : collectionState.getSlice(shardName)) {
      if (Objects.equals(attachedReplica.getCoreName(), replicaCoreName)) {
        isActuallyAttached = true;
      }
    }
    return isActuallyAttached;
  }

  /**
   * @return A copy of the given Solr Doc Collection object
   */
  public static SolrCollectionMetadata copy(DocCollection toCopy) {
    if (toCopy == null) {
      return null;
    }
    String name = toCopy.getName();
    Map<String, Slice> slices = toCopy.getSlicesMap();
    Map<String, Object> props = toCopy.getProperties();
    DocRouter router = toCopy.getRouter();
    return new SolrCollectionMetadata(name, slices, props, router);
  }

  /**
   * @return A SolrCollectionMetadata object from a Map with the properties of the collection represented as Maps, Strings, and Integers
   */
  // This block of code was taken from ClusterState.collectionFromObjects (since it is a private function)
  // It may have been moved to DocCollection.loadFromMap, as a to-do comment in collectionFromObjects(...) suggested.
  public static SolrCollectionMetadata fromMap(String collectionName, Map<String, Object> objs) {
    Map<String,Object> props;
    Map<String,Slice> slices;
    Map<String,Object> sliceObjs = (Map<String,Object>)objs.get(DocCollection.SHARDS);
    if (sliceObjs == null) {
      // legacy format from 4.0... there was no separate "shards" level to contain the collection shards.
      slices = Slice.loadAllFromMap(objs);
      props = Collections.emptyMap();
    } else {
      slices = Slice.loadAllFromMap(sliceObjs);
      props = new HashMap<>(objs);
      objs.remove(DocCollection.SHARDS);
    }
    Object routerObj = props.get(DocCollection.DOC_ROUTER);
    DocRouter router;
    if (routerObj == null) {
      router = DocRouter.DEFAULT;
    } else if (routerObj instanceof String) {
      // back compat with Solr4.4
      router = DocRouter.getDocRouter((String)routerObj);
    } else {
      Map routerProps = (Map)routerObj;
      router = DocRouter.getDocRouter((String) routerProps.get("name"));
    }
    return new SolrCollectionMetadata(collectionName, slices, props, router);
  }

  /**
   * @return A Map with the properties of the collection represented as Maps, Strings, and Integers
   */
  public Map<String, Object> toMap() {
    byte[] rawMapData = Utils.toJSON(this);
    return ZkNodeProps.load(rawMapData).getProperties();
  }
}
