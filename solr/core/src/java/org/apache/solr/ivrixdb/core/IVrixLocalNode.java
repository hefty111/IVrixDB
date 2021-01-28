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

package org.apache.solr.ivrixdb.core;

import java.lang.invoke.MethodHandles;
import java.util.*;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.*;
import org.apache.solr.core.*;
import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.election.LeaderElection;
import org.apache.solr.ivrixdb.core.overseer.physical.BucketsPhysicalHandler;
import org.apache.solr.ivrixdb.core.overseer.request.*;
import org.apache.solr.ivrixdb.index.node.HotBucket;
import org.apache.solr.ivrixdb.search.SearchJobRequestHandler;
import org.apache.solr.ivrixdb.search.job.SearchJob;
import org.slf4j.*;

/**
 * This object is the main static container of the logical IVrix Node.
 * It contains loggers, caches, handlers, leader-elected roles (such as
 * the IVrix Overseer), and more. It is responsible for booting up the
 * node, and shutting it down.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: CACHE-TO-REALITY ISSUE -- because the HOT bucket cache does not get re-filled
 *                                 at boot-time, there could potentially be lost HOT buckets
 *                                 that perpetually stay HOT. Right now, this is solved
 *                                 by removing all HOT buckets from node at boot-time.
 *                                 But the better solution, in my opinion, is to re-fill the cache at boot-time.
 */
public final class IVrixLocalNode {
  private IVrixLocalNode() {}

  public static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final BucketsPhysicalHandler bucketsPhysicalHandler = new BucketsPhysicalHandler();
  private static LeaderElection leaderElectionForCollectionsOverseer;
  private static IVrixOverseer ivrixOverseer;
  private static ZkController zkController;

  private static boolean isInitialized = false;
  private static final Map<String, SearchJob> jobStorageMap = new HashMap<>();
  private static final Map<String, HotBucket> hotBucketMap = new HashMap<>();

  /**
   * Boots up the logical IVrix Node. It does so by initializing a new thread that will
   * wait until Solr's CoreContainer has finished booting up and loading all cores.
   * It will also add a hook into the close procedure of the SolrCore of the
   * IVrix Controller in order to shutdown the IVrix Node at the right time.
   */
  public static synchronized void init(CoreContainer solrNode, SolrCore ivrixControllerCore) {
    if (!isInitialized) {
      isInitialized = true;

      solrNode.runAsync(() -> {
        waitForSolrToFinishInitialization(solrNode);
        log.info("Initializing IVrix node " + solrNode.getZkController().getNodeName() + "...");

        zkController = solrNode.getZkController();
        leaderElectionForCollectionsOverseer = LeaderElection.connect(zkController);
        bucketsPhysicalHandler.init(getBaseURL());

        ivrixOverseer = new IVrixOverseer();
        leaderElectionForCollectionsOverseer.addCandidate(ivrixOverseer);
        releasePossibleStaleHotBuckets();
        log.info("IVrix node is initialized.");
      });

      ivrixControllerCore.addCloseHook(new CloseHook() {
        public void preClose(SolrCore core) {}
        public void postClose(SolrCore core) {
          IVrixLocalNode.shutdown();
        }
      });
    }
  }

  private static void waitForSolrToFinishInitialization(CoreContainer solrNode) {
    log.info("Waiting for Solr to finish initializing...");
    long waitForLoadingCoresToFinishMs = 300000L;
    solrNode.waitForLoadingCoresToFinish(waitForLoadingCoresToFinishMs);
  }

  private static void releasePossibleStaleHotBuckets() {
    log.info("Ensuring that solr node is free of stale (i.e., old and not rolled) hot buckets...");
    try {
      IVrixOverseerClient.execute(IVrixOverseerOperationParams.FreeHotBucketsInNode());
    } catch (Exception e) {
      e.printStackTrace();
      log.error("failed to ensure that booted solr node is free of stale (i.e., old and not rolled) hot buckets");
    }
  }

  /**
   * Shuts down the IVrix Node and its related objects
   */
  public static void shutdown() {
    log.info("Shutting down IVrix node...");
    SearchJobRequestHandler.close();
    log.info("IVrix node has been shutdown.");
  }



  /**
   * Stores a new Search Job into the in-memory cache
   */
  public static void storeSearchJob(SearchJob searchJob) {
    jobStorageMap.put(searchJob.getJobID(), searchJob);
  }

  /**
   * Stores a new HOT bucket into the in-memory cache
   */
  public static void storeHotBucket(String indexName, HotBucket bucket) {
    hotBucketMap.put(indexName, bucket);
  }

  /**
   * Removes a HOT bucket from the in-memory cache
   */
  public static void removeHotBucket(String indexName) {
    hotBucketMap.remove(indexName);
  }



  /**
   * @return A Search Job from the in-memory cache
   */
  public static SearchJob getSearchJob(String jobID) {
    return jobStorageMap.get(jobID);
  }

  /**
   * @return A HOT bucket from the in-memory cache
   */
  public static HotBucket getHotBucket(String indexName) {
    return hotBucketMap.getOrDefault(indexName, null);
  }

  /**
   * @return A handler that handles physical buckets (creation, deletion, attachment, detachment, etc)
   */
  public static BucketsPhysicalHandler getBucketsPhysicalHandler() {
    return bucketsPhysicalHandler;
  }

  /**
   * @return The Leader Election object responsible for IVrix Overseer election
   */
  public static LeaderElection getOverseerElection() {
    return leaderElectionForCollectionsOverseer;
  }

  /**
   * @return The IVrix Overseer
   */
  public static IVrixOverseer getIVrixOverseer() {
    return ivrixOverseer;
  }

  /**
   * @return Is the IVrix Node initialized
   */
  public static boolean isInitialized() {
    return isInitialized;
  }



  /**
   * @return Is the Solr Node shutting down
   */
  public static boolean isShuttingDown() {
    return zkController.getCoreContainer().isShutDown();
  }

  /**
   * @return The base URL of the Solr Node (example: "http://localhost:8983/solr")
   */
  public static String getBaseURL() {
    return zkController.getBaseUrl();
  }

  /**
   * @return The name of the Solr Node (example: "127.0.0.1:8983_solr")
   */
  public static String getName() {
    return zkController.getNodeName();
  }

  /**
   * @return Solr's Zookeeper Controller object
   */
  public static ZkController getZkController() {
    return zkController;
  }

  /**
   * @return Solr's Cluster State object
   */
  public static ClusterState getClusterState() {
    return zkController.getClusterState();
  }

  /**
   * @return Solr's Core Container object
   */
  public static CoreContainer getCoreContainer() {
    return zkController.getCoreContainer();
  }

  /**
   * @return Solr's client to the Zookeeper
   */
  public static SolrZkClient getZkClient() {
    return zkController.getZkClient();
  }
}
