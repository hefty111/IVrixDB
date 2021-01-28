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

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.*;

/**
 * This class is a collection of functions that update the leader election state.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: I have a hunch that the reason there are duplicate watchers/events sometimes is because
 *       of the "tryOnConnLoss=true" parameter that is placed on almost every call to ZK...
 *       Figure out if this is actually the case
 */
public class ElectionZkUtil {
  private static final String ELECTION_PATH = "/ivrix_overseer/leader_election";

  /**
   * Ensures the Zookeeper path for the leader election exists
   */
  public static void ensureElectionPathExists(SolrZkClient zkClient) throws IOException {
    try {
      zkClient.makePath(ELECTION_PATH, false, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return The created ZK node under the leader election path for a leader election candidate
   */
  public static String createCandidateNode(SolrZkClient zkClient) throws IOException {
    String candidateIDWithoutRank = CandidateIdUtil.generateCandidateIDWithoutRank(zkClient);
    String candidateNodePath = ELECTION_PATH + "/" + candidateIDWithoutRank + "-n_";

    String fullPath;
    try {
      fullPath = zkClient.create(candidateNodePath, null, CreateMode.EPHEMERAL_SEQUENTIAL, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
    return CandidateIdUtil.extractCandidateIDFromFullPath(fullPath);
  }

  /**
   * Removes the ZK node of a leader election candidate from the leader election path
   */
  public static void removeCandidateNode(SolrZkClient zkClient, String candidateID) throws IOException {
    String candidateNodePath = ELECTION_PATH + "/" + candidateID;
    try {
      zkClient.delete(candidateNodePath, -1, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates a ZK Watcher for a leader election candidate (i.e, watches the candidate)
   */
  public static void watchCandidate(SolrZkClient zkClient, String candidateID, Watcher watcher) throws IOException {
    String selfNodePath = ELECTION_PATH + "/" + candidateID;
    try {
      zkClient.getData(selfNodePath, watcher, null, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return The list of all leader election candidate names (their IDs)
   */
  public static List<String> getAllCandidateIDs(SolrZkClient zkClient) throws IOException {
    return getAllCandidateIDs(zkClient, null);
  }

  /**
   * not only retrieves the candidate IDs, but also watches for any changes in the election as a whole
   * @return The list of all leader election candidate names (their IDs)
   */
  public static List<String> getAllCandidateIDsAndWatch(SolrZkClient zkClient, Watcher watcher) throws IOException {
    return getAllCandidateIDs(zkClient, watcher);
  }

  private static List<String> getAllCandidateIDs(SolrZkClient zkClient, Watcher watcher) throws IOException {
    List<String> candidateIDs;
    try {
      candidateIDs = zkClient.getChildren(ELECTION_PATH, watcher, true);
      candidateIDs.sort(Comparator.comparingInt(CandidateIdUtil::getCandidateRankFromID).thenComparing(o -> o));
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
    return candidateIDs;
  }
}
