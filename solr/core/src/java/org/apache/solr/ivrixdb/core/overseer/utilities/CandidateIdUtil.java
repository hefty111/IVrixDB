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

import java.util.*;
import java.util.regex.*;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.election.LeaderElectionCandidate;

/**
 * This class is a collection of functions that create
 * and manipulate leader election candidate IDs.
 *
 * @author Ivri Faitelson
 */
public class CandidateIdUtil {
  private static final Pattern CANDIDATE_RANK_PATTERN = Pattern.compile(".*?/?.*?-n_(\\d+)");
  private static final Pattern NODE_URL_PATTERN = Pattern.compile(".*?/?(.*?-)(.*?)-n_\\d+");

  /**
   * @return The leader candidate ID from the list of all candidate IDs
   */
  public static String getElectedLeaderID(List<String> allCandidateIDs) {
    if (allCandidateIDs.size() > 0) {
      return allCandidateIDs.get(0);
    } else {
      return null;
    }
  }

  /**
   * @return The created candidate ID, without the tacked-on rank that Zookeeper will provide
   */
  public static String generateCandidateIDWithoutRank(SolrZkClient zkClient) {
    return IVrixLocalNode.getName() + "-" + zkClient.getSolrZooKeeper().getSessionId();
  }

  /**
   * @return The extracted candidate ID from the ZK path of a candidate ZK Node
   */
  public static String extractCandidateIDFromFullPath(String fullPath) {
    return fullPath.substring(fullPath.lastIndexOf('/') + 1);
  }

  /**
   * @return The candidate's base URL from the candidate's ID (example: "http://localhost:8983/solr")
   */
  public static String extractBaseUrlFromID(String candidateID) {
    Matcher patternMatcher = NODE_URL_PATTERN.matcher(candidateID);
    String baseURL = "";
    if (patternMatcher.matches()) {
      baseURL = patternMatcher.group(1);
      baseURL = baseURL.replace("-", "");
      baseURL = baseURL.replace("_", "/");
      baseURL = "http://" + baseURL;
    } else {
      IVrixLocalNode.log.warn("Could not find regex match in:" + candidateID);
    }
    return baseURL;
  }

  /**
   * @return The candidate's Node Name from the candidate's ID (example: "127.0.0.1:8983_solr")
   */
  public static String extractNodeNameFromID(String candidateID) {
    Matcher patternMatcher = NODE_URL_PATTERN.matcher(candidateID);
    String nodeName = "";
    if (patternMatcher.matches()) {
      nodeName = patternMatcher.group(1);
      nodeName = nodeName.replace("-", "");
    } else {
      IVrixLocalNode.log.warn("Could not find regex match in:" + candidateID);
    }
    return nodeName;
  }

  /**
   * @return The rank of the candidate that was given by
   *          Zookeeper when the candidate's ID was added to the election path
   */
  protected static int getCandidateRankFromID(String candidateID) {
    Matcher patternMatcher = CANDIDATE_RANK_PATTERN.matcher(candidateID);
    int rank = 0;
    if (patternMatcher.matches()) {
      rank = Integer.parseInt(patternMatcher.group(1));
    } else {
      IVrixLocalNode.log.warn("Could not find regex match in:" + candidateID);
    }
    return rank;
  }



  /**
   * @return The candidate ID of the candidate right in-front of the requested candidate
   */
  public static String findNextCandidateID(LeaderElectionCandidate candidate, List<String> allCandidateIDs) {
    String candidateID = candidate.getID();
    String nextCandidateID = allCandidateIDs.get(0);

    for (String id : allCandidateIDs) {
      if (candidateID.equals(id)) {
        break;
      }
      nextCandidateID = id;
    }
    return nextCandidateID;
  }

  /**
   * @return The type of change that occurred between the
   *          old list of candidate IDs, and the new list of candidate IDs
   */
  public static LeaderElectionCandidate.ChangeType findChangeTypeInCandidates(List<String> oldIDs, List<String> newIDs) {
    int oldNumberOfSlaves = oldIDs.size();
    int newNumberOfSlaves = newIDs.size();
    boolean wasChildCreated = oldNumberOfSlaves < newNumberOfSlaves;
    boolean wasChildDeleted = oldNumberOfSlaves > newNumberOfSlaves;

    LeaderElectionCandidate.ChangeType changeType;
    if (wasChildCreated) {
      changeType = LeaderElectionCandidate.ChangeType.CREATED;
    } else if (wasChildDeleted) {
      changeType = LeaderElectionCandidate.ChangeType.DELETED;
    } else {
      changeType = null;
    }
    return changeType;
  }

  /**
   * @return The ID of the candidate that was created (assuming the change between the two list was an addition)
   */
  public static String findIdOfCreatedCandidate(List<String> oldIDs, List<String> newIDs) {
    return newIDs.get(newIDs.size() - 1);
  }

  /**
   * @return The ID of the candidate that was removed (assuming the change between the two list was a deletion)
   */
  public static String findIdOfDeletedCandidate(List<String> oldIDs, List<String> newIDs) {
    String idOfDeletedCandidate = null;
    for (int i = 0; i < oldIDs.size(); i++) {
      String idFromOldList = oldIDs.get(i);
      String idFromNewList = i < newIDs.size() ? newIDs.get(i) : null;

      if (!Objects.equals(idFromOldList, idFromNewList)) {
        idOfDeletedCandidate = idFromOldList;
        break;
      }
    }
    return idOfDeletedCandidate;
  }
}
