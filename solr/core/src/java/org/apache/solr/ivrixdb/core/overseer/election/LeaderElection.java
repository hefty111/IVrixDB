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

package org.apache.solr.ivrixdb.core.overseer.election;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.ivrixdb.core.overseer.utilities.*;
import org.apache.solr.ivrixdb.core.overseer.election.ElectionWatcher.*;
import org.apache.solr.ivrixdb.core.overseer.election.LeaderElectionCandidate.OperatingMode;
import org.slf4j.*;

/**
 * This object is responsible for the leader election of leader election candidates.
 * It is a "first-come, first-serve" based election. The election occurs through
 * the Zookeeper. Each leader election candidate has needs one LeaderElection object.
 * (So in the case of the IVrix Overseer, each IVrix Node has one LeaderElection object.)
 *
 * @author Ivri Faitelson
 */
public class LeaderElection {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Object reconnectToZookeeperNotifier = new Object();
  private static final Object reelectionNotifier = new Object();
  private final SolrZkClient zkClient;
  private String electedLeaderID;

  /**
   * @return A new Leader Election object that is connected to zookeeper
   *          and is watching the currently elected leader (if there is any)
   */
  public static LeaderElection connect(ZkController zkController) {
    zkController.addOnReconnectListener(LeaderElection::notifyReconnectWaiters);
    return new LeaderElection(zkController);
  }

  private LeaderElection(ZkController zkController) {
    this.zkClient = zkController.getZkClient();
    ensureElectionPathExists();
    updateStoredLeaderIdAndWatch();
  }

  private void ensureElectionPathExists() {
    log.info("Ensuring that election path exists...");
    try {
      ElectionZkUtil.ensureElectionPathExists(zkClient);
    } catch (IOException e) {
      log.warn("failed to ensure that election path exists", e);
      e.printStackTrace();
    }
  }


  /**
   * Removes a candidate from the election process
   */
  public void removeCandidate(LeaderElectionCandidate candidate) {
    log.info("removing leader election candidate from election queue...");
    try {
      ElectionZkUtil.removeCandidateNode(zkClient, candidate.getID());
      candidate.setID(null);

    } catch (IOException e) {
      log.warn("failed to remove candidate: " + candidate.getID(), e);
      e.printStackTrace();
    }
  }

  /**
   * Adds a candidate into the election process and runs it
   */
  public void addCandidate(LeaderElectionCandidate candidate) {
    log.info("adding leader election candidate into election queue...");
    try {
      String candidateID = ElectionZkUtil.createCandidateNode(zkClient);
      candidate.setID(candidateID);
      runCandidate(candidate);
      new SelfWatcher(candidate, this);

    } catch (IOException e) {
      log.warn("failed to add candidate: " + candidate.getID(), e);
      e.printStackTrace();
    }
  }

  /**
   *  If the candidate is elected leader, then run it as a LEADER.
   *  Otherwise, run it as a SLAVE.
   */
  protected void runCandidate(LeaderElectionCandidate candidate) {
    try {
      List<String> allCandidateIDs = ElectionZkUtil.getAllCandidateIDs(zkClient);
      String leaderID = CandidateIdUtil.getElectedLeaderID(allCandidateIDs);

      if (candidate.getID().equals(leaderID)) {
        log.info("Running candidate as leader...");
        candidate.operateAs(OperatingMode.LEADER);
        new SlavesWatcher(candidate, this);

      } else {
        log.info("Running candidate as slave...");
        candidate.operateAs(OperatingMode.SLAVE);
        new NextCandidateWatcher(candidate, allCandidateIDs, this);
      }

    } catch (Exception e) {
      log.warn("failed to run candidate. Removing it from election and shutting it down", e);
      e.printStackTrace();
      candidate.shutdown();
      removeCandidate(candidate);
    }
  }

  /**
   * Updates the cached leader ID by grabbing the currently elected leader from Zookeeper
   * and re-watches that leader (if it exists) to be alerted the next time it changes
   */
  protected void updateStoredLeaderIdAndWatch() {
    log.info("Watching currently elected leader (if it exists)...");
    try {
      List<String> allCandidateIDs = ElectionZkUtil.getAllCandidateIDs(zkClient);
      electedLeaderID = CandidateIdUtil.getElectedLeaderID(allCandidateIDs);

      if (electedLeaderID != null) {
        new ElectionLeaderWatcher(allCandidateIDs, this);
      }

    } catch (IOException e) {
      log.warn("failed to watch elected leader", e);
      e.printStackTrace();
    }
  }

  /**
   * Notifies all re-election waiters that a new leader was elected
   */
  protected void notifyReelectionWaiters() {
    synchronized (reelectionNotifier) {
      reelectionNotifier.notifyAll();
    }
  }

  /**
   * Waits for a re-election to happen, or times out after a certain interval
   */
  public void waitForReelection() {
    synchronized (reelectionNotifier) {
      try {
        reelectionNotifier.wait(30 * 1000);
      } catch (InterruptedException e) {
        log.warn("failed to wait for reelection", e);
        e.printStackTrace();
      }
    }
  }

  /**
   * Notifies all Zookeeper-reconnection waiters that the connection to Zookeeper has been re-established
   */
  protected static void notifyReconnectWaiters() {
    synchronized (reconnectToZookeeperNotifier) {
      reconnectToZookeeperNotifier.notifyAll();
    }
  }

  /**
   * Waits for the Zookeeper connection to be re-established
   */
  public void waitForZookeeperReconnect() {
    synchronized (reconnectToZookeeperNotifier) {
      try {
        reconnectToZookeeperNotifier.wait();
      } catch (InterruptedException e) {
        log.warn("failed to wait for zookeeper reconnect", e);
        e.printStackTrace();
      }
    }
  }

  /**
   * @return The IVrix Node's base URL of the currently elected leader
   */
  public String getLeaderBaseURL() {
    String baseURL = null;
    if (electedLeaderID != null) {
      baseURL = CandidateIdUtil.extractBaseUrlFromID(electedLeaderID);
    }
    return baseURL;
  }

  /**
   * @return The Solr zookeeper client that is being used for election
   */
  public SolrZkClient getZkClient() {
    return zkClient;
  }
}
