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

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.ivrixdb.core.overseer.utilities.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.*;
import org.slf4j.*;

import static org.apache.solr.ivrixdb.core.overseer.utilities.CandidateIdUtil.*;

/**
 * This class encapsulates commonalities of Zookeeper Watchers for leader elections.
 * Zookeeper Watchers are objects that get woken up as a new Thread whenever a certain
 * event has occurred. They can be placed on an entire path, or an a specific ZK Node,
 * and the event. By default, all election Watchers are dropped on Zookeeper disconnect.
 *
 * To learn more about the different types of watcher events that can occur, please
 * refer to {@link org.apache.zookeeper.Watcher.Event.EventType}
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: think about if "NodeDeleted" should be a scenario to include in SelfWatcher.
 *
 * TODO: change this dirty fix. The issue is that SelfWatcher is called upon twice, adding a duplicate node in ZK.
 *       may be related to the duplicate problem expressed in ElectionZkUtil.
 */
public abstract class ElectionWatcher implements Watcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void process(WatchedEvent event) {
    if (hasLostConnectionToZookeeper(event)) {
      return; // i.e., drop watcher
    }
    _process(event);
  }

  abstract void _process(WatchedEvent event);

  private static boolean hasLostConnectionToZookeeper(WatchedEvent event) {
    return event.getType() == EventType.None &&
          (
              event.getState() == KeeperState.Expired ||
              event.getState() == KeeperState.Disconnected ||
              event.getState() == KeeperState.Closed
          );
  }


  /**
   * This Watcher watches a candidate in the leader election path.
   * It shuts down the candidate at Zookeeper disconnection (which automatically removed it from election),
   * and re-adds the candidate to the election once the Zookeeper connection has been re-established.
   */
  public static class SelfWatcher extends ElectionWatcher {
    private final LeaderElectionCandidate self;
    private final LeaderElection leaderElection;
    /**
     * Creates the watcher given the candidate to watch and watches it
     */
    public SelfWatcher(LeaderElectionCandidate candidate, LeaderElection leaderElection) throws IOException {
      this.self = candidate;
      this.leaderElection = leaderElection;
      ElectionZkUtil.watchCandidate(leaderElection.getZkClient(), self.getID(), this);
    }

    @Override
    public void process(WatchedEvent event) {
      if (hasLostConnectionToZookeeper(event)) {
        log.info("IVrix node has either lost connection to ZK. Shutting down candidate, and waiting for reelection...");
        self.shutdown();
        self.setID(null);
        leaderElection.waitForZookeeperReconnect();
        reAddCandidate(self, leaderElection);
      }
    }

    private synchronized static void reAddCandidate(LeaderElectionCandidate self, LeaderElection leaderElection) {
      if (self.getID() == null) {
        leaderElection.addCandidate(self);
      }
    }

    @Override
    public void _process(WatchedEvent event) {}
  }


  /**
   * This Watcher watches all the non-leader candidates in the leader election path.
   * Once an event has occurred, it will figure out what changed, and alert that change
   * to the watching candidate. That watching candidate is usually the leader candidate.
   */
  public static class SlavesWatcher extends ElectionWatcher {
    private final LeaderElection leaderElection;
    private final SolrZkClient zkClient;
    private final LeaderElectionCandidate watchingCandidate;
    private final List<String> oldIDs;
    /**
     * Creates the watcher given the watching candidate and watches the whole leader election path
     */
    public SlavesWatcher(LeaderElectionCandidate watchingCandidate, LeaderElection leaderElection) throws IOException {
      this.zkClient = leaderElection.getZkClient();
      this.leaderElection = leaderElection;
      this.watchingCandidate = watchingCandidate;
      this.oldIDs = ElectionZkUtil.getAllCandidateIDsAndWatch(zkClient, this);
    }

    @Override
    public void _process(WatchedEvent event) {
      try {
        List<String> newIDs = ElectionZkUtil.getAllCandidateIDsAndWatch(
            zkClient, new SlavesWatcher(watchingCandidate, leaderElection)
        );

        LeaderElectionCandidate.ChangeType changeType = findChangeTypeInCandidates(oldIDs, newIDs);
        String idOfChangedNode = null;
        if (changeType == LeaderElectionCandidate.ChangeType.CREATED) {
          idOfChangedNode = findIdOfCreatedCandidate(oldIDs, newIDs);

        } else if (changeType == LeaderElectionCandidate.ChangeType.DELETED) {
          idOfChangedNode = findIdOfDeletedCandidate(oldIDs, newIDs);
        }

        boolean isSlaveNode = idOfChangedNode != null && !idOfChangedNode.equals(watchingCandidate.getID());
        if (isSlaveNode) {
          log.info("A change in the leader election queue has occurred. A node has been: " + changeType);
          watchingCandidate.handleSlaveChange(changeType, idOfChangedNode);
        }

      } catch (IOException e) {
        log.warn("slave watcher failed to process (possibly, it failed to re-watch)", e);
        e.printStackTrace();
      }
    }
  }


  /**
   * This watcher is responsible for watching a candidate that is in front of another candidate
   * in the leader election path. Once a change has happened to it, the watching candidate
   * will re-run itself with the hope of being the leader. If it is not the leader,
   * then it will run itself as a slave again, watching the new candidate that is in front of it.
   */
  public static class NextCandidateWatcher extends ElectionWatcher {
    private final LeaderElection leaderElection;
    private final LeaderElectionCandidate watchingCandidate;
    /**
     * Creates the watcher given the watching candidate and all the other candidates,
     * finds the candidate in-front of the watching candidate, and watches it
     */
    public NextCandidateWatcher(LeaderElectionCandidate watchingCandidate,
                                List<String> allCandidateIDs,
                                LeaderElection leaderElection) throws IOException {
      this.watchingCandidate = watchingCandidate;
      this.leaderElection = leaderElection;
      String nextCandidateID = CandidateIdUtil.findNextCandidateID(watchingCandidate, allCandidateIDs);
      ElectionZkUtil.watchCandidate(leaderElection.getZkClient(), nextCandidateID, this);
    }
    @Override
    public void _process(WatchedEvent event) {
      log.info("A change occurred in the election status of the candidate ahead of candidate: " + watchingCandidate.getID());
      leaderElection.runCandidate(watchingCandidate);
    }
  }


  /**
   * This watcher is responsible for watching the elected leader in order to update the cached leader ID
   * in each IVrix Node (LeaderElection object). IT IS NOT used by any leader election candidate.
   */
  public static class ElectionLeaderWatcher extends ElectionWatcher {
    private final LeaderElection leaderElection;
    /**
     * Creates the watcher given all the candidates, finds the leader, and watches it
     */
    public ElectionLeaderWatcher(List<String> allCandidateIDs, LeaderElection leaderElection) throws IOException {
      this.leaderElection = leaderElection;
      String electedLeaderID = getElectedLeaderID(allCandidateIDs);
      ElectionZkUtil.watchCandidate(leaderElection.getZkClient(), electedLeaderID, this);
    }
    @Override
    public void _process(WatchedEvent event) {
      log.info("Possibly a new leader has been elected...");
      leaderElection.updateStoredLeaderIdAndWatch();
      leaderElection.notifyReelectionWaiters();
    }
  }
}
