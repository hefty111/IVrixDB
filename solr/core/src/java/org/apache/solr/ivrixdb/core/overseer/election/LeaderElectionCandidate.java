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

/**
 * This class encapsulates the concept of a candidate for a leader election.
 * These candidate can either be a leader or a slave (and even not-determined.)
 * They can be booted up, shut down, and be alerted to overall changes in the
 * leader election.
 *
 * @author Ivri Faitelson
 */
public abstract class LeaderElectionCandidate {
  /**
   * Defines what role the candidate was assigned during leader election.
   * Can be leader, slave, or non-determined.
   */
  public enum OperatingMode {
    LEADER, SLAVE, NOT_RUNNING
  }
  /**
   * Defines the type of change in the overall leader election.
   * Can be that a candidate was removed from election, or added
   * to the election.
   */
  public enum ChangeType {
    CREATED, DELETED
  }

  private OperatingMode operatingMode;
  private String id;

  /**
   * Creates a non-determined leader election candidate
   */
  public LeaderElectionCandidate() {
    operatingMode = OperatingMode.NOT_RUNNING;
  }

  /**
   * Changes the role of the candidate. Sub-classes are expected to override this function
   * and implement a "boot-up" procedure, alongside with this super function.
   */
  protected void operateAs(OperatingMode operatingMode) throws Exception {
    this.operatingMode = operatingMode;
  }

  /**
   * Handles a change in the list of SLAVE candidates
   * @param changeType The type of change in the list (either "CREATED" or "DELETED")
   * @param slaveID The candidate ID that introduced the change in the list
   */
  protected abstract void handleSlaveChange(ChangeType changeType, String slaveID);

  /**
   * Shuts down the candidate, and assigns the role to be non-determined. Sub-classes
   * are expected to override this function and implement a "shut-down" procedure,
   * alongside with this super function.
   */
  protected void shutdown() {
    this.operatingMode = OperatingMode.NOT_RUNNING;
  }

  /**
   * Changes the ID of the candidate
   */
  protected void setID(String id) {
    this.id = id;
  }

  /**
   * @return The ID of the candidate
   */
  public String getID() {
    return id;
  }

  /**
   * @return The assigned role of this candidate
   */
  public OperatingMode getOperatingMode() {
    return operatingMode;
  }
}
