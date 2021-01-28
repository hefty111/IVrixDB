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

import java.time.OffsetDateTime;
import java.util.*;

import org.apache.solr.ivrixdb.utilities.Constants;

/**
 * This object defines a region/range/slice of time.
 * It can either be ranged or unbounded. It can be updated,
 * serialized/de-serialized, turned into a single number representation,
 * turned into a Solr Q search parameter, detecting overlap with another
 * TimeBounds object, and more. It is used to define a timestamp range
 * of an IVrix Bucket, and a time range to search on.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO -- implement explicit state of not-yet-defined range of a bounded TimeBounds object,
 *         so that its functions (and others outside that use them) can have 100% deterministic behaviors
 */
public class TimeBounds {
  private static final String IS_BOUNDED_KEY = "isBounded";
  private static final String LOWER_BOUND_KEY = "lowerBound";
  private static final String UPPER_BOUND_KEY = "upperBound";

  private boolean isBounded;
  private OffsetDateTime lowerBound;
  private OffsetDateTime upperBound;

  private TimeBounds() {
    this.isBounded = false;
  }

  /**
   * Creates either an unbounded time bounds, or an ranged (but not yet defined) time bounds
   * @param isBounded Whether the time bounds is of type bounded
   */
  public TimeBounds(boolean isBounded) {
    this.isBounded = isBounded;
  }

  /**
   * Creates a ranged time bounds
   * @param lowerBound The lower bound of the time bounds.
   * @param upperBound The upper bound of the time bounds
   */
  public TimeBounds(OffsetDateTime lowerBound, OffsetDateTime upperBound) {
    this.isBounded = true;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  /**
   * @return A TimeBounds object from a Map with the properties of the time bounds represented as Strings
   */
  public static TimeBounds fromMap(Map<String, Object> timeBoundsMap) {
    if (timeBoundsMap == null) {
      return null;
    }

    TimeBounds timeBounds = new TimeBounds();
    timeBounds.isBounded = (boolean)timeBoundsMap.get(IS_BOUNDED_KEY);
    timeBounds.lowerBound = extractDateTime((String)timeBoundsMap.get(LOWER_BOUND_KEY));
    timeBounds.upperBound = extractDateTime((String)timeBoundsMap.get(UPPER_BOUND_KEY));
    return timeBounds;
  }

  /**
   * @return A Map with the properties of the time bounds object represented as Strings
   */
  public Map<String, Object> toMap() {
    Map<String,Object> map = new HashMap<>();
    map.put(IS_BOUNDED_KEY, isBounded);
    map.put(LOWER_BOUND_KEY, lowerBound == null ? null : lowerBound.toString());
    map.put(UPPER_BOUND_KEY, upperBound == null ? null : upperBound.toString());
    return map;
  }


  /**
   * Given a date time object, updates the lower and upper bounds if appropriate
   */
  public void updateBounds(OffsetDateTime dateTime) {
    if (lowerBound == null || dateTime.isBefore(lowerBound)) {
      lowerBound = dateTime;
    }
    if (upperBound == null || dateTime.isAfter(lowerBound)) {
      upperBound = dateTime;
    }
  }

  /**
   * @return Whether this time bounds object overlaps with another,
   *          which includes "being inside" another's bounds.
   *          Inclusive of touching edges.
   */
  public boolean overlapsWith(TimeBounds that) {
    if (!this.isBounded || !that.isBounded) {
      return true;
    } else {
      boolean isThisLowerOverlapThatUpper = this.lowerBound.compareTo(that.upperBound) <= 0 && this.upperBound.compareTo(that.upperBound) >= 0;
      boolean isThisUpperOverlapThatLower = this.lowerBound.compareTo(that.lowerBound) <= 0 && this.upperBound.compareTo(that.lowerBound) >= 0;
      boolean isThatInsideThis = this.lowerBound.compareTo(that.lowerBound) <= 0 && this.upperBound.compareTo(that.upperBound) >= 0;
      boolean isThisInsideThat = that.lowerBound.compareTo(this.lowerBound) <= 0 && that.upperBound.compareTo(this.upperBound) >= 0;
      return isThisLowerOverlapThatUpper || isThisUpperOverlapThatLower || isThatInsideThis || isThisInsideThat;
    }
  }

  /**
   * @return A single number presentation of the time bounds
   *          Currently, that is set to number presentation of the upper bounds
   *          (and zero if it does not exist)
   */
  public long getLongRepresentation() {
    if (upperBound != null) {
      return upperBound.toEpochSecond();
    } else {
      return 0;
    }
  }

  /**
   * @return Whether the time bounds is of type bounded
   */
  public boolean isBounded() {
    return isBounded;
  }

  /**
   * @return The time bounds converted to a Solr Q search parameter
   */
  public String toSolrQuerySyntax() {
    if (isBounded) {
      return Constants.IVrix.RAW_TIME_FIELD + ":[" + lowerBound.toEpochSecond() + " TO " + upperBound.toEpochSecond() + "]";
    } else {
      return Constants.IVrix.RAW_TIME_FIELD + ":[* TO *]";
    }
  }

  private static OffsetDateTime extractDateTime(String dateTimeString) {
    OffsetDateTime dateTime;
    if (dateTimeString == null) {
      dateTime = null;
    } else {
      try {
        dateTime = OffsetDateTime.parse(dateTimeString);
      } catch (Exception e) {
        dateTime = null;
      }
    }
    return dateTime;
  }
}
