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

package org.apache.solr.ivrixdb.search.job.storage.timeline;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Defines the Scope level of the timeline.
 * Used to generate lower and upper bounds
 * of buckets.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: refactor -- for finding the next lower bound, simply decrement the current lower bound by one milisecond
 */
// ORDER MATTERS! The ordinal values are being utilized
public enum Scope {
  MILLISECOND_1(1),
  MILLISECOND_10(10),
  MILLISECOND_100(100),
  SECOND(1),
  MINUTE(1),
  HOUR( 1),
  DAY(1),
  MONTH(1),
  YEAR(1);

  private static final int NANOSECONDS_IN_MILLISECOND = 1000000;
  private final int unitLength;

  Scope(int unitLength) {
    this.unitLength = unitLength;
  }

  /**
   * @return creates an lower bound given the scope and the locale.
   *         flattens the locale to get the value.
   */
  public OffsetDateTime createLowerBound(OffsetDateTime locale) {
    return flattenLocale(locale, this);
  }

  /**
   * @return creates an upper bound given the scope and the locale.
   *         flattens the locale and then increments it to get to the value.
   */
  public OffsetDateTime createUpperBound(OffsetDateTime locale) {
    return flattenLocale(incrementLocale(locale), this);
  }

  /**
   * @return returns all the lower bounds in a given range in the given a scope.
   */
  public List<OffsetDateTime> getLowerBoundsBetween(OffsetDateTime earliest, OffsetDateTime latest) {
    List<OffsetDateTime> lowerBounds = new LinkedList<>();
    OffsetDateTime earliestLowerBound = createLowerBound(earliest);
    OffsetDateTime nextLowerBound = createLowerBound(latest);

    if (nextLowerBound.isEqual(latest) && earliestLowerBound.isBefore(nextLowerBound)) {
      nextLowerBound = nextLowerBound(nextLowerBound);
    }

    while (earliestLowerBound.compareTo(nextLowerBound) <= 0) {
      lowerBounds.add(nextLowerBound);
      nextLowerBound = nextLowerBound(nextLowerBound);
    }

    return lowerBounds;
  }

  /**
   * @return a new Scope that is a one level higher than the current one.
   */
  public Scope zoomOut() {
    return Scope.values()[this.ordinal() + 1];
  }

  private OffsetDateTime flattenLocale(OffsetDateTime locale, Scope scope) {
    OffsetDateTime flattenedLocale;
    switch (scope) {
      case MILLISECOND_10:
      case MILLISECOND_100:
        flattenedLocale = locale.withNano((locale.getNano() / unitLength*NANOSECONDS_IN_MILLISECOND) * unitLength);
        break;
      case SECOND:
        flattenedLocale = locale.withNano(0);
        break;
      case MINUTE:
        flattenedLocale = flattenLocale(locale, Scope.SECOND).withSecond(0);
        break;
      case HOUR:
        flattenedLocale = flattenLocale(locale, Scope.MINUTE).withMinute(0);
        break;
      case DAY:
        flattenedLocale = flattenLocale(locale, Scope.HOUR).withHour(0);
        break;
      case MONTH:
        flattenedLocale = flattenLocale(locale, Scope.DAY).withDayOfMonth(1);
        break;
      case YEAR:
        flattenedLocale = flattenLocale(locale, Scope.MONTH).withDayOfYear(1);
        break;
      default:
        flattenedLocale = OffsetDateTime.from(locale);
        break;
    }
    return flattenedLocale;
  }

  private OffsetDateTime incrementLocale(OffsetDateTime locale) {
    OffsetDateTime incrementedLocale;
    switch (this) {
      case SECOND:
        incrementedLocale = locale.plusSeconds(unitLength);
        break;
      case MINUTE:
        incrementedLocale = locale.plusMinutes(unitLength);
        break;
      case HOUR:
        incrementedLocale = locale.plusHours(unitLength);
        break;
      case DAY:
        incrementedLocale = locale.plusDays(unitLength);
        break;
      case MONTH:
        incrementedLocale = locale.plusMonths(unitLength);
        break;
      case YEAR:
        incrementedLocale = locale.plusYears(unitLength);
        break;
      default:
        incrementedLocale = OffsetDateTime.from(locale);
        break;
    }
    return incrementedLocale;
  }

  private OffsetDateTime nextLowerBound(OffsetDateTime locale) {
    OffsetDateTime jumpBounds;
    switch (this) {
      case MILLISECOND_1:
      case MILLISECOND_10:
      case MILLISECOND_100:
      case SECOND:
        jumpBounds = locale.minus(ChronoUnit.MILLIS.getDuration());
        break;
      case MINUTE:
        jumpBounds = locale.minus(ChronoUnit.SECONDS.getDuration());
        break;
      case HOUR:
        jumpBounds = locale.minus(ChronoUnit.MINUTES.getDuration());
        break;
      case DAY:
        jumpBounds = locale.minus(ChronoUnit.HOURS.getDuration());
        break;
      case MONTH:
        jumpBounds = locale.minus(ChronoUnit.DAYS.getDuration());
        break;
      case YEAR:
        jumpBounds = locale.minus(ChronoUnit.MONTHS.getDuration());
        break;
      default:
        jumpBounds = OffsetDateTime.from(locale);
        break;
    }

    return createLowerBound(jumpBounds);
  }
}