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

package org.apache.solr.ivrixdb.search.stream.nonstream.processor;

import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.HashKey;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.ivrixdb.search.stream.nonstream.NonStreamProcessor;
import org.apache.solr.ivrixdb.search.stream.stats.NonAdditiveMetric;

/**
 * This processor creates and holds buckets in-memory,
 * and then returns them in a new sort order. Note that
 * there is a limit to the number of buckets.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: make the processor obey limits
 * TODO: make the iterator return in a sort order
 * TODO: make sort order work with multiple buckets
 * TODO: make no/single bucket (returns single row with all metrics) work
 */
public class Roller extends NonStreamProcessor {
  private final Map<HashKey, Metric[]> metricsMap = new HashMap<>();
  private final Bucket[] bucketsConfig;
  private final Metric[] metricsConfig;

  /**
   * @param bucketsConfig the fields and values to rollup to create buckets.
   * @param metricsConfig the metrics to compute.
   * @param canBePreviewed if the processor can be previewed or not.
   */
  public Roller(Bucket[] bucketsConfig, Metric[] metricsConfig, boolean canBePreviewed) {
    super(canBePreviewed);
    this.bucketsConfig = bucketsConfig;
    this.metricsConfig = metricsConfig;
  }

  /**
   * {@inheritDoc}
   */
  public NonStreamProcessor createNewWithSameConfig() {
    return new Roller(bucketsConfig, metricsConfig, lockable);
  }

  /**
   * returns an iterator over generated buckets.
   */
  public Iterator<Tuple> generatedTuplesIterator() {
    return new Iterator<>() {
      private final Iterator<HashKey> _hashKeysIterator = metricsMap.keySet().iterator();
      @Override
      public boolean hasNext() {
        return _hashKeysIterator.hasNext();
      }
      @Override
      public Tuple next() {
        HashKey nextHashKey = _hashKeysIterator.next();
        return createTuple(nextHashKey);
      }
      @Override
      public void remove() {
        _hashKeysIterator.remove();
      }
    };
  }

  protected void _process(Tuple tuple) {
    HashKey hashKey = createHashKey(tuple);
    if (!metricsMap.containsKey(hashKey)) {
      createMetrics(hashKey, tuple);
    } else {
      updateMetrics(hashKey, tuple);
    }
  }

  private HashKey createHashKey(Tuple tuple) {
    Object[] bucketValues = new Object[bucketsConfig.length];
    for (int i = 0; i < bucketsConfig.length; i++) {
      bucketValues[i] = bucketsConfig[i].getBucketValue(tuple);
    }
    return new HashKey(bucketValues);
  }

  private void updateMetrics(HashKey hashKey, Tuple tuple) {
    Metric[] bucketMetrics = metricsMap.get(hashKey);
    if (bucketMetrics != null) {
      for (Metric bucketMetric : bucketMetrics) {
        bucketMetric.update(tuple);
      }
    }
  }

  private void createMetrics(HashKey hashKey, Tuple tuple) {
    if (metricsConfig != null) {
      Metric[] newMetrics = new Metric[metricsConfig.length];
      for (int i = 0; i < metricsConfig.length; i++) {
        Metric bucketMetric = metricsConfig[i].newInstance();
        bucketMetric.update(tuple);
        newMetrics[i] = bucketMetric;
      }
      metricsMap.put(hashKey, newMetrics);
    } else {
      metricsMap.put(hashKey, null);
    }
  }

  private Tuple createTuple(HashKey hashKey) {
    Tuple realBucket = null;

    Metric[] bucketMetrics = metricsMap.get(hashKey);
    if(bucketMetrics != null) {

      Map<String, Object> map = new HashMap<>();
      for(Metric metric : bucketMetrics) {
        map.put(metric.getIdentifier(), metric.getValue());

        if (metric instanceof NonAdditiveMetric) {
          NonAdditiveMetric nonAdditiveMetric = (NonAdditiveMetric) metric;
          map.put(nonAdditiveMetric.getValuesIdentifier(), nonAdditiveMetric.getNonAdditiveValues());
        }
      }

      for(int i = 0; i < bucketsConfig.length; i++) {
        map.put(bucketsConfig[i].toString(), hashKey.getParts()[i]);
      }
      realBucket = new Tuple(map);
    }

    return realBucket;
  }
}