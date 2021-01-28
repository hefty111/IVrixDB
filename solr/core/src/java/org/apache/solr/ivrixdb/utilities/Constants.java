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

package org.apache.solr.ivrixdb.utilities;

/**
 * This class is a collection of constants of Solr and IVrixDB that are used in IVrixDB.
 * Also includes configuration values, which will eventually be loaded from a file.
 *
 * @author Ivri Faitelson
 */
public class Constants {
  public static class IVrix {
    /**
     * The name of the controller collection. This collection is the one that
     * receives calls to index, query, and manage IVrix indexes.
     */
    public static final String CONTROLLER_COLLECTION_NAME = "ivrixdb";
    /**
     * The name of the ConfigSet for IVrix buckets
     */
    public static final String IVRIX_BUCKET_CONFIG_SET = "ivrix_configset";
    /**
     * The name of the field in the bucket schema that holds the raw event
     */
    public static final String RAW_EVENT_FIELD = "_raw";
    /**
     * The name of the field in the bucket schema that holds the time of the event
     */
    public static final String RAW_TIME_FIELD = "_time";
    /**
     * The key in StreamContext.entities that provides the SearchJob
     */
    public static final String SEARCH_JOB_IN_CONTEXT = "search-job";
    /**
     * The key in StreamContext.entities that provides the user-given search range
     */
    public static final String SEARCH_RANGE_IN_CONTEXT = "search-range";



    /**
     * The default size of an events batch during indexing
     */
    public static final Integer DEFAULT_EVENT_BATCH_SIZE = 30000;
    /**
     * The default max event cutoff for a bucket during indexing
     */
    public static final Integer DEFAULT_MAX_EVENT_COUNT_IN_BUCKET = 401500;
    /**
     * The default number of replicas a bucket is created with
     */
    public static final Integer DEFAULT_BUCKET_REPLICATION_FACTOR = 2;
    /**
     * The default number of buckets that can be searched at once for each Search Job
     */
    public static final Integer DEFAULT_PARALLELIZATION_FACTOR = 2;



    public static class Limitations {
      public static class State {
        /**
         * The maximum number of (hot and warm) buckets allowed to be attached per indexer
         */
        public static final Integer MAXIMUM_ATTACHED_HOT_PLUS_WARM_BUCKETS_PER_NODE = 10;
        /**
         * The maximum number of cold buckets allowed to be attached per indexer
         */
        public static final Integer MAXIMUM_ATTACHED_COLD_BUCKETS_PER_NODE = 3;
      }
      public static class Timeline {
        /**
         * The maximum number of stored events when digestion occurs through a Non-Stream push.
         * Dynamic Timeline will continue digestion if limit reached, but will not store anymore.
         */
        public static final int MAXIMUM_STORED_EVENTS_WITH_NON_STREAM_PUSH = 1000;
        /**
         * The maximum number of events stored in a time-bucket.
         * Time-bucket will not save any more events after limit has been reached.
         */
        public static final int MAXIMUM_STORED_EVENTS_PER_BUCKET = 1000;
        /**
         * The maximum number of time buckets allowed to exist in the timeline.
         * Dynamic Timeline will merge buckets if it surpassed the limit.
         */
        public static final int MAXIMUM_TIME_BUCKETS = 300;
      }
      public static class TopValues {
        /**
         * The maximum number of values to keep track of.
         * Will ignore other values if limit has been reached.
         */
        public static final int MAXIMUM_DISTINCT_VALUES = 50000;
      }
    }
  }

  public static class Solr {
    /**
     * The string in http calls designating the start of the solr API call
     */
    public static final String API_STR = "solr";
    /**
     * The EOF tag added to tuples to designate EOF
     */
    public static final String EOF_STR = "EOF";
    /**
     * The key in StreamContext.entities that provides the SolrCore
     */
    public static final String CORE_IN_CONTEXT = "solr-core";
  }
}
