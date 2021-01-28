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

package org.apache.solr.ivrixdb.search.utilities;

import java.util.*;

import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.ivrixdb.search.stream.*;
import org.apache.solr.ivrixdb.search.stream.job.*;
import org.apache.solr.ivrixdb.search.stream.nonstream.NonStreamPreviewStream;

/**
 * This class holds the definitions and properties (name,
 * class, is-transforming, etc) of all of IVrixDB's extensions
 * to Solr's Streaming API.
 *
 * @author Ivri Faitelson
 */
public class StreamDefinitionsHolder {
  public static final Map<String, StreamingFunction> streamingFunctions = new HashMap<>();
  public static final String PUSH_TO_JOB_FUNCTION_NAME = "pushToJob";
  public static final String NON_STREAM_PREVIEW_FUNCTION_NAME = "nonStreamPreview";
  public static final String UPDATE_HIT_COUNT_FUNCTION_NAME = "updateHitCount";
  public static final String SEARCH_FUNCTION_NAME = "search";

  static {
    putStreamingFunction(SEARCH_FUNCTION_NAME, IVrixSearchStream.class, false, false);
    putStreamingFunction(PUSH_TO_JOB_FUNCTION_NAME, PushTuplesToJobStream.class, false, false);
    putStreamingFunction(NON_STREAM_PREVIEW_FUNCTION_NAME, NonStreamPreviewStream.class, false, true);
    putStreamingFunction(UPDATE_HIT_COUNT_FUNCTION_NAME, UpdateHitCountStream.class, false, false);
    putStreamingFunction("rollup", IVrixRollupStream.class, true, true);
    putStreamingFunction("top", IVrixSortStream.class, false, true);
  }

  private static void putStreamingFunction(String name, Class<? extends Expressible> functionClass,
                                           boolean isTransforming, boolean isNonStreaming) {
    streamingFunctions.put(name, new StreamingFunction(name, functionClass, isTransforming, isNonStreaming));
  }


  /**
   * This object holds the definitions and properties of a
   * streaming function in the IVrix extension library.
   */
  public static class StreamingFunction {
    private final String name;
    private final Class<? extends Expressible> functionClass;
    private final boolean isTransforming;
    private final boolean isNonStreaming;

    /**
     * @param name The name of the function.
     * @param functionClass The class of the function.
     * @param isTransforming Is the function transforming (as in, after operation, will the tuple no longer be an event?)
     * @param isNonStreaming Is the function non-streaming (as in, will this function halt the pipeline?)
     */
    public StreamingFunction(String name, Class<? extends Expressible> functionClass,
                             boolean isTransforming, boolean isNonStreaming) {
      assert !isTransforming || isNonStreaming;
      this.name = name;
      this.functionClass = functionClass;
      this.isTransforming = isTransforming;
      this.isNonStreaming = isNonStreaming;
    }

    /**
     * @return Is the function non streaming.
     */
    public boolean isNonStreaming() {
      return isNonStreaming;
    }

    /**
     * @return Is the function transforming.
     */
    public boolean isTransforming() {
      return isTransforming;
    }

    /**
     * @return The class of the function.
     */
    public Class<? extends Expressible> getFunctionClass() {
      return functionClass;
    }

    /**
     * @return The name of the function.
     */
    public String getName() {
      return name;
    }
  }
}
