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

package org.apache.solr.ivrixdb.search.stream.export.adapter;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.ivrixdb.utilities.SolrObjectMocker;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * This object is a modified {@link org.apache.solr.handler.export.ExportWriter}
 * that instead of writing all at once to an output stream, it extracts one
 * Tuple at a time.
 *
 * @author Ivri Faitelson
 */
public class ExportAdapter {
  private static final int DOCUMENT_BATCH_SIZE = 30000;
  private int currentIndexInBatch = -1;
  private List<LeafReaderContext> leaves;
  private FixedBitSet[] sets;
  private FieldWriter[] fieldWriters;
  private SortDoc sortDoc;
  private SortQueue queue;
  private SortDoc[] outDocs;
  private int totalHits = 0;
  private int exported = 0;

  /**
   * @param selectedFields The fields to extract. Must be DocValues.
   * @param sort The sort order by which to extract. Must be on a DocValue field.
   * @param indexSearcher The SolrIndexSearcher for extracting fields given a doc ID.
   * @param exportBitSets The bit sets returned by the search.
   * @param totalHits The total number of documents found by the search.
   */
  public ExportAdapter(String[] selectedFields, Sort sort, SolrIndexSearcher indexSearcher,
                       FixedBitSet[] exportBitSets, int totalHits) throws IOException {
    int queueSize = Math.min(DOCUMENT_BATCH_SIZE, totalHits);
    this.leaves = indexSearcher.getTopReaderContext().leaves();
    this.fieldWriters = FieldWriter.getFieldWriters(selectedFields, indexSearcher);
    this.sortDoc = SortDoc.getSortDoc(indexSearcher, sort.getSort());
    this.queue = new SortQueue(queueSize, this.sortDoc);
    this.outDocs = new SortDoc[queueSize];
    this.totalHits = totalHits;
    this.sets = exportBitSets;
  }

  /**
   * Iterates over batch one at a time;
   * Calls for a new batch when necessary;
   * returns the next Tuple;
   * and clears it from batch.
   */
  public Tuple nextTuple() throws IOException {
    Tuple tuple;

    if (exported < totalHits) {
      if (currentIndexInBatch < 0) {
        currentIndexInBatch = nextBatch();
      }

      SortDoc s = outDocs[currentIndexInBatch];
      tuple = createTuple(s);

      outDocs[currentIndexInBatch] = null;
      currentIndexInBatch--;
      exported++;

    } else {
      tuple = SolrObjectMocker.mockEOFTuple();
    }

    return tuple;
  }

  /**
   * Sorts the values in a queue, then transfers them to an array.
   */
  private int nextBatch() throws IOException {
    identifyLowestSortingUnexportedDocs(leaves, sortDoc, queue);
    return transferBatchToArrayForOutput(queue, outDocs);
  }

  /**
   * writers fields to tuple one at a time.
   * Clears the tuple metadata from memory.
   */
  private Tuple createTuple(SortDoc doc) throws IOException {
    int ord = doc.ord;
    FixedBitSet set = sets[ord];
    set.clear(doc.docId);
    LeafReaderContext context = leaves.get(ord);
    Map map = new HashMap();

    int fieldIndex = 0;
    for (FieldWriter fieldWriter : fieldWriters) {
      if (fieldWriter.write(doc, context.reader(), map, fieldIndex)) {
        ++fieldIndex;
      }
    }

    doc.reset();
    return new Tuple(map);
  }

  private void identifyLowestSortingUnexportedDocs(List<LeafReaderContext> leaves, SortDoc sortDoc, SortQueue queue) throws IOException {
    queue.reset();
    SortDoc top = queue.top();
    for (int i = 0; i < leaves.size(); i++) {
      sortDoc.setNextReader(leaves.get(i));
      DocIdSetIterator it = new BitSetIterator(sets[i], 0); // cost is not useful here
      int docId;
      while ((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        sortDoc.setValues(docId);
        if (top.lessThan(sortDoc)) {
          top.setValues(sortDoc);
          top = queue.updateTop();
        }
      }
    }
  }

  private int transferBatchToArrayForOutput(SortQueue queue, SortDoc[] destinationArr) {
    int outDocsIndex = -1;
    for (int i = 0; i < queue.maxSize; i++) {
      SortDoc s = queue.pop();
      if (s.docId > -1) {
        destinationArr[++outDocsIndex] = s;
      }
    }
    return outDocsIndex;
  }
}
