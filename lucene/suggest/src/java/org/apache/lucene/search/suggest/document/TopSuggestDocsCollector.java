package org.apache.lucene.search.suggest.document;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDocPriorityQueue;

import static org.apache.lucene.search.suggest.document.TopSuggestDocs.EMPTY;
import static org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDoc;

/**
 * {@link org.apache.lucene.search.Collector} for
 * {@link NRTSuggester}
 * <p>
 * Non scoring collector that collect hits in order of their
 * pre-defined weight.
 * <p>
 * NOTE: One hit can be collected multiple times if a document
 * is matched for multiple completions for a given query
 * <p>
 * Subclasses should only override {@link #collect(int, CharSequence, long)},
 * {@link #setScorer(org.apache.lucene.search.Scorer)} is not
 * used
 *
 * @lucene.experimental
 */
public class TopSuggestDocsCollector extends SimpleCollector {

  private final SuggestScoreDocPriorityQueue priorityQueue;

  /**
   * Creates a leaf collector to hold
   * at most <code>num</code> hits
   */
  public TopSuggestDocsCollector(int num) {
    if (num <= 0) {
      throw new IllegalArgumentException("'num' must be > 0");
    }
    this.priorityQueue = new SuggestScoreDocPriorityQueue(num);
  }

  /**
   * Called for every hit, similar to {@link org.apache.lucene.search.LeafCollector#collect(int)}
   */
  public void collect(int docID, CharSequence key, long score) throws IOException {
    SuggestScoreDoc current = new SuggestScoreDoc(docID, key, score);
    SuggestScoreDoc overflow = priorityQueue.insertWithOverflow(current);
    if (overflow == current) {
      throw new CollectionTerminatedException();
    }
  }

  /**
   * Returns the hits
   */
  public TopSuggestDocs get() throws IOException {
    SuggestScoreDoc[] suggestScoreDocs = priorityQueue.getResults();
    if (suggestScoreDocs.length > 0) {
      return new TopSuggestDocs(suggestScoreDocs.length, suggestScoreDocs, suggestScoreDocs[0].score);
    } else {
      return TopSuggestDocs.EMPTY;
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    // {@link #collect(int, CharSequence, long)} is used
    // instead
  }

  @Override
  public boolean needsScores() {
    return false;
  }
}
