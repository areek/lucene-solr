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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;

import static org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDoc;

/**
 * {@link org.apache.lucene.search.Collector} that collects completion and
 * score, along with document id
 * <p>
 * Non scoring collector that collect completions in order of their
 * pre-computed scores.
 * <p>
 * NOTE: One document can be collected multiple times if a document
 * is matched for multiple unique completions for a given query
 * <p>
 * Subclasses should only override
 * {@link TopSuggestDocsCollector#collect(int, CharSequence, CharSequence, float)}.
 * <p>
 * NOTE: {@link #setScorer(org.apache.lucene.search.Scorer)} and
 * {@link #collect(int)} is not used
 *
 * @lucene.experimental
 */
public class TopSuggestDocsCollector extends TopSuggestionsCollector {

  private Map<Integer, SuggestScoreDoc> seenDocIds;

  /**
   * Sole constructor
   *
   * Collects at most <code>num</code> completions
   * with corresponding document and weight
   */
  public TopSuggestDocsCollector(int num) {
    super(num);
    seenDocIds = new HashMap<>(num);
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    super.doSetNextReader(context);
    seenDocIds.clear();
  }

  /**
   * Called for every matched completion,
   * similar to {@link org.apache.lucene.search.LeafCollector#collect(int)}
   * but for completions.
   * <p>
   * NOTE: collection at the leaf level is guaranteed to be in
   * descending order of score
   */
  public void collect(int doc, CharSequence key, CharSequence context, float score) throws IOException {
    //System.out.println("..........");
    //System.out.println("doc seen: " + (docBase + doc));
    SuggestScoreDoc originDoc = seenDocIds.get(doc);
    if (originDoc == null) {
      if (seenDocIds.size() == num) {
        throw new CollectionTerminatedException();
      }
      SuggestScoreDoc scoreDoc = new SuggestScoreDoc(docBase + doc, key, context, score);
      //System.out.println("doc added: " + (docBase + doc));
      if (scoreDoc == priorityQueue.insertWithOverflow(scoreDoc)) {
        throw new CollectionTerminatedException();
      }
      seenDocIds.put(doc, scoreDoc);
    } else {
      if (!originDoc.keys.contains(key)) {
        originDoc.keys.add(key);
      }
      if (!originDoc.contexts.contains(context)) {
        originDoc.contexts.add(context);
      }
      originDoc.score += score;
      //System.out.println("doc removed: " + (docBase + doc));
      if (priorityQueue.remove(originDoc)) {
        //System.out.println("doc updated: " + (docBase + doc));
        priorityQueue.add(originDoc);
      } else {
        if (originDoc == priorityQueue.insertWithOverflow(originDoc)) {
          throw new CollectionTerminatedException();
        }
      }
    }
    //System.out.println("..........");
  }
}
