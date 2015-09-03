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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.suggest.Lookup;

/**
 * {@link org.apache.lucene.search.TopDocs} wrapper with
 * an additional CharSequence key per {@link org.apache.lucene.search.ScoreDoc}
 *
 * @lucene.experimental
 */
public class TopSuggestDocs extends TopDocs {

  /**
   * Singleton for empty {@link TopSuggestDocs}
   */
  public final static TopSuggestDocs EMPTY = new TopSuggestDocs(0, new SuggestScoreDoc[0], 0);

  /**
   * {@link org.apache.lucene.search.ScoreDoc} with an
   * additional CharSequence key
   */
  public static class SuggestScoreDoc extends ScoreDoc implements Comparable<SuggestScoreDoc> {

    /**
     * Matched completion key
     */
    public final List<CharSequence> keys = new ArrayList<>(1);

    /**
     * Context for the completion
     */
    public final List<CharSequence> contexts = new ArrayList<>(1);

    /**
     * Creates a SuggestScoreDoc instance
     *
     * @param doc   document id (hit)
     * @param key   matched completion
     * @param score weight of the matched completion
     */
    public SuggestScoreDoc(int doc, CharSequence key, CharSequence context, float score) {
      super(doc, score);
      this.keys.add(key);
      this.contexts.add(context);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("keys: [");
      for (int i = 0; i < keys.size(); i++) {
        sb.append(keys.get(i));
        if (i == keys.size() - 1) {
          sb.append("] ");
        } else {
          sb.append(", ");
        }
      }
      sb.append("score: ");
      sb.append(score);
      if (contexts.size() > 0) {
        sb.append(" contexts: [");
        for (int i = 0; i < contexts.size(); i++) {
          sb.append(contexts.get(i));
          if (i == contexts.size() - 1) {
            sb.append("] ");
          } else {
            sb.append(", ");
          }
        }
      }
      return sb.toString();
    }

    @Override
    public int compareTo(SuggestScoreDoc other) {
      int cmp = Float.compare(other.score, score);
      if (cmp == 0) {
        // prefer smaller doc id, in case of a tie
        cmp = compare(keys, other.keys);
        if (cmp == 0) {
          cmp = compare(contexts, other.contexts);
        }
        if (cmp == 0) {
          cmp = Integer.compare(doc, other.doc);
        }
      }
      return cmp;
    }

    private int compare(List<CharSequence> left, List<CharSequence> right) {
      final int len = left.size() < right.size() ? left.size() : right.size();
      for (int i = 0, j = 0; i < len; i++, j++) {
        CharSequence leftValue = left.get(i);
        CharSequence rightValue = right.get(i);
        if (leftValue == null || rightValue == null) {
          if (leftValue == null && rightValue == null) {
            return 0;
          } else if (leftValue == null) {
            return 1;
          } else {
            return -1;
          }
        }
        int cmp = Lookup.CHARSEQUENCE_COMPARATOR.compare(leftValue, rightValue);
        if (cmp < 0) {
          return -1;
        }
        if (cmp > 0) {
          return 1;
        }
      }
      return left.size() - right.size();
    }
  }

  /**
   * {@link org.apache.lucene.search.TopDocs} wrapper with
   * {@link TopSuggestDocs.SuggestScoreDoc}
   * instead of {@link org.apache.lucene.search.ScoreDoc}
   */
  public TopSuggestDocs(int totalHits, SuggestScoreDoc[] scoreDocs, float maxScore) {
    super(totalHits, scoreDocs, maxScore);
  }

  /**
   * Returns {@link TopSuggestDocs.SuggestScoreDoc}s
   * for this instance
   */
  public SuggestScoreDoc[] scoreLookupDocs() {
    return (SuggestScoreDoc[]) scoreDocs;
  }

  /**
   * Returns a new TopSuggestDocs, containing topN results across
   * the provided TopSuggestDocs, sorting by score. Each {@link TopSuggestDocs}
   * instance must be sorted.
   * Analogous to {@link org.apache.lucene.search.TopDocs#merge(int, org.apache.lucene.search.TopDocs[])}
   * for {@link TopSuggestDocs}
   *
   * NOTE: assumes every <code>shardHit</code> is already sorted by score
   */
  public static TopSuggestDocs merge(int topN, TopSuggestDocs[] shardHits) {
    SuggestScoreDocPriorityQueue priorityQueue = new SuggestScoreDocPriorityQueue(topN);
    for (TopSuggestDocs shardHit : shardHits) {
      for (SuggestScoreDoc scoreDoc : shardHit.scoreLookupDocs()) {
        if (scoreDoc == priorityQueue.insertWithOverflow(scoreDoc)) {
          break;
        }
      }
    }
    SuggestScoreDoc[] topNResults = priorityQueue.getResults();
    if (topNResults.length > 0) {
      return new TopSuggestDocs(topNResults.length, topNResults, topNResults[0].score);
    } else {
      return TopSuggestDocs.EMPTY;
    }
  }

}
