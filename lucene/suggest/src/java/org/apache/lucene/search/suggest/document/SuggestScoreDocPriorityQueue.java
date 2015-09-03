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

import org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDoc;
import org.apache.lucene.util.PriorityQueue;

/**
 * Bounded priority queue for {@link SuggestScoreDoc}s.
 * Priority is based on {@link SuggestScoreDoc#score} and tie
 * is broken by {@link SuggestScoreDoc#doc}
 */
final class SuggestScoreDocPriorityQueue extends PriorityQueue<SuggestScoreDoc> {

  //private final Object[] heap;
  /**
   * Creates a new priority queue of the specified size.
   */
  public SuggestScoreDocPriorityQueue(int size) {
    super(size);
    //this.heap = getHeapArray();
  }

  @Override
  protected boolean lessThan(SuggestScoreDoc a, SuggestScoreDoc b) {
    return a.compareTo(b) > 0;
  }
  /*
  public void update(SuggestScoreDoc scoreDoc) {
    for (int i = 1; i <= size(); i++) {
      if (heap[i] == scoreDoc) {
        downHeap(i);
      }
    }
  }

  private final void downHeap(int i) {
    Object node = heap[i];          // save top node
    int j = i << 1;            // find smaller child
    int k = j + 1;
    if (k <= size() && lessThan(heap[k], heap[j])) {
      j = k;
    }
    while (j <= size() && lessThan(heap[j], node)) {
      heap[i] = heap[j];       // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size() && lessThan(heap[k], heap[j])) {
        j = k;
      }
    }
    heap[i] = node;            // install saved node
  }
  */
  /**
   * Returns the top N results in descending order.
   */
  public SuggestScoreDoc[] getResults() {
    int size = size();
    SuggestScoreDoc[] res = new SuggestScoreDoc[size];
    for (int i = size - 1; i >= 0; i--) {
      res[i] = pop();
    }
    return res;
  }
}
