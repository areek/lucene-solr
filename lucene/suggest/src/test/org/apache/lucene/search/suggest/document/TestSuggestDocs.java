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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;


public class TestSuggestDocs extends LuceneTestCase {

  @Test
  public void testSortingTieOnDocId() throws Exception {
    TopSuggestDocs.SuggestScoreDoc first = new TopSuggestDocs.SuggestScoreDoc(0, "a", "a", 1);
    TopSuggestDocs.SuggestScoreDoc second = new TopSuggestDocs.SuggestScoreDoc(1, "a", "a", 1);
    assertTrue(first.compareTo(second) < 0);
  }

  @Test
  public void testSortingTieOnContexts() throws Exception {
    TopSuggestDocs.SuggestScoreDoc first = new TopSuggestDocs.SuggestScoreDoc(random().nextInt(), "a", "a", 1);
    TopSuggestDocs.SuggestScoreDoc second = new TopSuggestDocs.SuggestScoreDoc(random().nextInt(), "a", "b", 1);
    assertTrue(first.compareTo(second) < 0);
  }

  @Test
  public void testSortingTieOnKeys() throws Exception {
    TopSuggestDocs.SuggestScoreDoc first = new TopSuggestDocs.SuggestScoreDoc(random().nextInt(), "a", TestUtil.randomSimpleString(random(), 10), 1);
    TopSuggestDocs.SuggestScoreDoc second = new TopSuggestDocs.SuggestScoreDoc(random().nextInt(), "b", TestUtil.randomSimpleString(random(), 10), 1);
    assertTrue(first.compareTo(second) < 0);
  }

  @Test
  public void testSorting() throws Exception {
    TopSuggestDocs.SuggestScoreDoc first = new TopSuggestDocs.SuggestScoreDoc(random().nextInt(), TestUtil.randomSimpleString(random(), 10), TestUtil.randomSimpleString(random(), 10), 2);
    TopSuggestDocs.SuggestScoreDoc second = new TopSuggestDocs.SuggestScoreDoc(random().nextInt(), TestUtil.randomSimpleString(random(), 10), TestUtil.randomSimpleString(random(), 10), 1);
    assertTrue(first.compareTo(second) < 0);
  }
}
