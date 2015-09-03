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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.search.suggest.document.TestSuggestField.*;
import static org.hamcrest.CoreMatchers.equalTo;

public class TestTopSuggestDocsCollector extends LuceneTestCase {

  public Directory dir;

  @Before
  public void before() throws Exception {
    dir = newDirectory();
  }

  @After
  public void after() throws Exception {
    dir.close();
  }

  @Test
  public void testSimple() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();
    document.add(new SuggestField("suggest_field", "abc", 3));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("suggest_field", "abd", 4));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("suggest_field", "The Foo Fighters", 2));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("suggest_field", "abcdd", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "ab"));
    TopSuggestDocsCollector collector = new TopSuggestDocsCollector(3);
    suggestIndexSearcher.suggest(query, collector);
    TopSuggestDocs suggestDocs = collector.get();
    assertSuggestions(suggestDocs, new Entry("abcdd", 5), new Entry("abd", 4), new Entry("abc", 3));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultipleSimple() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();
    document.add(new SuggestField("suggest_field", "abd", 4));
    document.add(new SuggestField("suggest_field", "abb", 2));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("suggest_field", "abc", 3));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("suggest_field", "abcdd", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "ab"));
    TopSuggestDocsCollector collector = new TopSuggestDocsCollector(4);
    suggestIndexSearcher.suggest(query, collector);
    TopSuggestDocs suggestDocs = collector.get();
    assertSuggestions(suggestDocs, new Entry(Arrays.asList("abd", "abb"), 6), new Entry("abcdd", 5), new Entry("abc", 3));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultipleHit() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();
    document.add(new SuggestField("suggest_field", "abd", 4));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("suggest_field", "abc", 3));
    document.add(new SuggestField("suggest_field", "The Foo Fighters", 2));
    document.add(new SuggestField("suggest_field", "abcdd", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "ab"));
    TopSuggestDocsCollector collector = new TopSuggestDocsCollector(3);
    suggestIndexSearcher.suggest(query, collector);
    TopSuggestDocs suggestDocs = collector.get();
    assertTrue(suggestDocs.totalHits == 2);
    //assertSuggestions(suggestDocs, new Entry("abcdd", 5), new Entry("abd", 4), new Entry("abc", 3));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultipleHitMultipleDocs() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();
    document.add(new SuggestField("suggest_field", "abc", 3));
    document.add(new SuggestField("suggest_field", "abd", 4));
    document.add(new SuggestField("suggest_field", "The Foo Fighters", 2));
    document.add(new SuggestField("suggest_field", "abcdd", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "ab"));
    TopSuggestDocsCollector collector = new TopSuggestDocsCollector(3);
    suggestIndexSearcher.suggest(query, collector);
    TopSuggestDocs suggestDocs = collector.get();
    assertTrue(suggestDocs.totalHits == 1);
    //assertSuggestions(suggestDocs, new Entry("abcdd", 5), new Entry("abd", 4), new Entry("abc", 3));

    reader.close();
    iw.close();
  }

  @Test
  public void testScoring() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));

    int num = Math.min(1000, atLeast(100));
    String[] prefixes = {"abc", "bac", "cab"};
    Map<String, Integer> mappings = new HashMap<>();
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      String suggest = prefixes[i % 3] + TestUtil.randomSimpleString(random(), 10) + "_" +String.valueOf(i);
      int weight = Math.abs(random().nextInt());
      document.add(new SuggestField("suggest_field", suggest, weight));
      mappings.put(suggest, weight);
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    for (String prefix : prefixes) {
      PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", prefix));
      TopSuggestDocsCollector collector = new TopSuggestDocsCollector(num);
      indexSearcher.suggest(query, collector);
      TopSuggestDocs suggest = collector.get();
      assertTrue(suggest.totalHits > 0);
      float topScore = -1;
      for (TopSuggestDocs.SuggestScoreDoc scoreDoc : suggest.scoreLookupDocs()) {
        if (topScore != -1) {
          assertTrue(topScore >= scoreDoc.score);
        }
        topScore = scoreDoc.score;
        assertThat((float) mappings.get(scoreDoc.keys.get(0).toString()), equalTo(scoreDoc.score));
        assertNotNull(mappings.remove(scoreDoc.keys.get(0).toString()));
      }
    }

    assertThat(mappings.size(), equalTo(0));
    reader.close();
    iw.close();
  }
  @Test
  public void testRandomDocs() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int nDoc = atLeast(10);
    int nSuggestion = atLeast(3);
    while (nDoc * nSuggestion > 5000) {
      nDoc = atLeast(10);
      nSuggestion = atLeast(3);
    }
    for (int i = 0; i < nDoc; i++) {
      Document document = new Document();
      for (int j = 0; j < nSuggestion; j++) {
        document.add(new SuggestField("suggest_field", "a", TestUtil.nextInt(random(), 1, 100)));
      }
      iw.addDocument(document);
      //System.out.println(document.toString());
      if (rarely()) {
        iw.commit();
      }
    }


    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "a"));
    TopSuggestDocsCollector collector = new TopSuggestDocsCollector(nDoc);
    suggestIndexSearcher.suggest(query, collector);
    TopSuggestDocs suggestDocs = collector.get();
    assertThat(suggestDocs.totalHits, equalTo(nDoc));
    float maxScore = Float.MAX_VALUE;
    for (TopSuggestDocs.SuggestScoreDoc scoreDoc : suggestDocs.scoreLookupDocs()) {
      assertTrue(scoreDoc.score <= maxScore);
      maxScore = scoreDoc.score;
    }
    //assertSuggestions(suggestDocs, new Entry("abcdd", 5), new Entry("abd", 4), new Entry("abc", 3));

    reader.close();
    iw.close();

  }
}
