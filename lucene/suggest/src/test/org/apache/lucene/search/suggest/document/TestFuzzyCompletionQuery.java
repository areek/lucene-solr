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

import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.search.suggest.document.CompletionQuery.BOOST;
import static org.apache.lucene.search.suggest.document.CompletionQuery.BOOST_FIRST;
import static org.apache.lucene.search.suggest.document.CompletionQuery.IGNORE_BOOST;
import static org.apache.lucene.search.suggest.document.TestSuggestField.Entry;
import static org.apache.lucene.search.suggest.document.TestSuggestField.assertSuggestions;
import static org.apache.lucene.search.suggest.document.TestSuggestField.iwcWithSuggestField;

public class TestFuzzyCompletionQuery extends LuceneTestCase {
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
  public void testFuzzyQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new SuggestField("suggest_field", "suggestion", 2));
    document.add(new SuggestField("suggest_field", "suaggestion", 4));
    document.add(new SuggestField("suggest_field", "ssuggestion", 3));
    iw.addDocument(document);
    document.clear();
    document.add(new SuggestField("suggest_field", "sugfoo", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    int maxWeight = 4;
    assertSuggestions(suggest,
        new Entry("suggestion", 2 + (maxWeight * 3)),
        new Entry("sugfoo", 1 + (maxWeight * 3)),
        new Entry("suaggestion", 4 + (maxWeight * 2)),
        new Entry("ssuggestion", 3 + (maxWeight)));

    reader.close();
    iw.close();
  }

  @Test
  public void testFuzzyQueryWithBoostScoreMode() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new SuggestField("suggest_field", "suggestion", 2));
    document.add(new SuggestField("suggest_field", "suaggestion", 4));
    document.add(new SuggestField("suggest_field", "ssuggestion", 1));
    iw.addDocument(document);
    document.clear();
    document.add(new SuggestField("suggest_field", "sugfoo", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
    query.setScoreMode(BOOST);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest,
        new Entry("suaggestion", 4 * 2),
        new Entry("suggestion", 2 * 3),
        new Entry("sugfoo", 1 * 3),
        new Entry("ssuggestion", 1 * 1)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testFuzzyQueryWithIgnoreBoostScoreMode() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new SuggestField("suggest_field", "suggestion", 4));
    document.add(new SuggestField("suggest_field", "suaggestion", 3));
    document.add(new SuggestField("suggest_field", "ssuggestion", 2));
    iw.addDocument(document);
    document.clear();
    document.add(new SuggestField("suggest_field", "sugfoo", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
    query.setScoreMode(IGNORE_BOOST);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest,
        new Entry("suggestion", 4),
        new Entry("suaggestion", 3),
        new Entry("ssuggestion", 2),
        new Entry("sugfoo", 1)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testFuzzyContextQueryWithBoostFirstScoreMode() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type1"), "sduggestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type2"), "sudggestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type3"), "sugdgestion", 1));
    iw.addDocument(document);
    document.clear();
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type4"), "suggdestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type4"), "suggestion", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query =  new ContextQuery(new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugge")));
    query.setScoreMode(BOOST_FIRST);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    int maxWeight = 1;
    assertSuggestions(suggest,
        new Entry("suggestion", "type4", 1 + (maxWeight * (1 + 4))),
        new Entry("suggdestion", "type4", 1 + (maxWeight * (1 + 4))),
        new Entry("sugdgestion", "type3", 1 + (maxWeight * (1 + 3))),
        new Entry("sudggestion", "type2", 1 + (maxWeight * (1 + 2))),
        new Entry("sduggestion", "type1", 1 + (maxWeight * (1 + 1))));

    reader.close();
    iw.close();
  }

  @Test
  public void testFuzzyContextQueryWithBoostScoreMode() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type1"), "sduggestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type2"), "sudggestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type3"), "sugdgestion", 1));
    iw.addDocument(document);
    document.clear();
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type4"), "suggdestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type4"), "suggestion", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query =  new ContextQuery(new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugge")));
    query.setScoreMode(BOOST);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion", "type4", 1 + 4),
        new Entry("suggdestion", "type4", 1 + 4),
        new Entry("sugdgestion", "type3", 1 + 3),
        new Entry("sudggestion", "type2", 1 + 2),
        new Entry("sduggestion", "type1", 1 + 1)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testFuzzyContextQueryWithInnerIgnoreBoostScoreMode() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type1"), "sduggestion", 6));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type2"), "sudggestion", 5));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type3"), "sugdgestion", 4));
    iw.addDocument(document);
    document.clear();
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type4"), "suggdestion", 3));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type4"), "suggestion", 2));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugge"));
    query.setScoreMode(IGNORE_BOOST);
    ContextQuery contextQuery = new ContextQuery(query);
    contextQuery.addContext("type4", 4);
    contextQuery.addContext("*");
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(contextQuery, 5);
    assertSuggestions(suggest,
        new Entry("suggdestion", "type4", 4 * 3),
        new Entry("suggestion",  "type4", 4 * 2),
        new Entry("sduggestion", "type1", 1 * 6),
        new Entry("sudggestion", "type2", 1 * 5),
        new Entry("sugdgestion", "type3", 1 * 4)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testFuzzyFilteredContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type1"), "sduggestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type2"), "sudggestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type3"), "sugdgestion", 1));
    iw.addDocument(document);
    document.clear();
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type4"), "suggdestion", 1));
    document.add(new ContextSuggestField("suggest_field", Collections.singletonList("type4"), "suggestion", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery fuzzyQuery = new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugge"));
    ContextQuery contextQuery = new ContextQuery(fuzzyQuery);
    contextQuery.setScoreMode(BOOST);
    contextQuery.addContext("type1", 6);
    contextQuery.addContext("type3", 2);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(contextQuery, 5);
    assertSuggestions(suggest,
        new Entry("sduggestion", "type1", 1 * (1 + 6)),
        new Entry("sugdgestion", "type3", 1 * (3 + 2))
    );

    reader.close();
    iw.close();
  }
}
