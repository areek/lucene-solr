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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.search.suggest.document.TestSuggestField.Entry;
import static org.apache.lucene.search.suggest.document.TestSuggestField.assertSuggestions;
import static org.apache.lucene.search.suggest.document.TestSuggestField.iwcWithSuggestField;

public class TestContextQuery extends LuceneTestCase {
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
  public void testSimpleContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 8, "type1"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 7, "type2"));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 6, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 5, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type1", 1);
    query.addContext("type2", 2);
    query.addContext("type3", 3);
    query.addContext("type4", 4);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion4", 5 * 4, "type4"),
        new Entry("suggestion3", 6 * 3, "type3"),
        new Entry("suggestion2", 7 * 2, "type2"),
        new Entry("suggestion1", 8 * 1, "type1")
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testContextQueryOnSuggestField() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new SuggestField("suggest_field", "abc", 3));
    document.add(new SuggestField("suggest_field", "abd", 4));
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
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "ab")));
    try {
      suggestIndexSearcher.suggest(query, 4);
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("SuggestField"));
    }
    reader.close();
    iw.close();
  }

  @Test
  public void testNonExactContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 4, "type1"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3, "type2"));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 2, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type", 1, false);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4, "type1"),
        new Entry("suggestion2", 3, "type2"),
        new Entry("suggestion3", 2, "type3"),
        new Entry("suggestion4", 1, "type4"));

    reader.close();
    iw.close();
  }

  @Test
  public void testContextPrecedenceBoost() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 4, "typetype"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3, "type"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type", 1);
    query.addContext("typetype", 2);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4 * 2, "typetype"),
        new Entry("suggestion2", 3 * 1, "type")
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testEmptyContext() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion_no_ctx", 4));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion_no_ctx", 4, null),
        new Entry("suggestion", 1, "type4"));

    reader.close();
    iw.close();
  }

  @Test
  public void testEmptyContextWithBoosts() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 2));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);

    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type4", 10);
    query.addAllContexts();
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion4", 1 * 10, "type4"),
        new Entry("suggestion1", 4, null),
        new Entry("suggestion2", 3, null),
        new Entry("suggestion3", 2, null)
    );
    reader.close();
    iw.close();
  }

  @Test
  public void testSameSuggestionMultipleContext() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion", 4, "type1", "type2", "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type1", 10);
    query.addContext("type2", 2);
    query.addContext("type3", 3);
    query.addContext("type4", 4);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion", 4 * 10, "type1"),
        new Entry("suggestion", 4 * 3, "type3"),
        new Entry("suggestion", 4 * 2, "type2"),
        new Entry("suggestion", 1 * 4, "type4")
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testMixedContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 4, "type1"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3, "type2"));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 2, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type1", 7);
    query.addContext("type2", 6);
    query.addAllContexts();
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4 * 7, "type1"),
        new Entry("suggestion2", 3 * 6, "type2"),
        new Entry("suggestion3", 2, "type3"),
        new Entry("suggestion4", 1, "type4")
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testFilteringContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 4, "type1"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3, "type2"));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 2, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type3", 3);
    query.addContext("type4", 4);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion3", 2 * 3, "type3"),
        new Entry("suggestion4", 1 * 4, "type4")
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testContextQueryRewrite() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 4, "type1"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3, "type2"));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 2, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4, "type1"),
        new Entry("suggestion2", 3, "type2"),
        new Entry("suggestion3", 2, "type3"),
        new Entry("suggestion4", 1, "type4"));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultiContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 8, "type1", "type3"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 7, "type2"));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 6, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 5, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type1", 1);
    query.addContext("type2", 2);
    query.addContext("type3", 3);
    query.addContext("type4", 4);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", 8 * 3, "type3"),
        new Entry("suggestion4", 5 * 4, "type4"),
        new Entry("suggestion3", 6 * 3, "type3"),
        new Entry("suggestion2", 7 * 2, "type2"),
        new Entry("suggestion1", 8 * 1, "type1"));

    reader.close();
    iw.close();
  }

  @Test
  public void testAllContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 4, "type1"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3, "type2"));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 2, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4, "type1"),
        new Entry("suggestion2", 3, "type2"),
        new Entry("suggestion3", 2, "type3"),
        new Entry("suggestion4", 1, "type4"));

    reader.close();
    iw.close();
  }

  @Test
  public void testNestedContextQuerySimple() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(contextSuggestField("suggest_field", "suggestion1", 4, new CharSequence[] {"type1"}, new CharSequence[] {"cat1"}));
    document.add(contextSuggestField("suggest_field", "suggestion2", 3, new CharSequence[] {"type2"}, new CharSequence[] {"cat2"}));
    document.add(contextSuggestField("suggest_field", "suggestion3", 2, new CharSequence[] {"type3"}, new CharSequence[] {"cat3"}));
    iw.addDocument(document);

    document = new Document();
    document.add(contextSuggestField("suggest_field", "suggestion4", 1, new CharSequence[] {"type4"}, new CharSequence[] {"cat4"}));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg"))));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4, "type1", "cat1"),
        new Entry("suggestion2", 3, "type2", "cat2"),
        new Entry("suggestion3", 2, "type3", "cat3"),
        new Entry("suggestion4", 1, "type4", "cat4"));

    reader.close();
    iw.close();
  }

  @Test
  public void testNestedContextQueryWithMissingContext() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(contextSuggestField("suggest_field", "suggestion1", 4, new CharSequence[] {"type1"}, new CharSequence[] {"cat1"}));
    document.add(contextSuggestField("suggest_field", "suggestion2", 3, new CharSequence[] {"type2"}, new CharSequence[0]));
    document.add(contextSuggestField("suggest_field", "suggestion3", 2, new CharSequence[] {"type3"}, new CharSequence[] {"cat3"}));
    iw.addDocument(document);

    document = new Document();
    document.add(contextSuggestField("suggest_field", "suggestion4", 1, new CharSequence[] {"type4"}, new CharSequence[0]));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg"))));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4, "type1", "cat1"),
        new Entry("suggestion2", 3, "type2", null),
        new Entry("suggestion3", 2, "type3", "cat3"),
        new Entry("suggestion4", 1, "type4", null));

    reader.close();
    iw.close();
  }

  @Test
  public void testNestedContextQueryFilteringSingle() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(contextSuggestField("suggest_field", "suggestion1", 4, new CharSequence[] {"type1"}, new CharSequence[] {"cat1"}));
    document.add(contextSuggestField("suggest_field", "suggestion2", 3, new CharSequence[] {"type2"}, new CharSequence[] {"cat2"}));
    document.add(contextSuggestField("suggest_field", "suggestion3", 2, new CharSequence[] {"type3"}, new CharSequence[] {"cat3"}));
    iw.addDocument(document);

    document = new Document();
    document.add(contextSuggestField("suggest_field", "suggestion4", 1, new CharSequence[] {"type4"}, new CharSequence[] {"cat4"}));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery innerQuery = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    ContextQuery query = new ContextQuery(innerQuery);
    query.addContext("type1");
    query.addContext("type3");
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4, "type1", "cat1"),
        new Entry("suggestion3", 2, "type3", "cat3"));

    reader.close();
    iw.close();
  }

  @Test
  public void testNestedContextQueryFiltering() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(contextSuggestField("suggest_field", "suggestion1", 4, new CharSequence[] {"type1"}, new CharSequence[] {"cat1"}));
    document.add(contextSuggestField("suggest_field", "suggestion2", 3, new CharSequence[] {"type2"}, new CharSequence[] {"cat2"}));
    document.add(contextSuggestField("suggest_field", "suggestion3", 2, new CharSequence[] {"type3"}, new CharSequence[] {"cat3"}));
    iw.addDocument(document);

    document = new Document();
    document.add(contextSuggestField("suggest_field", "suggestion4", 1, new CharSequence[] {"type4"}, new CharSequence[] {"cat4"}));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery innerQuery = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    innerQuery.addContext("cat1");
    ContextQuery query = new ContextQuery(innerQuery);
    query.addContext("type1");
    query.addContext("type3");
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest,
        new Entry("suggestion1", 8, "type1", "cat1")
    );

    reader.close();
    iw.close();
  }


  private static ContextSuggestField contextSuggestField(String name, String value, int weight, CharSequence[]... contexts) {
    return new ContextSuggestField(name, value, weight) {
      CharsRefBuilder scratch = new CharsRefBuilder();

      @Override
      protected Set<CharSequence> contexts() {
        Set<CharSequence> ctxs = new HashSet<>();
        for (CharSequence ctx : contexts[0]) {
          scratch.append(ctx);
          for (int i = 1; i < contexts.length; i++) {
            if (contexts[i].length == 0) {
              scratch.append((char) CONTEXT_SEPARATOR);
            } else {
              for (CharSequence innerCtx : contexts[i]) {
                scratch.append((char) CONTEXT_SEPARATOR);
                scratch.append(innerCtx);
              }
            }
          }
          ctxs.add(scratch.toCharsRef());
          scratch.clear();
        }
        return ctxs;
      }
    };
  }

  @Test
  public void testRandomContextQueryScoring() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    try(RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"))) {
      int numSuggestions = atLeast(20);
      int numContexts = atLeast(5);

      Set<Integer> seenWeights = new HashSet<>();
      List<Entry> expectedEntries = new ArrayList<>();
      List<CharSequence> contexts = new ArrayList<>();
      for (int i = 1; i <= numContexts; i++) {
        CharSequence context = TestUtil.randomSimpleString(random(), 10) + i;
        contexts.add(context);
        for (int j = 1; j <= numSuggestions; j++) {
          String suggestion = "sugg_" + TestUtil.randomSimpleString(random(), 10) + j;
          int weight = TestUtil.nextInt(random(), 1, 1000 * numContexts * numSuggestions);
          while (seenWeights.contains(weight)) {
            weight = TestUtil.nextInt(random(), 1, 1000 * numContexts * numSuggestions);
          }
          seenWeights.add(weight);
          Document document = new Document();
          document.add(new ContextSuggestField("suggest_field", suggestion, weight, context));
          iw.addDocument(document);
          expectedEntries.add(new Entry(suggestion, i * weight, context.toString()));
        }
        if (rarely()) {
          iw.commit();
        }
      }
      Entry[] expectedResults = expectedEntries.toArray(new Entry[expectedEntries.size()]);

      ArrayUtil.introSort(expectedResults, new Comparator<Entry>() {
        @Override
        public int compare(Entry o1, Entry o2) {
          int cmp = Float.compare(o2.value, o1.value);
          if (cmp != 0) {
            return cmp;
          } else {
            return o1.output.compareTo(o2.output);
          }
        }
      });

      try(DirectoryReader reader = iw.getReader()) {
        SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
        ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
        for (int i = 0; i < contexts.size(); i++) {
          query.addContext(contexts.get(i), i + 1);
        }
        TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
        assertSuggestions(suggest, Arrays.copyOfRange(expectedResults, 0, 4));
      }
    }
  }
}
