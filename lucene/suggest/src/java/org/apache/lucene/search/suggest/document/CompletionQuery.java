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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.HOLE_CHARACTER;
import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.SEP_LABEL;

/**
 * Abstract {@link Query} that match documents containing terms with a specified prefix
 * filtered by {@link Filter}. This should be used to query against any {@link SuggestField}s
 * or {@link ContextSuggestField}s of documents.
 * Suggestion scoring can be configured by {@link #setScoreMode(ScoreMode)}
 * <p>
 * Use {@link SuggestIndexSearcher#suggest(CompletionQuery, int)} to execute any query
 * that provides a concrete implementation of this query. Example below shows using this query
 * to retrieve the top 5 documents.
 *
 * <pre class="prettyprint">
 *  SuggestIndexSearcher searcher = new SuggestIndexSearcher(reader);
 *  TopSuggestDocs suggestDocs = searcher.suggest(query, 5);
 * </pre>
 * This query rewrites to an appropriate {@link CompletionQuery} depending on the
 * type ({@link SuggestField} or {@link ContextSuggestField}) of the field the query is run against.
 *
 * @lucene.experimental
 */
public abstract class CompletionQuery extends Query {

  /**
   * Interface that defines how suggestions are scored
   */
  public interface ScoreMode {

    /**
     * Scores a suggestion
     *
     * @param weight index-time weight
     * @param boost query-time boost
     * @param minWeight minimum index-time weight
     * @param maxWeight maximum index-time weight
     * @return computed score
     */
    float score(float weight, float boost, float minWeight, float maxWeight);
  }

  /**
   * Scores the suggestion using only index-time weight
   */
  public final static ScoreMode IGNORE_BOOST = new ScoreMode() {
    @Override
    public float score(float weight, float boost, float minWeight, float maxWeight) {
      return weight;
    }
  };

  /**
   * Scores the suggestion by giving precedence to
   * the query-time boost
   */
  public final static ScoreMode BOOST_FIRST = new ScoreMode() {
    @Override
    public float score(float weight, float boost, float minWeight, float maxWeight) {
      if (maxWeight == 0f && minWeight == 0f) {
        return boost;
      }
      return weight + (boost * maxWeight);
    }
  };

  /**
   * Scores the suggestion by multiplying the
   * index-time weight with query-time boost
   */
  public final static ScoreMode BOOST = new ScoreMode() {
    @Override
    public float score(float weight, float boost, float minWeight, float maxWeight) {
      if (boost == 0f) {
        return weight;
      }
      return weight * boost;
    }
  };

  /**
   * Term to query against
   */
  private final Term term;

  /**
   * Filter for document scoping
   */
  private final Filter filter;

  /**
   * Defines how suggestions should be scored
   */
  private ScoreMode scoreMode = IGNORE_BOOST;

  /**
   * Creates a base Completion query against a <code>term</code>
   * with a <code>filter</code> to scope the documents
   */
  protected CompletionQuery(Term term, Filter filter) throws IOException {
    validate(term.text());
    this.term = term;
    this.filter = filter;
  }

  /**
   * Returns the filter for the query, used to
   * suggest completions on a subset of indexed documents
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * Returns the field name this query should
   * be run against
   */
  public String getField() {
    return term.field();
  }

  /**
   * Returns the term to be queried against
   */
  public Term getTerm() {
    return term;
  }

  /**
   * Sets the score mode defining how suggestions
   * would be scored when executing the query.
   * Use the pre-defined score modes: {@link #BOOST},
   * {@link #BOOST_FIRST}, {@link #IGNORE_BOOST} or
   * implement your own {@link ScoreMode}
   */
  public void setScoreMode(ScoreMode scoreMode) {
    this.scoreMode = scoreMode;
  }

  /**
   * Returns the score mode to be used
   * when executing the query.
   * See {@link #setScoreMode(ScoreMode)}
   */
  public ScoreMode getScoreMode() {
    return scoreMode;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    byte type = 0;
    boolean first = true;
    Terms terms;
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader leafReader = context.reader();
      try {
        if ((terms = leafReader.terms(getField())) == null) {
          continue;
        }
      } catch (IOException e) {
        continue;
      }
      if (terms instanceof CompletionTerms) {
        CompletionTerms completionTerms = (CompletionTerms) terms;
        byte t = completionTerms.getType();
        if (first) {
          type = t;
          first = false;
        } else if (type != t) {
          throw new IllegalStateException(getField() + " has values of multiple types");
        }
      }
    }

    if (first == false) {
      if (this instanceof ContextQuery) {
        if (type == SuggestField.TYPE) {
          throw new IllegalStateException(this.getClass().getSimpleName()
              + " can not be executed against a non context-enabled SuggestField: "
              + getField());
        }
      } else {
        if (type == ContextSuggestField.TYPE) {
          return new ContextQuery(this);
        }
      }
    }
    return this;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(term.text());
    buffer.append('*');
    if (filter != null) {
      buffer.append(",");
      buffer.append("filter");
      buffer.append(":");
      buffer.append(filter.toString(field));
    }
    return buffer.toString();
  }

  private void validate(String termText) {
    for (int i = 0; i < termText.length(); i++) {
      switch (termText.charAt(i)) {
        case HOLE_CHARACTER:
          throw new IllegalArgumentException(
              "Term text cannot contain HOLE character U+001E; this character is reserved");
        case SEP_LABEL:
          throw new IllegalArgumentException(
              "Term text cannot contain unit separator character U+001F; this character is reserved");
        default:
          break;
      }
    }
  }
}
