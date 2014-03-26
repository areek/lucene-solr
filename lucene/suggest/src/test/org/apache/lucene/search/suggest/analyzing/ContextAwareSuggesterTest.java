package org.apache.lucene.search.suggest.analyzing;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

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

public class ContextAwareSuggesterTest extends LuceneTestCase {
  
  private static Set<BytesRef> asContexts(String... vals) {
    Set<BytesRef> contexts = new HashSet<>();
    for (String v : vals) {
      contexts.add(new BytesRef(v));
    }
    return contexts;
  }
  
  public void testBasicContext() throws IOException {
    
    Input keys[] = new Input[] {
        new Input("lend me your ear", 8, new BytesRef("foobar"), asContexts("foo", "bar")),
        new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz"), asContexts("foo", "baz"))
        };
    
    ContextAwareSuggester suggester;
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    suggester = new ContextAwareSuggester(a, false);
    suggester.build(new InputArrayIterator(keys));
    
    // No context provided, all results returned
    List<LookupResult> results = suggester.lookup(
        TestUtil.stringToCharSequence("lend", random()), asContexts("foo"), false, 10);
    assertEquals(1, results.size());
    
    LookupResult result = results.get(0);
    assertEquals("lend me your ear", result.key);
    assertEquals(8, result.value);
    assertEquals(new BytesRef("foobar"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("baz")));
    /*
    result = results.get(1);
    assertEquals("lend me your <b>ear</b>", result.key);
    assertEquals(8, result.value);
    assertEquals(new BytesRef("foobar"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("bar")));
    
    // Both suggestions have "foo" context:
    results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()),
        asSet(new BytesRef("foo")), 10, true, true);
    assertEquals(2, results.size());
    
    result = results.get(0);
    assertEquals("a penny saved is a penny <b>ear</b>ned", result.key);
    assertEquals(10, result.value);
    assertEquals(new BytesRef("foobaz"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("baz")));
    
    result = results.get(1);
    assertEquals("lend me your <b>ear</b>", result.key);
    assertEquals(8, result.value);
    assertEquals(new BytesRef("foobar"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("bar")));
    
    // Only one has "bar" context:
    results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()),
        asSet(new BytesRef("bar")), 10, true, true);
    assertEquals(1, results.size());
    
    result = results.get(0);
    assertEquals("lend me your <b>ear</b>", result.key);
    assertEquals(8, result.value);
    assertEquals(new BytesRef("foobar"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("bar")));
    
    // Only one has "baz" context:
    results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()),
        asSet(new BytesRef("baz")), 10, true, true);
    assertEquals(1, results.size());
    
    result = results.get(0);
    assertEquals("a penny saved is a penny <b>ear</b>ned", result.key);
    assertEquals(10, result.value);
    assertEquals(new BytesRef("foobaz"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("baz")));
    
    // Both have foo or bar:
    results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()),
        asSet(new BytesRef("foo"), new BytesRef("bar")), 10, true, true);
    assertEquals(2, results.size());
    
    result = results.get(0);
    assertEquals("a penny saved is a penny <b>ear</b>ned", result.key);
    assertEquals(10, result.value);
    assertEquals(new BytesRef("foobaz"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("baz")));
    
    result = results.get(1);
    assertEquals("lend me your <b>ear</b>", result.key);
    assertEquals(8, result.value);
    assertEquals(new BytesRef("foobar"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("bar")));
    
    suggester.close();
    */
  }
}