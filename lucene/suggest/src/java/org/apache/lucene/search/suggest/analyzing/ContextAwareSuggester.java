package org.apache.lucene.search.suggest.analyzing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;


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

public class ContextAwareSuggester extends Lookup {

  private Map<String, AnalyzingSuggester> suggesterMap = new ConcurrentHashMap<>();
  private final SuggesterConfig suggesterConfig;
  private int count = 0;
  private class SuggesterConfig {
    private final Analyzer indexAnalyzer;
    private final Analyzer queryAnalyzer;
    private int options;
    private int maxSurfaceFormsPerAnalyzedForm;
    private int maxGraphExpansions;
    private boolean preservePositionIncrements;
    private final boolean isFuzzy;
    private int maxEdits;
    private boolean transpositions;
    private int nonFuzzyPrefix;
    private int minFuzzyLength;
    private boolean unicodeAware;
    
    SuggesterConfig(Analyzer analyer, boolean isFuzzy) {
      this(analyer, analyer, isFuzzy);
    }
    
    SuggesterConfig(Analyzer indexAnalyer, Analyzer queryAnalyer, boolean isFuzzy) {
      this.indexAnalyzer = indexAnalyer;
      this.queryAnalyzer = queryAnalyer;
      this.isFuzzy = isFuzzy;
      options = AnalyzingSuggester.EXACT_FIRST | AnalyzingSuggester.PRESERVE_SEP;
      maxSurfaceFormsPerAnalyzedForm = 256;
      maxGraphExpansions = -1;
      preservePositionIncrements = true;
      if (isFuzzy) {
        maxEdits = FuzzySuggester.DEFAULT_MAX_EDITS;
        transpositions = FuzzySuggester.DEFAULT_TRANSPOSITIONS;
        nonFuzzyPrefix = FuzzySuggester.DEFAULT_NON_FUZZY_PREFIX;
        minFuzzyLength = FuzzySuggester.DEFAULT_MIN_FUZZY_LENGTH;
        unicodeAware = FuzzySuggester.DEFAULT_UNICODE_AWARE;
      }
    }
    
    SuggesterConfig(Analyzer indexAnalyzer, Analyzer queryAnalyzer, int options, 
        int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions, boolean preservePositionIncrements) {
      this.indexAnalyzer = indexAnalyzer;
      this.queryAnalyzer = queryAnalyzer;
      this.options = options;
      this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;
      this.maxGraphExpansions = maxGraphExpansions;
      this.preservePositionIncrements = preservePositionIncrements;
      this.isFuzzy = false;
    }
    
    SuggesterConfig(Analyzer indexAnalyzer, Analyzer queryAnalyzer,
        int options, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
        boolean preservePositionIncrements, int maxEdits, boolean transpositions,
        int nonFuzzyPrefix, int minFuzzyLength, boolean unicodeAware) {
      this.indexAnalyzer = indexAnalyzer;
      this.queryAnalyzer = queryAnalyzer;
      this.options = options;
      this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;
      this.maxGraphExpansions = maxGraphExpansions;
      this.preservePositionIncrements = preservePositionIncrements;
      this.maxEdits = maxEdits;
      this.transpositions = transpositions;
      this.nonFuzzyPrefix = nonFuzzyPrefix;
      this.minFuzzyLength = minFuzzyLength;
      this.unicodeAware = unicodeAware;
      this.isFuzzy = true;
    }
    
    AnalyzingSuggester constructSuggester() {
      return (isFuzzy) ?
          new FuzzySuggester(indexAnalyzer, queryAnalyzer, options, maxSurfaceFormsPerAnalyzedForm,
              maxGraphExpansions, preservePositionIncrements, maxEdits, transpositions, nonFuzzyPrefix, 
              minFuzzyLength, unicodeAware) : 
          new AnalyzingSuggester(indexAnalyzer, queryAnalyzer, options, maxSurfaceFormsPerAnalyzedForm, 
              maxGraphExpansions, preservePositionIncrements);
    }
  }
  
  private class InputEntry {
    BytesRef entry;
    long weight;
    BytesRef payload;
    Set<BytesRef> contexts;
    
    InputEntry(BytesRef entry, long weight, BytesRef payload,
        Set<BytesRef> contexts) {
      super();
      this.entry = entry;
      this.weight = weight;
      this.payload = payload;
      this.contexts = contexts;
    }
    
    
  }
  
  public ContextAwareSuggester(Analyzer analyzer, boolean isFuzzy) {
    this.suggesterConfig = new SuggesterConfig(analyzer, isFuzzy);
  }

  public ContextAwareSuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer, int options, 
      int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions, boolean preservePositionIncrements) {
    this.suggesterConfig = new SuggesterConfig(indexAnalyzer, queryAnalyzer, options, 
        maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions, preservePositionIncrements);
    
  }
  
  public ContextAwareSuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer,
      int options, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
      boolean preservePositionIncrements, int maxEdits, boolean transpositions,
      int nonFuzzyPrefix, int minFuzzyLength, boolean unicodeAware) {
    this.suggesterConfig = new SuggesterConfig(indexAnalyzer, queryAnalyzer, options, 
        maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions, preservePositionIncrements,
        maxEdits, transpositions, nonFuzzyPrefix, minFuzzyLength, unicodeAware);
    
  }

  @Override
  public long getCount() throws IOException {
    return count;
  }

  @Override
  public void build(InputIterator inputIterator) throws IOException {
    if (!inputIterator.hasContexts()) {
      throw new IllegalArgumentException("the suggester requires contexts");
    }
    Map<String, List<InputEntry>> contextEntries = new ConcurrentHashMap<>();
    String prefix = getClass().getSimpleName();
    File directory = OfflineSorter.defaultTempDir();
    File tempInput = File.createTempFile(prefix, ".input", directory);
    OfflineSorter.ByteSequencesWriter writer = new OfflineSorter.ByteSequencesWriter(tempInput);
    final boolean hasPayloads = inputIterator.hasPayloads();
    byte buffer[] = new byte[32];
    BytesRef entry;
    boolean success = false;
    try {
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
      while((entry = inputIterator.next()) != null) {
        count++;
        BytesRef payload = inputIterator.payload();
        Set<BytesRef> contexts = inputIterator.contexts();
        int requiredLength = entry.length + 8 + ((hasPayloads) ? 2 + payload.length : 0);
        for(BytesRef ctx : inputIterator.contexts()) {
          requiredLength += 2 + ctx.length;
        }
        requiredLength += 2; // for length of contexts
        
        buffer = ArrayUtil.grow(buffer, requiredLength);
        
        output.reset(buffer);
        output.writeBytes(entry.bytes, entry.offset, entry.length);
        if (hasPayloads) {
          output.writeBytes(payload.bytes, payload.offset, payload.length);
          output.writeShort((short) payload.length);
        }
        if (contexts != null) {
          for (BytesRef ctx : contexts) {
            output.writeBytes(ctx.bytes, ctx.offset, ctx.length);
            output.writeShort((short) ctx.length);
          }
          output.writeShort((short) contexts.size());
        } else {
          output.writeShort((short) 0);
        }
        output.writeLong(inputIterator.weight());
        writer.write(buffer, 0, output.getPosition());
      }
      
      
      success = true;
    } finally {
      if (success) {
        IOUtils.close(writer);
      } else {
        IOUtils.closeWhileHandlingException(writer);
      }
      tempInput.delete();
    }
   
    for (Map.Entry<String, List<InputEntry>> contextEntry : contextEntries.entrySet()) {
      AnalyzingSuggester suggester = suggesterConfig.constructSuggester();
      final Iterator<InputEntry> inputEntries = contextEntry.getValue().iterator();
      suggesterMap.put(contextEntry.getKey(), suggester);
      suggester.build(new InputIterator() {
        InputEntry current;
        
        @Override
        public BytesRef next() throws IOException {
          if (inputEntries.hasNext()) {
            current = inputEntries.next();
            return current.entry;
          } else {
            return null;
          }
        }
        
        @Override
        public long weight() {
          return (current != null) ? current.weight : 0;
        }
        
        @Override
        public BytesRef payload() {
          return (current != null) ? current.payload : null;
        }
        
        @Override
        public boolean hasPayloads() {
          return hasPayloads;
        }
        
        @Override
        public boolean hasContexts() {
          return true;
        }
        
        @Override
        public Set<BytesRef> contexts() {
          return (current != null) ? current.contexts : null;
        }
      });
    }
    
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts,
      boolean onlyMorePopular, int num) throws IOException {
    Lookup.LookupPriorityQueue lookupResults = new LookupPriorityQueue(num);
    for (BytesRef ctx : contexts) {
      AnalyzingSuggester suggester = suggesterMap.get(ctx.utf8ToString());
      if (suggester != null) {
        for (LookupResult lookupResult : suggester.lookup(key, onlyMorePopular, num)) {
          lookupResults.insertWithOverflow(lookupResult);
        }
      }
    }
    return Arrays.asList(lookupResults.getResults());
  }

  @Override
  public boolean store(DataOutput output) throws IOException {
    return false;
  }

  @Override
  public boolean load(DataInput input) throws IOException {
    return false;
  }

  @Override
  public long sizeInBytes() {
    long sizeInBytes = 0;
    for(AnalyzingSuggester suggester : suggesterMap.values()) {
      sizeInBytes += suggester.sizeInBytes();
    }
    return sizeInBytes;
  }
  
}
