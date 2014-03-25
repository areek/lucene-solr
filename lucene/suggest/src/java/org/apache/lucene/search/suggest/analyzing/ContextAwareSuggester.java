package org.apache.lucene.search.suggest.analyzing;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.ByteSequencesReader;


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
  
  private class FilteredContextInputIterator implements InputIterator {
    
    private final BytesRef context;
    private final ByteSequencesReader reader;
    private final boolean hasPayloads;
    private boolean done = false;
  
    private long weight;
    private final BytesRef scratch = new BytesRef();
    private BytesRef payload = new BytesRef();
    private Set<BytesRef> contexts = null;
    
    public FilteredContextInputIterator(File input, BytesRef context, boolean hasPayloads) throws IOException {
      this.context = context;
      this.reader = new OfflineSorter.ByteSequencesReader(input);
      this.hasPayloads = hasPayloads;
    }
    

    @Override
    public BytesRef next() throws IOException {
      boolean success = false;
      if (done) {
        return null;
      }
      try {
        ByteArrayDataInput input = new ByteArrayDataInput();
        while (reader.read(scratch)) {
          weight = decode(scratch, input);
          contexts = decodeContexts(scratch, input);
          if (hasPayloads) {
            payload = decodePayload(scratch, input);
          }
          
          if (contexts.contains(context)) {
            success = true;
            return scratch;
          }
        }
        success = done = true;
        return null;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(reader);
          done = true;
        } else if (done)
          IOUtils.close(reader);
      }
    }
    
    @Override
    public long weight() {
      return weight;
    }

    @Override
    public BytesRef payload() {
      if (hasPayloads) {
        return payload;
      }
      return null;
    }

    @Override
    public boolean hasPayloads() {
      return hasPayloads;
    }
    
    @Override
    public Set<BytesRef> contexts() {
      return contexts;
    }

    @Override
    public boolean hasContexts() {
      return true;
    }
    
    private long decode(BytesRef scratch, ByteArrayDataInput tmpInput) {
      tmpInput.reset(scratch.bytes);
      tmpInput.skipBytes(scratch.length - 8); // suggestion
      scratch.length -= 8; // long
      return tmpInput.readLong();
    }
    
    private Set<BytesRef> decodeContexts(BytesRef scratch, ByteArrayDataInput tmpInput) {
      tmpInput.reset(scratch.bytes);
      tmpInput.skipBytes(scratch.length - 2); //skip to context set size
      short ctxSetSize = tmpInput.readShort();

      System.out.println("Ctxs: " + ctxSetSize);
      scratch.length -= 2;
      final Set<BytesRef> contextSet = new HashSet<>();
      for (short i = 0; i < ctxSetSize; i++) {
        tmpInput.setPosition(scratch.length - 2);
        short curContextLength = tmpInput.readShort();
        scratch.length -= 2;
        tmpInput.setPosition(scratch.length - curContextLength);
        BytesRef contextSpare = new BytesRef(curContextLength);
        System.out.println("Ctx Length: " + curContextLength);
        tmpInput.readBytes(contextSpare.bytes, 0, curContextLength);
        contextSpare.length = curContextLength;
        contextSet.add(contextSpare);
        scratch.length -= curContextLength;
      }
      return contextSet;
    }

    private BytesRef decodePayload(BytesRef scratch, ByteArrayDataInput tmpInput) {
      tmpInput.reset(scratch.bytes);
      tmpInput.skipBytes(scratch.length - 2); // skip to payload size
      short payloadLength = tmpInput.readShort(); // read payload size
      tmpInput.setPosition(scratch.length - 2 - payloadLength); // setPosition to start of payload
      BytesRef payloadScratch = new BytesRef(payloadLength); 
      tmpInput.readBytes(payloadScratch.bytes, 0, payloadLength); // read payload
      payloadScratch.length = payloadLength;
      scratch.length -= 2; // payload length info (short)
      scratch.length -= payloadLength; // payload
      return payloadScratch;
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
      throw new IllegalArgumentException("this suggester requires contexts");
    }
    String prefix = getClass().getSimpleName();
    File directory = OfflineSorter.defaultTempDir();
    File tempInput = File.createTempFile(prefix, ".input", directory);
    OfflineSorter.ByteSequencesWriter writer = new OfflineSorter.ByteSequencesWriter(tempInput);
    final boolean hasPayloads = inputIterator.hasPayloads();
    Set<BytesRef> globalContexts = new HashSet<>();
    
    byte buffer[] = new byte[32];
    BytesRef entry;
    boolean success = false;
    try {
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
      while((entry = inputIterator.next()) != null) {
        //System.out.println("Input");
        //System.out.println(entry.utf8ToString());
        //System.out.println(inputIterator.weight());
        //System.out.println(inputIterator.payload().utf8ToString());
        //for (BytesRef ctx : inputIterator.contexts())
          //System.out.println(ctx.utf8ToString());
        count++;
        BytesRef payload = inputIterator.payload();
        Set<BytesRef> contexts = inputIterator.contexts();
        globalContexts.addAll(contexts);
        int requiredLength = entry.length + 8 + ((hasPayloads) ? 2 + payload.length : 0);
        for(BytesRef ctx : inputIterator.contexts()) {
          requiredLength += 2 + ctx.length;
        }
        requiredLength += 2; // for length of contexts
        
        buffer = ArrayUtil.grow(buffer, requiredLength);
        
        output.reset(buffer);
        // write entry
        output.writeBytes(entry.bytes, entry.offset, entry.length);
        // write payload
        if (hasPayloads) {
          output.writeBytes(payload.bytes, payload.offset, payload.length);
          output.writeShort((short) payload.length);
        }
        // write context
        if (contexts != null) {
          for (BytesRef ctx : contexts) {
            output.writeBytes(ctx.bytes, ctx.offset, ctx.length);
            output.writeShort((short) ctx.length);
          }
          output.writeShort((short) contexts.size());
        } else {
          output.writeShort((short) 0);
        }
        
        // weight
        output.writeLong(inputIterator.weight());
        writer.write(buffer, 0, output.getPosition());
      }
      writer.close();
      
      for(BytesRef context : globalContexts) {
        AnalyzingSuggester suggester = suggesterConfig.constructSuggester();
        suggesterMap.put(context.utf8ToString(), suggester);
        BytesRef f;
        InputIterator i = new FilteredContextInputIterator(tempInput, context, hasPayloads);
        while((f = i.next()) != null) {
          System.out.println(f.utf8ToString());
          System.out.println(i.weight());
          System.out.println(i.payload());
         // for (BytesRef ctx : i.contexts())
          //System.out.println(ctx.utf8ToString());
        }
        
        suggester.build(new FilteredContextInputIterator(tempInput, context, hasPayloads));
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
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts,
      boolean onlyMorePopular, int num) throws IOException {
    if (contexts == null) {
      // should we allow null context to match all contexts? not today
      throw new IllegalArgumentException("this suggester requires contexts");
    }
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
