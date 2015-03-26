package org.apache.lucene.search.suggest.analyzing;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

// TODO: move to core?  nobody else uses it yet though...

/**
 * Exposes a utility method to enumerate all paths
 * intersecting an {@link Automaton} with an {@link FST}.
 */
public class FSTUtil {

  private FSTUtil() {
  }

  /** Holds a pair (automaton, fst) of states and accumulated output in the intersected machine. */
  public static final class Path<T> {

    /** Node in the automaton where path ends: */
    public final int state;

    /** Node in the FST where path ends: */
    public final FST.Arc<T> fstNode;

    /** Output of the path so far: */
    public T output;

    /** Input of the path so far: */
    public final IntsRefBuilder input;

    /** Sole constructor. */
    public Path(int state, FST.Arc<T> fstNode, T output, IntsRefBuilder input) {
      this.state = state;
      this.fstNode = fstNode;
      this.output = output;
      this.input = input;
    }
  }

  public static void main(String[] args) throws Exception {
    Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, PositiveIntOutputs.getSingleton());
    IntsRefBuilder scratch = new IntsRefBuilder();

    builder.add(Util.toIntsRef(new BytesRef("abc"), scratch), 1l);
    builder.add(Util.toIntsRef(new BytesRef("abd"), scratch), 2l);
    builder.add(Util.toIntsRef(new BytesRef("abf"), scratch), 3l);
    FST<Long> fst = builder.finish();

    FST.Arc<Long> arc = fst.getFirstArc(new FST.Arc<>());

    final FST.Arc<Long> scratchArc = new FST.Arc<>();
    FST.BytesReader reader = fst.getBytesReader();

    fst.findTargetArc(0, arc, scratchArc, reader);

    int x = 0;
  }

  private static class Paths {
    private final Path<Long> contextPath;
    private final Path<PairOutputs.Pair<Long, BytesRef>> fstPath;

    private Paths(Path<Long> contextPath, Path<PairOutputs.Pair<Long, BytesRef>> fstPath) {
      this.contextPath = contextPath;
      this.fstPath = fstPath;
    }
  }

  public static List<Path<PairOutputs.Pair<Long, BytesRef>>> intersect(FST<Long> context, FST<PairOutputs.Pair<Long, BytesRef>> fst) throws IOException {
    final List<Paths> queue = new ArrayList<>();
    final List<Path<PairOutputs.Pair<Long, BytesRef>>> endNodes = new ArrayList<>();

    Path<PairOutputs.Pair<Long, BytesRef>> fstPath = new Path<>(0, fst
        .getFirstArc(new FST.Arc<>()), fst.outputs.getNoOutput(),
        new IntsRefBuilder());

    Path<Long> contextPath = new Path<>(0, context.getFirstArc(new FST.Arc<>()),
        context.outputs.getNoOutput(), new IntsRefBuilder());

    queue.add(new Paths(contextPath, fstPath));

    final FST.Arc<Long> conextScratchArc = new FST.Arc<>();
    final FST.Arc<PairOutputs.Pair<Long, BytesRef>> scratchArc = new FST.Arc<>();
    final BytesRef emptyBytesRef = new BytesRef();
    final PairOutputs<Long, BytesRef> scratchOutputs = new PairOutputs<>(PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton());

    final FST.BytesReader fstReader = fst.getBytesReader();
    final FST.BytesReader contextReader = context.getBytesReader();

    while (queue.size() != 0) {
      final Paths paths = queue.remove(queue.size() - 1);
      if (paths.contextPath.fstNode.isFinal() && paths.fstPath.fstNode.isFinal()) {
        PairOutputs.Pair<Long, BytesRef> output = fst.outputs.add(paths.fstPath.output, paths.fstPath.fstNode.nextFinalOutput);
        PairOutputs.Pair<Long, BytesRef> contextOutput = scratchOutputs.newPair(paths.contextPath.output, emptyBytesRef);
        contextOutput = fst.outputs.add(contextOutput, scratchOutputs.newPair(paths.contextPath.fstNode.nextFinalOutput, emptyBytesRef));
        output = fst.outputs.add(output, contextOutput);
        endNodes.add(new Path<>(0, paths.fstPath.fstNode, output, paths.fstPath.input));
        continue;
      }
      IntsRefBuilder currentInput = paths.fstPath.input;

      context.readFirstRealTargetArc(paths.contextPath.fstNode.target, paths.contextPath.fstNode, contextReader);
      fst.readFirstRealTargetArc(paths.fstPath.fstNode.target, paths.fstPath.fstNode, fstReader);

      while (true) {

        if (paths.contextPath.fstNode.isFinal() || paths.fstPath.fstNode.isFinal()) {
          break;
        }
        paths.fstPath.fstNode.numArcs

        context.readNextArc(paths.contextPath.fstNode, contextReader);
        fst.readNextArc(paths.fstPath.fstNode, fstReader);
      }

      paths.contextPath.input.append(paths.contextPath.fstNode.label);
      paths.contextPath.output = context.outputs.add(paths.contextPath.output, paths.contextPath.fstNode.output);

      paths.fstPath.input.append(paths.fstPath.fstNode.label);
      paths.fstPath.output = fst.outputs.add(paths.fstPath.output, paths.fstPath.fstNode.output);

    }
    return endNodes;
  }

  /**
   * Enumerates all minimal prefix paths in the automaton that also intersect the FST,
   * accumulating the FST end node and output for each path.
   */
  public static <T> List<Path<T>> intersectPrefixPaths(Automaton a, FST<T> fst)
      throws IOException {
    assert a.isDeterministic();
    final List<Path<T>> queue = new ArrayList<>();
    final List<Path<T>> endNodes = new ArrayList<>();
    if (a.getNumStates() == 0) {
      return endNodes;
    }

    queue.add(new Path<>(0, fst
        .getFirstArc(new FST.Arc<T>()), fst.outputs.getNoOutput(),
        new IntsRefBuilder()));
    
    final FST.Arc<T> scratchArc = new FST.Arc<>();
    final FST.BytesReader fstReader = fst.getBytesReader();

    Transition t = new Transition();

    while (queue.size() != 0) {
      final Path<T> path = queue.remove(queue.size() - 1);
      if (a.isAccept(path.state)) {
        endNodes.add(path);
        // we can stop here if we accept this path,
        // we accept all further paths too
        continue;
      }
      
      IntsRefBuilder currentInput = path.input;
      int count = a.initTransition(path.state, t);
      for (int i=0;i<count;i++) {
        a.getNextTransition(t);
        final int min = t.min;
        final int max = t.max;
        if (min == max) {
          final FST.Arc<T> nextArc = fst.findTargetArc(t.min,
              path.fstNode, scratchArc, fstReader);
          if (nextArc != null) {
            final IntsRefBuilder newInput = new IntsRefBuilder();
            newInput.copyInts(currentInput.get());
            newInput.append(t.min);
            queue.add(new Path<>(t.dest, new FST.Arc<T>()
                .copyFrom(nextArc), fst.outputs
                .add(path.output, nextArc.output), newInput));
          }
        } else {
          // TODO: if this transition's TO state is accepting, and
          // it accepts the entire range possible in the FST (ie. 0 to 255),
          // we can simply use the prefix as the accepted state instead of
          // looking up all the ranges and terminate early
          // here.  This just shifts the work from one queue
          // (this one) to another (the completion search
          // done in AnalyzingSuggester).
          FST.Arc<T> nextArc = Util.readCeilArc(min, fst, path.fstNode,
              scratchArc, fstReader);
          while (nextArc != null && nextArc.label <= max) {
            assert nextArc.label <=  max;
            assert nextArc.label >= min : nextArc.label + " "
                + min;
            final IntsRefBuilder newInput = new IntsRefBuilder();
            newInput.copyInts(currentInput.get());
            newInput.append(nextArc.label);
            queue.add(new Path<>(t.dest, new FST.Arc<T>()
                .copyFrom(nextArc), fst.outputs
                .add(path.output, nextArc.output), newInput));
            final int label = nextArc.label; // used in assert
            nextArc = nextArc.isLast() ? null : fst.readNextRealArc(nextArc,
                fstReader);
            assert nextArc == null || label < nextArc.label : "last: " + label
                + " next: " + nextArc.label;
          }
        }
      }
    }
    return endNodes;
  }
  
}
