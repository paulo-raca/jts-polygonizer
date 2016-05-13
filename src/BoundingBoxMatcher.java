import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

/**
 * Fast NxN polygon matching.
 * 
 * The input is a stream of items ordered by Xmin.
 * 
 * The output is a stream of tuples with overlapping envelopes. The output order is not guaranteed.
 * 
 * Performance-wise, it is quite fast!
 */
public class BoundingBoxMatcher<T> implements Iterable<BoundingBoxMatcher<T>.Match> {
    Iterable<ObjectWithEnvelope> values;

    public BoundingBoxMatcher(Iterable<T> values, final Function<T, Envelope> extractEnvelope) {
        this.values = Iterables.transform(values, new Function<T, ObjectWithEnvelope>() {
            public BoundingBoxMatcher<T>.ObjectWithEnvelope apply(T value) {
                return new ObjectWithEnvelope(value, extractEnvelope.apply(value));
            }
        });
    }

    @Override
    public Iterator<Match> iterator() {
        return Iterators.concat(
                new AbstractIterator<Iterator<Match>>() {
                    PeekingIterator<ObjectWithEnvelope> valuesIt = Iterators.peekingIterator(values.iterator());

                    RangeMultiMap<Double, ObjectWithEnvelope> activeObjectsByYRange = RangeMultiMap.newDouble();

                    PriorityQueue<ObjectWithEnvelope> activeObjectsByXEnd = new PriorityQueue<>(new Comparator<ObjectWithEnvelope>() {
                        public int compare(ObjectWithEnvelope o1, ObjectWithEnvelope o2) {
                            return Double.compare(o1.envelope.getMaxX(), o2.envelope.getMaxX());
                        }
                    });

                    ObjectWithEnvelope addNext = null;
                    
                    int maxConcurrent = 0;

                    @Override
                    protected Iterator<Match> computeNext() {
                        if (addNext != null) {
                            //System.out.println("POP " + addNext);
                            activeObjectsByXEnd.add(addNext);
                            activeObjectsByYRange.put(addNext.yRange, addNext);
                            maxConcurrent = Math.max(maxConcurrent, activeObjectsByXEnd.size());
                            addNext = null; 
                        }

                        while (!activeObjectsByXEnd.isEmpty() && 
                                (!valuesIt.hasNext() || activeObjectsByXEnd.peek().envelope.getMaxX() < valuesIt.peek().envelope.getMinX())) {
                            ObjectWithEnvelope val = activeObjectsByXEnd.remove();
                            activeObjectsByYRange.remove(val.yRange, val);
                            //System.out.println("POP " + val);
                        }

                        if (!valuesIt.hasNext()) {
                            System.out.println("Peak size=" + maxConcurrent);
                            return endOfData();
                        }

                        addNext = valuesIt.next();
                        return Iterators.transform(
                                activeObjectsByYRange.getValues(addNext.yRange, RangeMultiMap.RangeSearchType.INTERSECTS).iterator(),
                                new Function<ObjectWithEnvelope, Match>() {
                                    public Match apply(BoundingBoxMatcher<T>.ObjectWithEnvelope v) {
                                        return new Match(addNext.value, v.value);
                                    };
                                });
                    }
                });
    }

    public class Match {
        public final T value1;
        public final T value2;
        public Match(T value1, T value2) {
            this.value1 = value1;
            this.value2 = value2;
        }
    }

    private class ObjectWithEnvelope {
        private final T value;
        private final Envelope envelope;
        private final Range<Double> yRange;
        public ObjectWithEnvelope(T value, Envelope envelope) {
            this.value = value;
            this.envelope = envelope;
            this.yRange = Range.closed(envelope.getMinY(), envelope.getMaxY());
        }
    }
    
    
    public static void main(String[] args) {
        Stopwatch timer = Stopwatch.createStarted();
        System.out.println("Building envelopes: START");
        
        STRtree strTree = new STRtree();
        List<Envelope> envelopes = new ArrayList<>();
        for (int i=0; i<10000000; i++) {
            int x1 = (int)(Math.random()*10000);
            int x2 = (int)(x1+Math.random()*10);
            int y1 = (int)(Math.random()*10000);
            int y2 = (int)(y1+Math.random()*10);
            Envelope env = new Envelope(x1,x2,y1,y2);
            envelopes.add(env);
            strTree.insert(env, env);
        }
        Collections.sort(envelopes, new Comparator<Envelope>() {
            @Override
            public int compare(Envelope o1, Envelope o2) {
                return Double.compare(o1.getMinX(), o2.getMinX());
            }
        });
        System.out.println("Building envelopes: COMPLETE - " + timer);
        
        
        
        timer = Stopwatch.createStarted();
        System.out.println("New Search: START");
        
        int count = 0;
        BoundingBoxMatcher<Envelope> matcher = new BoundingBoxMatcher<>(envelopes, new Function<Envelope, Envelope>() {
            public Envelope apply(Envelope a) {
                return a;
            }
        });
        for (BoundingBoxMatcher<Envelope>.Match m : matcher) {
            count ++;
            if (count % 100000 == 0) {
                System.out.print(".");
            }
        }
        System.out.println();
        System.out.println("New Search: COMPLETE: " + count + " - " + timer);
        
        

        Stopwatch timerStr = Stopwatch.createStarted();
        timer = Stopwatch.createStarted();
        System.out.println("Building STR-Tree: START");
        strTree.build();
        System.out.println("Building STR-Tree: COMPLETE - " + timer);

        timer = Stopwatch.createStarted();
        System.out.println("STR-Tree Search: START");
        count = 0;
        for (Envelope e : envelopes) {
            count += strTree.query(e).size() - 1;
        }
        System.out.println("STR-Tree Search: COMPLETE - " + count/2 + " - " + timer + " " + timerStr);
    }
}
