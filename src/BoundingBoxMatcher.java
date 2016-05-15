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
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

/**
 * Fast NxN polygon matching.
 * 
 * The input is a stream of items ordered by Xmin.
 * 
 * The output is a stream of Match objects, associating each value from the input stream to the list of previous values it intercepts (Ordered by Ymin)
 * 
 * The match object is only valid before the next item is fetched from the iterator! 
 * 
 * Performance-wise, it is quite fast! 
 * It about as fast as using a {@link STRtree}, but with the advantage that is works on data streams -- If that's what you are trying to do.
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
        return new AbstractIterator<Match>() {
            PeekingIterator<ObjectWithEnvelope> valuesIt = Iterators.peekingIterator(values.iterator());

            RangeMultiMap<ObjectWithEnvelope> activeObjectsByYRange = new RangeMultiMap<>();

            PriorityQueue<ObjectWithEnvelope> activeObjectsByXEnd = new PriorityQueue<>(new Comparator<ObjectWithEnvelope>() {
                public int compare(ObjectWithEnvelope o1, ObjectWithEnvelope o2) {
                    return Double.compare(o1.envelope.getMaxX(), o2.envelope.getMaxX());
                }
            });

            ObjectWithEnvelope addNext = null;

            int maxConcurrent = 0;

            @Override
            protected Match computeNext() {
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
                return new Match(addNext.value, 
                        Iterables.transform(activeObjectsByYRange.getValues(addNext.yRange), new Function<ObjectWithEnvelope, T>() {
                            @Override
                            public T apply(BoundingBoxMatcher<T>.ObjectWithEnvelope o) {
                                return o.value;
                            }
                        }));
            }
        };
    }

    public class Match {
        public final T value;
        public final Iterable<T> matches;
        public Match(T value, Iterable<T> matches) {
            this.value = value;
            this.matches = matches;
        }
    }

    private class ObjectWithEnvelope {
        private final T value;
        private final Envelope envelope;
        private final Envelope1D yRange;
        public ObjectWithEnvelope(T value, Envelope envelope) {
            this.value = value;
            this.envelope = envelope;
            this.yRange = new Envelope1D(envelope.getMinY(), envelope.getMaxY());
        }
    }


    public static void main(String[] args) {
        Stopwatch timer = Stopwatch.createStarted();
        System.out.println("Building envelopes: START");

        STRtree strTree = new STRtree();
        List<Envelope> envelopes = new ArrayList<>();
        for (int i=0; i<100000; i++) {
            int x1 = (int)(Math.random()*10000);
            int x2 = (int)(x1+Math.random()*100);
            int y1 = (int)(Math.random()*10000);
            int y2 = (int)(y1+Math.random()*100);
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
            for (Envelope e : m.matches) {
                count ++;
                if (count % 100000 == 0) {
                    System.out.print(".");
                }
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
