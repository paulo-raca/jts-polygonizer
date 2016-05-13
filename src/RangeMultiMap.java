import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BoundType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;

public class RangeMultiMap<K extends Comparable<K>, V> {
    Comparator<Range<K>> rangeComparator = new RangeComparator<>();
    DomainOperation<K, K> domainOperations;
    Map<K, SortedMap<Range<K>, Collection<V>>> scaleMapss = new HashMap<>();
    
    public static <T> RangeMultiMap<Double, T> newDouble() {
        return new RangeMultiMap<>(DomainOperation.DOUBLE);
    }
    public static <T> RangeMultiMap<Long, T> newLong() {
        return new RangeMultiMap<>(DomainOperation.LONG);
    }
    
    public RangeMultiMap(DomainOperation<K,K> domainOperations) {
        this.domainOperations = domainOperations;
    }
    
    public boolean put(Range<K> range, V value) {
        K scaleLength = domainOperations.discreteLen(range);
        SortedMap<Range<K>, Collection<V>> scaleData = scaleMapss.get(scaleLength);
        if (scaleData == null) {
            scaleData = new TreeMap<>(rangeComparator);
            scaleMapss.put(scaleLength, scaleData);
        }
        Collection<V> values = scaleData.get(range);
        if (values == null) {
            values = new ArrayList<>();
            scaleData.put(range, values);
        }
        return values.add(value);
    }


    public boolean remove(Range<K> range, V value) {
        K scaleLength = domainOperations.discreteLen(range);
        SortedMap<Range<K>, Collection<V>> scaleData = scaleMapss.get(scaleLength);
        if (scaleData == null) {
            return false;
        }
        Collection<V> values = scaleData.get(range);
        if (values == null) {
            return false;
        }
        if (!values.remove(value)) {
            return false;
        }
        if (values.isEmpty()) {
            scaleData.remove(range);
            if (scaleData.isEmpty()) {
                scaleMapss.remove(scaleLength);
            }
        }
        return true;
    }

    public Iterable<Range<K>> getKeys(Range<K> query, RangeSearchType searchType) {
        return Iterables.transform(getEntries(query, searchType), new Function<Map.Entry<Range<K>, Collection<V>>, Range<K>>() {
            public Range<K> apply(Map.Entry<Range<K>, Collection<V>> entry) {
                return entry.getKey();
            };            
        });
    }

    public Iterable<V> getValues(Range<K> query, RangeSearchType searchType) {
        return Iterables.concat(Iterables.transform(getEntries(query, searchType), new Function<Map.Entry<Range<K>, Collection<V>>, Collection<V>>() {
            public Collection<V> apply(Map.Entry<Range<K>, Collection<V>> entry) {
                return entry.getValue();
            };
        }));
    }

    public Iterable<Map.Entry<Range<K>, Collection<V>>> getEntries(final Range<K> query, final RangeSearchType rangeSearchType) {
        return new Iterable<Map.Entry<Range<K>, Collection<V>>>() {
            @Override
            public Iterator<Entry<Range<K>, Collection<V>>> iterator() {
                Collection<Iterator<Map.Entry<Range<K>, Collection<V>>>> scaleIterators = new ArrayList<>();
                for (Map.Entry<K, SortedMap<Range<K>, Collection<V>>> entry : scaleMapss.entrySet()) {
                    K scale = entry.getKey();
                    SortedMap<Range<K>, Collection<V>> scaleValues = entry.getValue();

                    if (query.hasLowerBound() && scale != null) {
                        //TODO: Slicing for INTERSECTS, could be more optimized for other operations
                        K minValue = domainOperations.sub(query.lowerEndpoint(), scale);
                        scaleValues = scaleValues.tailMap( Range.closedOpen(minValue, minValue ) );
                    }

                    final Iterator<Entry<Range<K>, Collection<V>>> preFilterIterator = scaleValues.entrySet().iterator();

                    scaleIterators.add(new AbstractIterator<Entry<Range<K>, Collection<V>>>() {
                        @Override
                        protected Entry<Range<K>, Collection<V>> computeNext() {
                            while (true) {
                                if (!preFilterIterator.hasNext()) {
                                    return endOfData();
                                }

                                Entry<Range<K>, Collection<V>> entry = preFilterIterator.next();

                                //TODO: Slicing for INTERSECTS, could be more optimized for other operations
                                if (query.hasUpperBound() && entry.getKey().hasLowerBound() && entry.getKey().lowerEndpoint().compareTo(query.upperEndpoint()) > 0) {
                                    return endOfData();
                                }

                                if (rangeSearchType.predicate(query, entry.getKey())) {
                                    return entry;
                                }
                            }
                        }
                    });
                }
                return Iterators.mergeSorted(scaleIterators, new Comparator<Map.Entry<Range<K>, Collection<V>>>() {
                    @Override
                    public int compare(Entry<Range<K>, Collection<V>> o1, Entry<Range<K>, Collection<V>> o2) {
                        return rangeComparator.compare(o1.getKey(), o2.getKey());
                    }
                });
            }
        };
    }

    public static enum RangeSearchType {
        INTERSECTS {
            @Override
            public <V extends Comparable<V>> boolean predicate(Range<V> query, Range<V> value) {
                return query.isConnected(value) && !query.intersection(value).isEmpty();
            }
        },
        ENCLOSES {
            @Override
            public <V extends Comparable<V>> boolean predicate(Range<V> query, Range<V> value) {
                return query.encloses(value);
            }
        }, 
        ENCLOSED {
            @Override
            public <V extends Comparable<V>> boolean predicate(Range<V> query, Range<V> value) {
                return value.encloses(query);
            }
        };

        public abstract <V extends Comparable<V>> boolean predicate(Range<V> query, Range<V> value);
    }


    public static interface DomainOperation<V extends Comparable<V>, L> {
        public L len(Range<V> range);
        public L discreteLen(Range<V> range);
        public V sub(V a, L b);

        public static final DomainOperation<Double, Double> DOUBLE = new DomainOperation<Double, Double> () {
            public Double len(Range<Double> range) {
                if (range.hasLowerBound() && range.hasUpperBound()) {
                    return range.upperEndpoint() - range.lowerEndpoint();
                } else {
                    return null;
                }
            }
            public Double discreteLen(Range<Double> range) {
                Double len = len(range);
                if (len == null || len == 0) {
                    return len;
                }
                return Math.exp(Math.ceil(Math.log(len)));
            }
            public Double sub(Double a, Double b) {
                return a-b;
            }
        };


        public static final DomainOperation<Long, Long> LONG = new DomainOperation<Long, Long> () {
            public Long len(Range<Long> range) {
                if (range.hasLowerBound() && range.hasUpperBound()) {
                    return range.upperEndpoint() - range.lowerEndpoint();
                } else {
                    return null;
                }
            }
            public Long discreteLen(Range<Long> range) {
                Long len = len(range);
                if (len == null || len == 0) {
                    return len;
                }
                long ret = 0;
                while (ret < len) {
                    ret = ret*2 + 1;
                }
                return ret;
            }
            public Long sub(Long a, Long b) {
                return a-b;
            }
        };
    }


    public static class RangeComparator<K extends Comparable<K>> implements Comparator<Range<K>> {
        @Override
        public int compare(Range<K> o1, Range<K> o2) {
            //Ranges without lower bound
            if (o1.hasLowerBound() != o2.hasLowerBound()) {
                return !o1.hasLowerBound() ? -1 : 1;
            }
            //Ranges with lowest lower bound
            if (o1.hasLowerBound() && o2.hasLowerBound()) {
                int ret = o1.lowerEndpoint().compareTo(o2.lowerEndpoint());
                if (ret != 0) return ret;
                if (o1.lowerBoundType() != o2.lowerBoundType()) {
                    return o1.lowerBoundType() == BoundType.CLOSED ? -1 : 1;
                }
            }


            //Ranges with upper bound
            if (o1.hasUpperBound() != o2.hasUpperBound()) {
                return o1.hasUpperBound() ? -1 : 1;
            }
            //Ranges with lowest upper bound
            if (o1.hasUpperBound() && o2.hasUpperBound()) {
                int ret = o1.upperEndpoint().compareTo(o2.upperEndpoint());
                if (ret != 0) return ret;
                if (o1.lowerBoundType() != o2.lowerBoundType()) {
                    return o1.lowerBoundType() == BoundType.OPEN? -1 : 1;
                }
            }


            return 0;
        }
    }
    
    public static void main(String[] args) {
        RangeMultiMap<Long, Object> rmm = new RangeMultiMap<>(DomainOperation.LONG);
        rmm.put(Range.all(), "ALL");
        rmm.put(Range.atLeast(10L), ">= 10");
        rmm.put(Range.atLeast(20L), ">= 20");
        rmm.put(Range.atLeast(30L), ">= 30");
        rmm.put(Range.atMost(10L), "<= 10");
        rmm.put(Range.atMost(20L), "<= 20");
        rmm.put(Range.atMost(30L), "<= 30");
        rmm.put(Range.closed(10L, 30L), "[10..30]");
        rmm.put(Range.closed(15L, 25L), "[15..25]");
        rmm.put(Range.closed(5L, 10L), "[5..10]");
        rmm.put(Range.closed(10L, 15L), "[10..15]");
        rmm.put(Range.singleton(5L), "[5]");
        rmm.put(Range.singleton(7L), "[7]");
        
        System.out.println(Iterables.toString(rmm.getValues(Range.greaterThan(20L), RangeSearchType.INTERSECTS)));
        System.out.println(Iterables.toString(rmm.getValues(Range.singleton(7L), RangeSearchType.ENCLOSED)));
        System.out.println(Iterables.toString(rmm.getValues(Range.singleton(7L), RangeSearchType.ENCLOSES)));
        System.out.println(Iterables.toString(rmm.getValues(Range.singleton(7L), RangeSearchType.INTERSECTS)));
        
        System.out.println(Iterables.toString(rmm.getValues(Range.closed(7L, 27L), RangeSearchType.INTERSECTS)));
        
    }
}
