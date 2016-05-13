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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

public class RangeMultiMap<V> {
    Map<Double, SortedMap<Envelope1D, Collection<V>>> scaleMapss = new HashMap<>();

    private double discreteLen(double len) {
        return len == 0 ? 0 : Math.exp(Math.ceil(Math.log(len)));
    }
    
    public boolean put(Envelope1D range, V value) {
        double scaleLength = discreteLen(range.len());
        SortedMap<Envelope1D, Collection<V>> scaleData = scaleMapss.get(scaleLength);
        if (scaleData == null) {
            scaleData = new TreeMap<>();
            scaleMapss.put(scaleLength, scaleData);
        }
        Collection<V> values = scaleData.get(range);
        if (values == null) {
            values = new ArrayList<>();
            scaleData.put(range, values);
        }
        return values.add(value);
    }


    public boolean remove(Envelope1D range, V value) {
        double scaleLength = discreteLen(range.len());
        SortedMap<Envelope1D, Collection<V>> scaleData = scaleMapss.get(scaleLength);
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

    public Iterable<Envelope1D> getKeys(Envelope1D query) {
        return Iterables.transform(getEntries(query), new Function<Map.Entry<Envelope1D, Collection<V>>, Envelope1D>() {
            public Envelope1D apply(Map.Entry<Envelope1D, Collection<V>> entry) {
                return entry.getKey();
            };            
        });
    }

    public Iterable<V> getValues(Envelope1D query) {
        return Iterables.concat(Iterables.transform(getEntries(query), new Function<Map.Entry<Envelope1D, Collection<V>>, Collection<V>>() {
            public Collection<V> apply(Map.Entry<Envelope1D, Collection<V>> entry) {
                return entry.getValue();
            };
        }));
    }

    public Iterable<Map.Entry<Envelope1D, Collection<V>>> getEntries(final Envelope1D query) {
        return new Iterable<Map.Entry<Envelope1D, Collection<V>>>() {
            @Override
            public Iterator<Entry<Envelope1D, Collection<V>>> iterator() {
                Collection<Iterator<Map.Entry<Envelope1D, Collection<V>>>> scaleIterators = new ArrayList<>();
                for (Map.Entry<Double, SortedMap<Envelope1D, Collection<V>>> entry : scaleMapss.entrySet()) {
                    Double scale = entry.getKey();
                    SortedMap<Envelope1D, Collection<V>> scaleValues = entry.getValue();

                    scaleValues = scaleValues.tailMap( new Envelope1D(query.min - scale, Double.NEGATIVE_INFINITY) );

                    final Iterator<Entry<Envelope1D, Collection<V>>> preFilterIterator = scaleValues.entrySet().iterator();

                    scaleIterators.add(new AbstractIterator<Entry<Envelope1D, Collection<V>>>() {
                        @Override
                        protected Entry<Envelope1D, Collection<V>> computeNext() {
                            while (true) {
                                if (!preFilterIterator.hasNext()) {
                                    return endOfData();
                                }

                                Entry<Envelope1D, Collection<V>> entry = preFilterIterator.next();

                                if (entry.getKey().min > query.max) {
                                    return endOfData();
                                }
                                
                                if (query.intersects(entry.getKey())) {
                                    return entry;
                                }
                            }
                        }
                    });
                }
                return Iterators.mergeSorted(scaleIterators, new Comparator<Map.Entry<Envelope1D, Collection<V>>>() {
                    @Override
                    public int compare(Entry<Envelope1D, Collection<V>> o1, Entry<Envelope1D, Collection<V>> o2) {
                        return o1.getKey().compareTo(o2.getKey());
                    }
                });
            }
        };
    }
}
