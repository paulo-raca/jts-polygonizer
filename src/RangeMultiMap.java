import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

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
        Collection<Iterable<Map.Entry<Envelope1D, Collection<V>>>> scaleIterators = new ArrayList<>();
        for (Map.Entry<Double, SortedMap<Envelope1D, Collection<V>>> entry : scaleMapss.entrySet()) {
            Double scale = entry.getKey();
            SortedMap<Envelope1D, Collection<V>> scaleValues = entry.getValue();

            scaleIterators.add(Iterables.filter(
                    scaleValues.subMap(
                            new Envelope1D(query.min - scale, Double.NEGATIVE_INFINITY),
                            new Envelope1D(query.max, Double.POSITIVE_INFINITY)).entrySet(),
                    new Predicate<Map.Entry<Envelope1D, Collection<V>>>() {
                        @Override
                        public boolean apply(Entry<Envelope1D, Collection<V>> entry) {
                            return query.intersects(entry.getKey())
                                    ;
                        }
                    }));
        }
        
        return Iterables.mergeSorted(scaleIterators, new Comparator<Map.Entry<Envelope1D, Collection<V>>>() {
            @Override
            public int compare(Entry<Envelope1D, Collection<V>> o1, Entry<Envelope1D, Collection<V>> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
    }
}

