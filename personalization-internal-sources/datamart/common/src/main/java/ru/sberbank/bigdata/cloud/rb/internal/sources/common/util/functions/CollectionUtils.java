package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions;

import java.util.*;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

public class CollectionUtils {

    public static <T> Set<T> setOf(T... elements) {
        return new HashSet<>(Arrays.asList(elements));
    }

    /**
     * Collections.singletonMap(...) will return immutable map, so this method is needed
     *
     * @return mutable instance of map
     */
    public static <K, V> Map<K, V> mutableMapOf(K key, V value) {
        HashMap<K, V> kvHashMap = new HashMap<>();
        kvHashMap.put(key, value);
        return kvHashMap;
    }

    public static <T> T extractExactlyOneElement(Collection<T> collection, Predicate<T> predicate) {
        return extractExactlyOneOptional(collection, predicate)
                .orElseThrow(() -> new IllegalArgumentException("Collection  doesn't contain exactly 1 element that satisfied by predicate"));
    }

    public static <T> Optional<T> extractExactlyOneOptional(Collection<T> collection, Predicate<T> predicate) {
        final List<T> filtered = collection.stream().filter(predicate).collect(toList());
        if (filtered.size() == 1) {
            return Optional.of(filtered.get(0));
        } else {
            return Optional.empty();
        }
    }

    public static <T> T extractExactlyOneElement(Collection<T> collection) {
        if (collection.size() != 1) {
            throw new IllegalArgumentException("Expected 1 element, but found " + collection.size());
        }
        return collection.iterator().next();
    }

    public static <K, V> Map<K, V> emptyMapIfNull(Map<K, V> nullableMap) {
        return nullableMap == null ? Collections.emptyMap() : nullableMap;
    }

    public static <E> List<E> emptyListIfNull(List<E> nullableList) {
        return nullableList == null ? Collections.emptyList() : nullableList;
    }

    @SafeVarargs
    public static <T> List<T> asMutableList(T... ts) {
        return new ArrayList<>(Arrays.asList(ts));
    }

    public static <T> List<T> transformToMutableList(List<T> immutableList) {
        return new ArrayList<>(immutableList);
    }

    public static <T> List<T> subtract(List<? extends T> a, List<? extends T> b) {
        final List<T> aListCopy = new ArrayList<>(a);
        aListCopy.removeAll(b);
        return aListCopy;
    }

    public static <T> Object[] joinValues(T[]... values) {
        List<T> list = new ArrayList<>();
        for (T[] value : values) {
            if (value != null) {
                Collections.addAll(list, value);
            }
        }
        return list.toArray(new Object[list.size()]);
    }

    public static <T> List<T> union(List<T> a, List<T> b) {
        List<T> copyA = a;
        List<T> copyB = b;
        if (a == null) {
            copyA = Collections.emptyList();
        }
        if (b == null) {
            copyB = Collections.emptyList();
        }
        ArrayList<T> objects = new ArrayList<>();
        objects.addAll(copyA);
        objects.addAll(copyB);
        return objects;
    }

    public static <T> void addIfNotNull(List<T> listToAdd, T element) {
        if (element != null) {
            listToAdd.add(element);
        }
    }
}
