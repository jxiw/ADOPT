package util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CartesianProduct {

    public static <T> List<List<T>> constructCombinations(
            Collection<? extends Collection<T>> collections) {
        return constructCombinations(
                new ArrayList<Collection<T>>(collections),
                Collections.emptyList()).collect(Collectors.toList());
    }

    private static <T> Stream<List<T>> constructCombinations(
            List<? extends Collection<T>> collections, List<T> current) {
        return collections.isEmpty() ? Stream.of(current) :
                collections.get(0).stream().flatMap(e -> {
                    List<T> list = new ArrayList<T>(current);
                    list.add(e);
                    return constructCombinations(
                            collections.subList(1, collections.size()), list);
                });
    }

}
