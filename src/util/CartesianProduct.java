package util;

import java.util.*;
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

    public static void main(String[] args) {
//        Set<ResultTuple> b = new HashSet<>();
//        int[] a = new int[]{1, 2, 3};
//        b.add(new ResultTuple(a));
//        a[0] = 100;
//        b.add(new ResultTuple(a));
        List<List<Integer>> resultTuples = new ArrayList<>();
        List<Integer> resultTuple1 = new ArrayList<>();
        List<Integer> resultTuple2 = new ArrayList<>();
        List<Integer> resultTuple3 = new ArrayList<>();
        List<Integer> resultTuple4 = new ArrayList<>();
        resultTuple1.add(-1);
        resultTuple2.add(3);
        resultTuple2.add(5);
        resultTuple2.add(6);
        resultTuple3.add(10);
        resultTuple3.add(100);
        resultTuple4.add(-1);
        resultTuples.add(resultTuple1);
        resultTuples.add(resultTuple2);
        resultTuples.add(resultTuple3);
        resultTuples.add(resultTuple4);
        System.out.println(CartesianProduct.constructCombinations(resultTuples));
    }

}
