package joining.join.wcoj;

import util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ArrayUtilities {

    public static List<int[]> permutations(int num) {
        List<int[]> results = new ArrayList<int[]>();
        Set<Integer> setToPermutate = IntStream.range(0, num).boxed().collect(Collectors.toSet());
        permutations(setToPermutate, new Stack<Integer>(), num, results);
        return results;
    }

    public static void permutations(Set<Integer> items, Stack<Integer> permutation, int size, List<int[]> results) {

        /* permutation stack has become equal to size that we require */
        if(permutation.size() == size) {
            /* print the permutation */
            results.add(permutation.stream().mapToInt(i->i).toArray());
        }

        /* items available for permutation */
        Integer[] availableItems = items.toArray(new Integer[0]);
        for(Integer i : availableItems) {
            /* add current item */
            permutation.push(i);

            /* remove item from available item set */
            items.remove(i);

            /* pass it on for next permutation */
            permutations(items, permutation, size, results);

            /* pop and put the removed item back */
            items.add(permutation.pop());
        }
    }

    public static <T> List<List<T>> batches(List<T> source, int nrPartition) {
        int size = source.size();
        int length = (int) Math.ceil(source.size() / (double) nrPartition);
        int fullChunks = (size - 1) / length;
        return IntStream.range(0, fullChunks + 1).mapToObj(
                n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length)).collect(Collectors.toList());
    }


    public static void main(String[] args) {
//        List<int[]> combinations = permutations(3);
//        System.out.println(combinations.size());
//        for (int[] combination : combinations) {
//            System.out.println(Arrays.toString(combination));
//        }

//        List<Integer> test = new ArrayList<>();
//        test.add(34);
//        test.add(56);
//        System.out.println(test.hashCode());
//        test = new ArrayList<>();
//        test.add(56);
//        test.add(34);
//        System.out.println(test.hashCode());

        List<Pair<Integer, Integer>> intervals1 = new ArrayList<>();
        intervals1.add(new Pair<>(1, 10));
        intervals1.add(new Pair<>(3, 7));
        intervals1.add(new Pair<>(2, 8));
        Hypercube hypercube1 = new Hypercube(intervals1);

        List<Pair<Integer, Integer>> intervals2 = new ArrayList<>();
        intervals2.add(new Pair<>(23, 10));
        intervals2.add(new Pair<>(23, 7));
        intervals2.add(new Pair<>(22, 8));
        Hypercube hypercube2 = new Hypercube(intervals2);

        List<Pair<Integer, Integer>> intervals3 = new ArrayList<>();
        intervals3.add(new Pair<>(33, 10));
        intervals3.add(new Pair<>(33, 7));
        intervals3.add(new Pair<>(32, 8));
        Hypercube hypercube3 = new Hypercube(intervals3);

        List<Pair<Integer, Integer>> intervals4 = new ArrayList<>();
        intervals4.add(new Pair<>(41, 10));
        intervals4.add(new Pair<>(43, 7));
        intervals4.add(new Pair<>(42, 8));
        Hypercube hypercube4 = new Hypercube(intervals4);

        List<Pair<Integer, Integer>> intervals5 = new ArrayList<>();
        intervals5.add(new Pair<>(53, 10));
        intervals5.add(new Pair<>(53, 7));
        intervals5.add(new Pair<>(52, 8));
        Hypercube hypercube5 = new Hypercube(intervals5);

        List<Pair<Integer, Integer>> intervals6 = new ArrayList<>();
        intervals6.add(new Pair<>(33, 10));
        intervals6.add(new Pair<>(33, 7));
        intervals6.add(new Pair<>(22, 8));
        Hypercube hypercube6 = new Hypercube(intervals6);

        List<Hypercube> l1 = new ArrayList<>();
        l1.add(hypercube1);
        l1.add(hypercube2);
        l1.add(hypercube3);
        l1.add(hypercube4);
        l1.add(hypercube5);
        l1.add(hypercube6);

        System.out.println("len:" + l1.size());
        int nrThreads = 32;
        int nrPerThread = (int) Math.ceil(l1.size() / (double) nrThreads);
        System.out.println("nrPerThread:"+ nrPerThread);
        System.out.println(ArrayUtilities.batches(l1, nrPerThread));
    }
}
