package joining.join.wcoj;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

        List<Integer> l1 = Arrays.asList();
        int nrThreads = 32;
        int nrPerThread = (int) Math.ceil(l1.size() / (double) nrThreads);
        System.out.println("nrPerThread:"+ nrPerThread);
        System.out.println(ArrayUtilities.batches(l1, nrPerThread));
    }
}
