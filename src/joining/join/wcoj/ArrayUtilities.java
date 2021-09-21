package joining.join.wcoj;

import java.util.*;
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

    public static void main(String[] args) {
        List<int[]> combinations = permutations(10);
        System.out.println(combinations.size());
//        for (int[] combination : combinations) {
//            System.out.println(Arrays.toString(combination));
//        }
    }
}
