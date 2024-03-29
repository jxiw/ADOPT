package util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ArrayUtil {

    public static int getLowerBound(int[] data) {
        int lb = data[0];
        for (int i = 1; i < data.length; i++) {
            if (data[i] < lb) {
                lb = data[i];
            }
        }
        return lb;
    }

    public static int getUpperBound(int[] data) {
        int ub = data[0];
        for (int i = 1; i < data.length; i++) {
            if (data[i] > ub) {
                ub = data[i];
            }
        }
        return ub;
    }

}
