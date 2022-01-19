package joining.join.wcoj;

import util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Hypercube {

    final private List<Pair<Integer, Integer>> intervals;

    final int dim;

    public Hypercube(List<Pair<Integer, Integer>> intervals) {
        this.intervals = intervals;
        this.dim = intervals.size();
    }

    public int getVolume() {
        int volume = 1;
        for (Pair<Integer, Integer> interval: intervals) {
            volume *= (interval.getSecond() - interval.getFirst());
        }
        return volume;
    }

    public List<Pair<Integer, Integer>> unfoldCube(int[] order) {
        return Arrays.stream(order).mapToObj(intervals::get).collect(Collectors.toList());
    }

    public void addDimension(int start, int end) {
        intervals.add(new Pair<Integer, Integer>(start, end));
    }

    public List<Hypercube> subtract(Hypercube subtractCube) {
        List<Hypercube> subCubes = new ArrayList<>();
        for (int i = 0; i < dim; i++) {
            // interval on dimension i
            Pair<Integer, Integer> intervalOnI = intervals.get(i);
            Pair<Integer, Integer> subIntervalOnI = subtractCube.intervals.get(i);
            int intervalOnIStart = intervalOnI.getFirst();
            int intervalOnIEnd = intervalOnI.getSecond();
            int subIntervalOnIStart = subIntervalOnI.getFirst();
            int subIntervalOnIEnd = subIntervalOnI.getSecond();
            if (intervalOnIStart < subIntervalOnIStart && intervalOnIEnd > subIntervalOnIEnd) {
                // e.g., 1, 7; 3, 6
                Pair<Integer, Integer> newIntervalOnI1 = new Pair<>(intervalOnIStart, subIntervalOnIStart - 1);
                Pair<Integer, Integer> newIntervalOnI2 = new Pair<>(subIntervalOnIEnd + 1, intervalOnIEnd);
                List<Pair<Integer, Integer>> subIntervals1 = new ArrayList<>(this.intervals);
                subIntervals1.set(i, newIntervalOnI1);
                Hypercube cube1 = new Hypercube(subIntervals1);
                subCubes.add(cube1);
                List<Pair<Integer, Integer>> subIntervals2 = new ArrayList<>(this.intervals);
                subIntervals2.set(i, newIntervalOnI2);
                Hypercube cube2 = new Hypercube(subIntervals2);
                subCubes.add(cube2);
            } else if (intervalOnIStart < subIntervalOnIStart && intervalOnIEnd == subIntervalOnIEnd) {
                // e.g., 1, 5; 3, 5
                Pair<Integer, Integer> newIntervalOnI = new Pair<>(intervalOnIStart, subIntervalOnIStart - 1);
                List<Pair<Integer, Integer>> subIntervals = new ArrayList<>(this.intervals);
                subIntervals.set(i, newIntervalOnI);
                Hypercube cube = new Hypercube(subIntervals);
                subCubes.add(cube);
            } else if (intervalOnIEnd > subIntervalOnIEnd && intervalOnIStart == subIntervalOnIStart) {
                // e.g., 1, 5; 1, 4
                Pair<Integer, Integer> newIntervalOnI = new Pair<>(subIntervalOnIEnd + 1, intervalOnIEnd);
                List<Pair<Integer, Integer>> subIntervals = new ArrayList<>(this.intervals);
                subIntervals.set(i, newIntervalOnI);
                Hypercube cube = new Hypercube(subIntervals);
                subCubes.add(cube);
            }
        }
        return subCubes;
    }

    @Override
    public String toString() {
        return "Hypercube{" +
                "intervals=" + intervals +
                ", dim=" + dim +
                '}';
    }

    public static void main(String[] args) {
        List<Pair<Integer, Integer>> intervals = new ArrayList<>();
        intervals.add(new Pair<>(1, 10));
        intervals.add(new Pair<>(3, 5));
        intervals.add(new Pair<>(2, 8));
        Hypercube hypercube = new Hypercube(intervals);
        List<Pair<Integer, Integer>> subIntervals = new ArrayList<>();
        subIntervals.add(new Pair<>(2, 6));
        subIntervals.add(new Pair<>(4, 5));
        subIntervals.add(new Pair<>(2, 4));
        Hypercube subCube = new Hypercube(subIntervals);
        //        Hypercube{intervals=[Pair{key=1, value=1}, Pair{key=3, value=5}, Pair{key=2, value=8}], dim=3},
        //        Hypercube{intervals=[Pair{key=7, value=10}, Pair{key=3, value=5}, Pair{key=2, value=8}], dim=3},
        //        Hypercube{intervals=[Pair{key=1, value=10}, Pair{key=3, value=3}, Pair{key=2, value=8}], dim=3},
        //        Hypercube{intervals=[Pair{key=1, value=10}, Pair{key=3, value=5}, Pair{key=5, value=8}], dim=3}
        System.out.println(hypercube.subtract(subCube));
    }
}
