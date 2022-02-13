package joining.join.wcoj;

import util.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Hypercube {

    private List<Pair<Integer, Integer>> intervals;

    final int dim;

    public Hypercube(List<Pair<Integer, Integer>> intervals) {
        this.intervals = intervals;
        this.dim = intervals.size();
    }

    public double getVolume() {
        double volume = 1;
        for (Pair<Integer, Integer> interval: intervals) {
            volume *= (interval.getSecond() - interval.getFirst() + 1);
        }
//        System.out.println("volume:" + volume);
        return volume;
    }

    public List<Pair<Integer, Integer>> unfoldCube(int[] order) {
        return Arrays.stream(order).mapToObj(intervals::get).collect(Collectors.toList());
    }

    public void addDimension(int start, int end) {
        intervals.add(new Pair<Integer, Integer>(start, end));
    }

    public void alignToUniversalOrder(int order[]) {
        // [1, 0, 2], [[1, 3], [4, 5], [2, 6]] -> [[4, 5], [1, 3], [2, 6]]
        Pair<Integer, Integer>[] reorderInterval = new Pair[dim];
        for (int i = 0; i < dim; i++) {
            reorderInterval[order[i]] = intervals.get(i);
        }
        intervals = Arrays.asList(reorderInterval);
//        intervals.sort(Comparator.comparing(s -> order[intervals.indexOf(s)]));
    }

    public boolean overlap(Hypercube cube) {
        for (int i = 0; i < dim; i++) {
            Pair<Integer, Integer> interval1 = intervals.get(i);
            Pair<Integer, Integer> interval2 = cube.intervals.get(i);
            // test whether interval1 overlap interval2
            if (!(interval1.getFirst() <= interval2.getSecond() && interval2.getFirst() <= interval1.getSecond())){
                return false;
            }
        }
        return true;
    }

    public List<Hypercube> subtract(Hypercube subtractCube) {
        System.out.println("currentCube:" + this);
        System.out.println("subtractCube:" + subtractCube);
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

    public List<Hypercube> subtractByPoint(List<Integer> points) {
        List<Hypercube> subCubes = new ArrayList<>();
        for (int i = 0; i < dim; i++) {
            // point in i-th dimension
            int vb = points.get(i);
            int ub = this.intervals.get(i).getSecond();
            if (vb < ub) {
                List<Pair<Integer, Integer>> subIntervals = new ArrayList<>(this.intervals);
                for (int j = 0; j < i; j++) {
                    subIntervals.set(j, new Pair<Integer, Integer>(points.get(j), points.get(j)));
                }
                subIntervals.set(i, new Pair<Integer, Integer>(vb + 1, ub));
                subCubes.add(new Hypercube(subIntervals));
            }
        }
        return subCubes;
    }

    @Override
    public String toString() {
        return String.format("hypercube:%s", intervals.toString());
    }

    public static void main(String[] args) {
        List<Pair<Integer, Integer>> intervals = new ArrayList<>();
        intervals.add(new Pair<>(1, 10));
        intervals.add(new Pair<>(3, 7));
        intervals.add(new Pair<>(2, 8));
        Hypercube hypercube = new Hypercube(intervals);
        //        List<Pair<Integer, Integer>> subIntervals = new ArrayList<>();
        //        subIntervals.add(new Pair<>(2, 6));
        //        subIntervals.add(new Pair<>(4, 5));
        //        subIntervals.add(new Pair<>(2, 4));
        //        Hypercube subCube = new Hypercube(subIntervals);
        //        Hypercube{intervals=[Pair{key=1, value=1}, Pair{key=3, value=5}, Pair{key=2, value=8}], dim=3},
        //        Hypercube{intervals=[Pair{key=7, value=10}, Pair{key=3, value=5}, Pair{key=2, value=8}], dim=3},
        //        Hypercube{intervals=[Pair{key=1, value=10}, Pair{key=3, value=3}, Pair{key=2, value=8}], dim=3},
        //        Hypercube{intervals=[Pair{key=1, value=10}, Pair{key=3, value=5}, Pair{key=5, value=8}], dim=3}

        List<Integer> points1 = new ArrayList<>();
        points1.add(5);
        points1.add(4);
        points1.add(6);
        List<Integer> points2 = new ArrayList<>();
        points2.add(3);
        points2.add(6);
        points2.add(2);
        List<Integer> points3 = new ArrayList<>();
        points3.add(10);
        points3.add(4);
        points3.add(5);
        List<Integer> points4 = new ArrayList<>();
        points4.add(9);
        points4.add(7);
        points4.add(5);
        List<Integer> points5 = new ArrayList<>();
        points5.add(10);
        points5.add(7);
        points5.add(8);
        List<Integer> points6 = new ArrayList<>();
        // if reach to [5, 4]
        points6.add(5);
        points6.add(3);
        points6.add(8);
        List<Integer> points7 = new ArrayList<>();
        // if reach to [5]
        points7.add(4);
        points7.add(7);
        points7.add(8);
        System.out.println(hypercube.subtractByPoint(points1));
        System.out.println(hypercube.subtractByPoint(points2));
        System.out.println(hypercube.subtractByPoint(points3));
        System.out.println(hypercube.subtractByPoint(points4));
        System.out.println(hypercube.subtractByPoint(points5));
        System.out.println(hypercube.subtractByPoint(points6));
        System.out.println(hypercube.subtractByPoint(points7));
        int[] order = {1, 2, 0};
        hypercube.alignToUniversalOrder(order);
        System.out.println(hypercube);

        List<Pair<Integer, Integer>> intervals2 = new ArrayList<>();
        intervals2.add(new Pair<>(3002, 4000));
        intervals2.add(new Pair<>(2001, 3000));
        intervals2.add(new Pair<>(1001, 2000));
        intervals2.add(new Pair<>(2, 1000));
        Hypercube hypercube2 = new Hypercube(intervals2);
        int[] order2 = {0, 3, 1, 2};
        hypercube2.alignToUniversalOrder(order2);
        System.out.println(hypercube2);
//        after swap:hypercube:[[3002,4000], [1001,2000], [2,1000], [2001,3000]]

    }
}
