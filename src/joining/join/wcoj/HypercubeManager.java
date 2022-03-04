package joining.join.wcoj;

import config.JoinConfig;
import util.Pair;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class HypercubeManager {

    /**
     * hypercubes
     */
    public static BlockingDeque<Hypercube> hypercubes;

//    public static ConcurrentHashMap<Long, Boolean> isWorking;

    public static final CubeSelectionPolicy DEFAULT_CUBE_SELECTION =
            CubeSelectionPolicy.FIRST;

    public static AtomicInteger nrCube;

//    public static double totalVolume;

    public static void init(List<Pair<Integer, Integer>> joinValues, int initNrCube) {
        hypercubes = new LinkedBlockingDeque<Hypercube>();
        int rangeInFirstDim = joinValues.get(0).getSecond() - joinValues.get(0).getFirst();
        int rangeInEachPartition = rangeInFirstDim / initNrCube;
        int firstDimStart = joinValues.get(0).getFirst();
        int firstDimEnd = joinValues.get(0).getSecond();
//        System.out.println("cube:" + cube);
        for (int i = 0; i < initNrCube; i++) {
            int partitionStart = firstDimStart + i * rangeInEachPartition;
            int partitionEnd = firstDimStart + (i + 1) * rangeInEachPartition - 1;
            if (i == initNrCube - 1) {
                partitionEnd = firstDimEnd;
            }
            List<Pair<Integer, Integer>> subJoinValues = new ArrayList<>(joinValues);
            subJoinValues.set(0, new Pair<>(partitionStart, partitionEnd));
            Hypercube cube = new Hypercube(subJoinValues);
//            System.out.println("cube:" + cube);
            hypercubes.add(cube);
//            totalVolume += cube.getVolume();
        }

//        totalVolume = cube.getVolume();
//        isWorking = new ConcurrentHashMap<>();
        nrCube = new AtomicInteger(initNrCube);
//        System.out.println("totalVolume:" + totalVolume);
    }

//    public boolean checkOverlap() {
//        for (int i = 0; i < hypercubes.size() - 1; i++) {
//            for (int j = i + 1; j < hypercubes.size(); j++) {
//                if (hypercubes.get(i).overlap(hypercubes.get(j))) {
//                    System.out.println(hypercubes.get(i));
//                    System.out.println(hypercubes.get(j));
//                    return true;
//                }
//            }
//        }
//        return false;
//    }

//    public List<Hypercube> allocateNHypercube(int nrCubes) {
//        if (hypercubes.size() < nrCubes) {
//            return hypercubes;
//        } else {
//            switch (DEFAULT_CUBE_SELECTION) {
//                case VOLUMEPR:
//                    //
//                case RANDOM:
//                    Collections.shuffle(hypercubes);
//                    return hypercubes.stream().limit(nrCubes).collect(Collectors.toList());
//                default:
//                    return hypercubes.stream().limit(nrCubes).collect(Collectors.toList());
//            }
//        }
//    }

//    public List<List<Hypercube>> allocateNHypercubeEachThread(int nrPartition) {
//        if (hypercubes.size() == 0) {
//            return null;
//        }
//        return ArrayUtilities.batches(hypercubes, nrPartition);
//    }

//    public Hypercube allocateHypercube() {
//        if (hypercubes.size() == 0) {
//            // finish the execution
//            return null;
//        }
////        System.out.println("number of cubes:" + hypercubes.size());
//        // check the overlap of hypercubes
////        if (checkOverlap()) {
////            System.out.println("error");
////            System.exit(0);
////        }
//        Hypercube selectCube = hypercubes.get(0);
//        switch (DEFAULT_CUBE_SELECTION) {
//            case FIRST:
//                break;
//            case RANDOM:
//                selectCube =  hypercubes.get((int) (Math.random() * hypercubes.size()));
//                break;
//            case VOLUMEPR:
//                List<Double> volumes = hypercubes.stream().map(Hypercube::getVolume).collect(Collectors.toList());
//                double totalVolume = volumes.stream().mapToDouble(a -> a).sum();
//                List<Double> probs = volumes.stream().map(v -> v / totalVolume).collect(Collectors.toList());
//                double probGen = Math.random();
//                double cumulativeProb = 0;
//                for (int i = 0; i < probs.size(); i++) {
//                    double prob = probs.get(i);
//                    cumulativeProb += prob;
//                    if (cumulativeProb >= probGen) {
//                        // choose i-th hypercube
//                        selectCube = hypercubes.get(i);
//                        break;
//                    }
//                }
//        }
//        hypercubes.remove(selectCube);
//        return selectCube;
//    }


    public static Hypercube allocateHypercube() throws InterruptedException {
        Hypercube cube = hypercubes.take();
//        long threadId = Thread.currentThread().getId();
//        System.out.println("threadId 1:" + threadId);
//        isWorking.put(threadId, true);
        return cube;
    }


    public static void updateInterval(Hypercube parentCube, List<Integer> endValues, int[] order) {
//        long threadId = Thread.currentThread().getId();
        Hypercube cubeWithOrder = new Hypercube(parentCube.unfoldCube(order));
        List<Hypercube> remainHypercubes = cubeWithOrder.subtractByPoint(endValues);
//        System.out.println("cubeWithOrder:" + cubeWithOrder);
//        System.out.println("remainHypercubes:" + remainHypercubes);
//        double remainVolume = 0;
        for (Hypercube remainHypercube : remainHypercubes) {
//            remainVolume += remainHypercube.getVolume();
            // swap the order for remainHypercube
//            System.out.println("before swap:" + remainHypercube);
//            System.out.println("order:" + Arrays.toString(order));
            remainHypercube.alignToUniversalOrder(order);
//            System.out.println("after swap:" + remainHypercube);
//            System.out.println("cube volume:" + remainHypercube.getVolume());
        }
//        hypercubes.addAll(remainHypercubes);
//        System.out.println("parentCube volume:" + cubeWithOrder.getVolume());
//        System.out.println("remain Volume:" + remainVolume);
//        System.out.println("process Volume:" + (cubeWithOrder.getVolume() - remainVolume));

        hypercubes.addAll(remainHypercubes);
        nrCube.addAndGet(remainHypercubes.size() - 1);

//        System.out.println("threadId 22222:" + threadId);
//        isWorking.put(threadId, false);
//        System.out.println("threadId 2:" + threadId);
//        return cubeWithOrder.getVolume() - remainVolume;
    }

    public static void finishHyperCube() {
//        long threadId = Thread.currentThread().getId();
//        isWorking.put(threadId, false);
//        System.out.println("threadId 2----:" + threadId);
        nrCube.decrementAndGet();
    }

    public static boolean isFinished() {
        return hypercubes.size() == 0;
    }
}
