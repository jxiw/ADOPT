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

    public static AtomicInteger nrCube;

    public static void init(List<Pair<Integer, Integer>> joinValues, int initNrCube) {
        hypercubes = new LinkedBlockingDeque<Hypercube>();
        int rangeInFirstDim = joinValues.get(0).getSecond() - joinValues.get(0).getFirst();
        int rangeInEachPartition = rangeInFirstDim / initNrCube;
        int firstDimStart = joinValues.get(0).getFirst();
        int firstDimEnd = joinValues.get(0).getSecond();
        for (int i = 0; i < initNrCube; i++) {
            int partitionStart = firstDimStart + i * rangeInEachPartition;
            int partitionEnd = firstDimStart + (i + 1) * rangeInEachPartition - 1;
            if (i == initNrCube - 1) {
                partitionEnd = firstDimEnd;
            }
            List<Pair<Integer, Integer>> subJoinValues = new ArrayList<>(joinValues);
            subJoinValues.set(0, new Pair<>(partitionStart, partitionEnd));
            Hypercube cube = new Hypercube(subJoinValues);
            hypercubes.add(cube);
        }

        nrCube = new AtomicInteger(initNrCube);
    }

    public static Hypercube allocateHypercube() throws InterruptedException {
        Hypercube cube = hypercubes.take();
        return cube;
    }


    public static void updateInterval(Hypercube parentCube, List<Integer> endValues, int[] order) {
        Hypercube cubeWithOrder = new Hypercube(parentCube.unfoldCube(order));
        List<Hypercube> remainHypercubes = cubeWithOrder.subtractByPoint(endValues);
        for (Hypercube remainHypercube : remainHypercubes) {
            remainHypercube.alignToUniversalOrder(order);
        }

        hypercubes.addAll(remainHypercubes);
        nrCube.addAndGet(remainHypercubes.size() - 1);
    }

    public static void finishHyperCube() {
        nrCube.decrementAndGet();
    }

    public static boolean isFinished() {
        return hypercubes.size() == 0;
    }
}
