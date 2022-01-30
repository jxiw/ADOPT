package joining.join.wcoj;

import util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HypercubeManager {

    /**
     * hypercubes
     *
     */
    List<Hypercube> hypercubes;

    /**
     *
     * @param total volumne
     */
    double totalVolume = 0;

    public HypercubeManager(List<Pair<Integer, Integer>> joinValues) {
        Hypercube cube = new Hypercube(joinValues);
        hypercubes = new ArrayList<>();
        hypercubes.add(cube);
        totalVolume = cube.getVolume();
        System.out.println("totalVolume:" + totalVolume);
    }

    public boolean checkOverlap() {
        for (int i = 0; i < hypercubes.size() - 1; i++) {
            for (int j = i + 1; j < hypercubes.size(); j++) {
                if (hypercubes.get(i).overlap(hypercubes.get(j))) {
                    System.out.println(hypercubes.get(i));
                    System.out.println(hypercubes.get(j));
                    return true;
                }
            }
        }
        return false;
    }

    public Hypercube allocateHypercube() {
        if (hypercubes.size() == 0) {
            // finish the execution
            return null;
        }
        System.out.println("number of cubes:" + hypercubes.size());
        // check the overlap of hypercubes
        if (checkOverlap()) {
            System.out.println("error");
            System.exit(0);
        }
        // sample hypercube according to its volume
        // todo
        Hypercube selectCube = hypercubes.get(0);
        List<Double> volumes = hypercubes.stream().map(Hypercube::getVolume).collect(Collectors.toList());
        double totalVolume = volumes.stream().mapToDouble(a-> a).sum();
        List<Double> probs = volumes.stream().map(v -> v / totalVolume).collect(Collectors.toList());
        double probGen = Math.random();
        double cumulativeProb = 0 ;
        for (int i = 0; i < probs.size(); i++) {
            double prob = probs.get(i);
            cumulativeProb += prob;
            if (cumulativeProb < probGen) {
                // choose i-th hypercube
                selectCube = hypercubes.get(i);
                break;
            }
        }
        return selectCube;
    }

    public double updateInterval(Hypercube parentCube, List<Integer> endValues, int[] order) {
        hypercubes.remove(parentCube);
        Hypercube cubeWithOrder = new Hypercube(parentCube.unfoldCube(order));
        List<Hypercube> remainHypercubes = cubeWithOrder.subtractByPoint(endValues);
        double remainVolume = 0;
        for (Hypercube remainHypercube:remainHypercubes) {
            remainVolume += remainHypercube.getVolume();
            // swap the order for remainHypercube
            System.out.println("before swap:" + remainHypercube);
//            System.out.println("order:" + Arrays.toString(order));
            remainHypercube.alignToUniversalOrder(order);
            System.out.println("after swap:" + remainHypercube);
            System.out.println("cube volume:" + remainHypercube.getVolume());
        }
        hypercubes.addAll(remainHypercubes);
        System.out.println("parentCube volume:" + parentCube.getVolume());
        return parentCube.getVolume() - remainVolume;
    }

    public void finishHyperCube(Hypercube selectCube) {
        hypercubes.remove(selectCube);
    }
}
