package joining.join.wcoj;

import util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HypercubeManager {

    /**
     * hypercubes
     *
     */
    List<Hypercube> hypercubes;

    public HypercubeManager(List<Pair<Integer, Integer>> joinValues) {
        Hypercube cube = new Hypercube(joinValues);
        hypercubes = new ArrayList<>();
        hypercubes.add(cube);
    }

    public Hypercube allocateHypercube() {
        // sample hypercube according to its volume
        Hypercube selectCube = hypercubes.get(0);
        List<Integer> volumes = hypercubes.stream().map(Hypercube::getVolume).collect(Collectors.toList());
        double totalVolume = volumes.stream().mapToDouble(Integer::doubleValue).sum();
        List<Double> probs = volumes.stream().map(v -> v / totalVolume).collect(Collectors.toList());
        double probGen = Math.random();
        double cumulativeProb = 0 ;
        for (int i = 0; i < probs.size(); i++) {
            double prob = probs.get(i);
            cumulativeProb += prob;
            if (cumulativeProb < probGen) {
                // choose i-th hypercube
                selectCube = hypercubes.get(i);
            }
        }
        return selectCube;
    }

    public void updateInterval(Hypercube parentCube, Hypercube subCube) {
        hypercubes.remove(parentCube);
        hypercubes.addAll(parentCube.subtract(subCube));
    }
}
