package joining.join.wcoj;

import util.Pair;

import java.util.ArrayList;
import java.util.List;

public class HypercubeManager {

    List<Hypercube> hypercubes;

    public HypercubeManager(List<Pair<Integer, Integer>> joinValues) {
        Hypercube cube = new Hypercube(joinValues);
        hypercubes = new ArrayList<>();
        hypercubes.add(cube);
    }



}
