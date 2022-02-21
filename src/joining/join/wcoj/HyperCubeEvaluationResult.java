package joining.join.wcoj;

import joining.result.JoinResult;

import java.util.List;

public class HyperCubeEvaluationResult {

    /**
     *
     */
    public List<Hypercube> finishedCubes;

    public Hypercube unfinishedCube;

    public JoinResult joinResult;

    public List<Integer> endValues;

    public HyperCubeEvaluationResult(List<Hypercube> finishedCubes, Hypercube unfinishedCube, JoinResult joinResult, List<Integer> endValues) {
        this.finishedCubes = finishedCubes;
        this.unfinishedCube = unfinishedCube;
        this.joinResult = joinResult;
        this.endValues = endValues;
//        System.out.println("finishedCubes:" + finishedCubes);
//        System.out.println("unfinishedCube:" + unfinishedCube);
//        System.out.println("endValues:" + endValues);
    }
}
