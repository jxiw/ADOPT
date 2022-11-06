package joining.join.wcoj;

import joining.result.JoinResult;

import java.util.List;

public class HyperCubeEvaluationResult {

    /**
     *
     */
    public Hypercube selectCube;

    public boolean isFinish;

    public JoinResult joinResult;

    public List<Integer> endValues;

    public long remainBudget;

    public HyperCubeEvaluationResult(Hypercube selectCube, boolean isFinish, JoinResult joinResult, List<Integer> endValues, long remainBudget) {
        this.selectCube = selectCube;
        this.isFinish = isFinish;
        this.joinResult = joinResult;
        this.endValues = endValues;
        this.remainBudget = remainBudget;
    }
}
