package joining;

import config.JoinConfig;
import joining.join.wcoj.HyperCubeEvaluationTask;
import joining.join.wcoj.StateLFTJ;
import joining.join.wcoj.StaticLFTJ;
import joining.plan.AttributeOrder;
import joining.result.JoinResult;

import java.util.HashMap;

public class ParallelLFTJ {

    public final HashMap<AttributeOrder, HyperCubeEvaluationTask> orderToLFTJ;

    private final JoinResult joinResult;

    public ParallelLFTJ(JoinResult result) {
        this.orderToLFTJ = new HashMap<>();
        this.joinResult = result;
    }

    public double execute(int[] order) {
        AttributeOrder attributeOrder = new AttributeOrder(order);
        try {
            if (orderToLFTJ.containsKey(attributeOrder)) {
                HyperCubeEvaluationTask hyperCubeTask = orderToLFTJ.get(attributeOrder);
                return hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order);
            } else {
                StaticLFTJ staticLFTJ = StaticLFTJCollections.generateLFTJ(attributeOrder);
                HyperCubeEvaluationTask hyperCubeTask = new HyperCubeEvaluationTask(staticLFTJ.idToIter, staticLFTJ.itersNumberByVar, this.joinResult);
                orderToLFTJ.put(attributeOrder, hyperCubeTask);
                return hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
