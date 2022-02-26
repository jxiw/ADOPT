package joining;

import config.JoinConfig;
import joining.join.wcoj.*;
import joining.plan.AttributeOrder;
import joining.result.JoinResult;

import java.util.HashMap;

public class ParallelLFTJ {

    public final HashMap<AttributeOrder, HyperCubeEvaluationTask> orderToLFTJ;

    private final JoinResult joinResult;

    public long executionTime = 0;

    public long waitTime = 0;

    public boolean isFinish = false;

    public ParallelLFTJ(JoinResult result) {
        this.orderToLFTJ = new HashMap<>();
        this.joinResult = result;
        this.executionTime = 0;
        this.waitTime = 0;
        this.isFinish = false;
    }

    public double execute(int[] order) {
        AttributeOrder attributeOrder = new AttributeOrder(order);
        try {
            if (orderToLFTJ.containsKey(attributeOrder)) {
                HyperCubeEvaluationTask hyperCubeTask = orderToLFTJ.get(attributeOrder);
                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if(selectCube.intervals.size() == 0) {
                    // receive special hypercube (terminate hypercube)
                    this.isFinish = true;
                    return 0;
                }
                long startExecMillis = System.currentTimeMillis();
                double reward = hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                long endMillis = System.currentTimeMillis();
                waitTime += startExecMillis - startWaitMillis;
                executionTime += endMillis - startExecMillis;
                return reward;
            } else {
                StaticLFTJ staticLFTJ = StaticLFTJCollections.generateLFTJ(attributeOrder);
                HyperCubeEvaluationTask hyperCubeTask = new HyperCubeEvaluationTask(staticLFTJ.idToIter, staticLFTJ.itersNumberByVar, this.joinResult);
                orderToLFTJ.put(attributeOrder, hyperCubeTask);
                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if(selectCube.intervals.size() == 0) {
                    // finish all hypercubes
                    this.isFinish = true;
                    return 0;
                }
                long startExecMillis = System.currentTimeMillis();
                double reward = hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                long endMillis = System.currentTimeMillis();
                waitTime += startExecMillis - startWaitMillis;
                executionTime += endMillis - startExecMillis;
                return reward;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
