package joining;

import config.JoinConfig;
import joining.join.wcoj.*;
import joining.plan.AttributeOrder;
import joining.result.JoinResult;
import util.Pair;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ParallelLFTJ {

    public final HashMap<AttributeOrder, HyperCubeEvaluationTask> orderToLFTJ;

    public long executionTime = 0;

    public long waitTime = 0;

    public long initLFTJTime = 0;

    public boolean isFinish = false;

    public long resultTuple = 0;

    public ParallelJoinResult joinResult;

    public ParallelLFTJ(ParallelJoinResult joinResult) {
        this.orderToLFTJ = new HashMap<>();
        this.executionTime = 0;
        this.waitTime = 0;
        this.isFinish = false;
        this.joinResult = joinResult;
        this.resultTuple = 0;
    }

    public double execute(int[] order) {
        AttributeOrder attributeOrder = new AttributeOrder(order);
        try {
            if (orderToLFTJ.containsKey(attributeOrder)) {
                long initStartMillis = System.currentTimeMillis();
                HyperCubeEvaluationTask hyperCubeTask = orderToLFTJ.get(attributeOrder);
                long initEndMillis = System.currentTimeMillis();
                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if(selectCube.intervals.size() == 0) {
                    // receive special hypercube (terminate hypercube)
                    this.isFinish = true;
                    return 0;
                }
                long startExecMillis = System.currentTimeMillis();
                hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                long endMillis = System.currentTimeMillis();
                waitTime += startExecMillis - startWaitMillis;
                executionTime += endMillis - startExecMillis;
                initLFTJTime += initEndMillis - initStartMillis;
                return 100;
            } else {
                long initStartMillis = System.currentTimeMillis();
                StaticLFTJ staticLFTJ = StaticLFTJCollections.generateLFTJ(attributeOrder);
                long initEndMillis = System.currentTimeMillis();
                List<Pair<Integer, Integer>> attributeValueBound = Arrays.stream(order).mapToObj(StaticLFTJCollections.joinValueBound::get).collect(Collectors.toList());
                HyperCubeEvaluationTask hyperCubeTask = new HyperCubeEvaluationTask(staticLFTJ.idToIter, staticLFTJ.itersNumberByVar, attributeValueBound, this.joinResult);
                orderToLFTJ.put(attributeOrder, hyperCubeTask);
                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if(selectCube.intervals.size() == 0) {
                    // finish all hypercubes
                    this.isFinish = true;
                    return 0;
                }
                long startExecMillis = System.currentTimeMillis();
                hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
//                this.resultTuple += result;
                long endMillis = System.currentTimeMillis();
                waitTime += startExecMillis - startWaitMillis;
                executionTime += endMillis - startExecMillis;
                initLFTJTime += initEndMillis - initStartMillis;
                return 100;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
