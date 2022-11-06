package joining;

import config.JoinConfig;
import joining.join.wcoj.*;
import joining.plan.AttributeOrder;
import util.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ParallelLFTJ {

    final HashMap<AttributeOrder, HyperCubeEvaluationTask> orderToLFTJ;

//    public long executionTime = 0;
//
//    public long waitTime = 0;
//
//    public long initLFTJTime = 0;
//
//    public long taskInitTime = 0;

    public boolean isFinish;

    public int[] joinResult;

    final AggregateData[] aggregateDatas;

    final Map<Integer, List<Integer>> aggregateInfo;

    public ParallelLFTJ(AggregateData[] aggregateDatas, Map<Integer, List<Integer>> aggregateInfo) {
        this.orderToLFTJ = new HashMap<>();
        this.isFinish = false;
        this.aggregateDatas = aggregateDatas;
        this.aggregateInfo = aggregateInfo;
        this.joinResult = new int[this.aggregateDatas.length];
        Arrays.fill(joinResult, -1);
    }

    public double execute(int[] order) {
        AttributeOrder attributeOrder = new AttributeOrder(order);
        try {
            if (orderToLFTJ.containsKey(attributeOrder)) {
//                long initStartMillis = System.currentTimeMillis();
                HyperCubeEvaluationTask hyperCubeTask = orderToLFTJ.get(attributeOrder);
//                long initEndMillis = System.currentTimeMillis();
//                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if (selectCube.intervals.size() == 0) {
                    // receive special hypercube (terminate hypercube)
                    this.isFinish = true;
                    return 0;
                }
//                long startExecMillis = System.currentTimeMillis();
                Pair<Double, int[]> result = hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                double reward = result.getFirst();
                int[] queryResult = result.getSecond();
                for (int i = 0; i < queryResult.length; i++) {
                    if (joinResult[i] != -1 && queryResult[i] != -1) {
                        if ((queryResult[i] < joinResult[i] && aggregateDatas[i].isMin) || (queryResult[i] > joinResult[i] && !aggregateDatas[i].isMin))
                            this.joinResult[i] = queryResult[i];
                    } else if (queryResult[i] != -1) {
                        this.joinResult[i] = queryResult[i];
                    }
                }
//                long endMillis = System.currentTimeMillis();
//                waitTime += startExecMillis - startWaitMillis;
//                executionTime += endMillis - startExecMillis;
//                initLFTJTime += initEndMillis - initStartMillis;
                return reward;
            } else {
//                long initStartMillis = System.currentTimeMillis();
                StaticLFTJ staticLFTJ = StaticLFTJCollections.generateLFTJ(attributeOrder);
//                long initEndMillis = System.currentTimeMillis();
                List<Pair<Integer, Integer>> attributeValueBound = Arrays.stream(order).mapToObj(StaticLFTJCollections.joinValueBound::get).collect(Collectors.toList());
                HyperCubeEvaluationTask hyperCubeTask = new HyperCubeEvaluationTask(staticLFTJ.idToIter, staticLFTJ.itersNumberByVar, attributeValueBound, aggregateDatas, aggregateInfo);
                orderToLFTJ.put(attributeOrder, hyperCubeTask);
//                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if (selectCube.intervals.size() == 0) {
                    // finish all hypercubes
                    this.isFinish = true;
                    return 0;
                }
//                long startExecMillis = System.currentTimeMillis();
                Pair<Double, int[]> result = hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                double reward = result.getFirst();
                int[] queryResult = result.getSecond();
                for (int i = 0; i < queryResult.length; i++) {
                    if (joinResult[i] != -1 && queryResult[i] != -1) {
                        if ((queryResult[i] < joinResult[i] && aggregateDatas[i].isMin) || (queryResult[i] > joinResult[i] && !aggregateDatas[i].isMin))
                            this.joinResult[i] = queryResult[i];
                    } else if (queryResult[i] != -1) {
                        this.joinResult[i] = queryResult[i];
                    }
                }
//                long endMillis = System.currentTimeMillis();
//                waitTime += startExecMillis - startWaitMillis;
//                executionTime += endMillis - startExecMillis;
//                initLFTJTime += initEndMillis - initStartMillis;
//                taskInitTime += startWaitMillis - initEndMillis;
                return reward;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
