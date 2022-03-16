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

//    private final JoinResult joinResult;

//    private final List<int[]> joinResult;

    public long executionTime = 0;

    public long waitTime = 0;

    public long initLFTJTime = 0;

    public boolean isFinish = false;

    public long resultTuple = 0;

    public ParallelLFTJ() {
        this.orderToLFTJ = new HashMap<>();
//        this.joinResult = result;
        this.executionTime = 0;
        this.waitTime = 0;
        this.isFinish = false;
        this.resultTuple = 0;
    }

    public double execute(int[] order) {
//        if (Thread.currentThread().getId() % JoinConfig.NTHREAD == 0) {
//            System.out.println("order:" + Arrays.toString(order));
//        }
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
                Pair<Double, Long> result = hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                double reward = result.getFirst();
                this.resultTuple += result.getSecond();
                long endMillis = System.currentTimeMillis();
                waitTime += startExecMillis - startWaitMillis;
                executionTime += endMillis - startExecMillis;
                initLFTJTime += initEndMillis - initStartMillis;
                return reward;
            } else {
                long initStartMillis = System.currentTimeMillis();
                StaticLFTJ staticLFTJ = StaticLFTJCollections.generateLFTJ(attributeOrder);
                long initEndMillis = System.currentTimeMillis();
                List<Pair<Integer, Integer>> attributeValueBound = Arrays.stream(order).mapToObj(StaticLFTJCollections.joinValueBound::get).collect(Collectors.toList());
                HyperCubeEvaluationTask hyperCubeTask = new HyperCubeEvaluationTask(staticLFTJ.idToIter, staticLFTJ.itersNumberByVar, attributeValueBound);
                orderToLFTJ.put(attributeOrder, hyperCubeTask);
                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if(selectCube.intervals.size() == 0) {
                    // finish all hypercubes
                    this.isFinish = true;
                    return 0;
                }
                long startExecMillis = System.currentTimeMillis();
                Pair<Double, Long> result =  hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                double reward = result.getFirst();
                this.resultTuple += result.getSecond();
                long endMillis = System.currentTimeMillis();
                waitTime += startExecMillis - startWaitMillis;
                executionTime += endMillis - startExecMillis;
                initLFTJTime += initEndMillis - initStartMillis;
                return reward;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
