package joining;

import config.JoinConfig;
import joining.join.wcoj.Hypercube;
import joining.join.wcoj.HypercubeManager;
import joining.join.wcoj.LFTJoin;
import joining.uct.SelectionPolicy;
import joining.uct.UctNodeLFTJ;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class ParallelJoinTask implements Callable<ParallelJoinResult> {

    final QueryInfo query;

    private ParallelLFTJ parallelLFTJ;

    private UctNodeLFTJ root;

    static int roundCtr = 0;

    public ParallelJoinTask(QueryInfo query, UctNodeLFTJ uctNodeLFTJ) {
        this.query = query;
        this.parallelLFTJ = new ParallelLFTJ();
        this.root = uctNodeLFTJ;
    }

    @Override
    public ParallelJoinResult call() throws Exception {
        // Initialize counters and variables
        int[] attributeOrder = new int[query.nrAttribute];
        // Get default action selection policy
        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
//        long totalSampleMillis = 0;
//        long totalUpdateMillis = 0;
        long totalExecMillis = 0;
//        long testMillis = 0;
//        long testPrevMillis = 0;
        long startMillis = System.currentTimeMillis();
        while (!this.parallelLFTJ.isFinish) {
            // sample attribute order
//            long startSampleMillis = System.nanoTime();
            synchronized(root) {
                ++roundCtr;
                root.sample(roundCtr, attributeOrder, policy);
            }
            long endSampleMillis = System.nanoTime();
            double reward = parallelLFTJ.execute(attributeOrder);
            long startUpdateMillis = System.nanoTime();
            synchronized (root) {
//                System.out.println("join order:" + Arrays.toString(attributeOrder));
//                System.out.println("reward:" + reward);
                root.updateReward(reward, attributeOrder, 0);
//                int[] optimalOrder = new int[query.nrAttribute];
//                Arrays.fill(optimalOrder, -1);
//                root.getOptimalOrder(optimalOrder);
//                System.out.println("current optimal join order:" + Arrays.toString(optimalOrder));
//                Arrays.fill(optimalOrder, -1);
//                root.getMostFreqOrder(optimalOrder);
//                System.out.println("current most frequent join order:" + Arrays.toString(optimalOrder));
            }

//            long endUpdateMillis = System.nanoTime();
//            long testStartMillis = System.nanoTime();
            if (HypercubeManager.nrCube.get() == 0 && HypercubeManager.isFinished()) {
                // notify other thread to terminate
                for (int i = 0; i < JoinConfig.NTHREAD ; i++) {
                    HypercubeManager.hypercubes.add(new Hypercube(new ArrayList<>()));
                }
                break;
            }
//            long testEndMillis = System.nanoTime();
//            testMillis += (testEndMillis - testStartMillis);
//            totalSampleMillis += (endSampleMillis - startSampleMillis);
//            totalUpdateMillis += (endUpdateMillis - startUpdateMillis);
            totalExecMillis += (startUpdateMillis - endSampleMillis);

//            System.out.println("total sample duration:" + totalSampleMillis * 1e-6);
//            System.out.println("total update duration:" + totalUpdateMillis * 1e-6);
//            System.out.println("init time in ms:" + parallelLFTJ.initLFTJTime * 1e-6);
//            System.out.println("wait time in ms:" + parallelLFTJ.waitTime * 1e-6);
//            System.out.println("execution time in ms:" + parallelLFTJ.executionTime * 1e-6);
//            System.out.println("lftj exec time in ms:" + totalExecMillis * 1e-6);
//            System.out.println("task init time in ms:" + parallelLFTJ.taskInitTime * 1e-6);
//            System.out.println("time duration prev test in ms:" + testPrevMillis * 1e-6);
//            System.out.println("test millis:" + testMillis * 1e-6);
//            System.out.println("time until now in ms:" + (System.currentTimeMillis() - startMillis));
        }

        long endMillis = System.currentTimeMillis();
        int[] optimalOrder = new int[query.nrAttribute];
        int[] bestFreqOrder = new int[query.nrAttribute];
        root.getOptimalOrder(optimalOrder);
        root.getMostFreqOrder(bestFreqOrder);
        System.out.println("thread:"+ Thread.currentThread().getId() + ", total duration in ms:" + (endMillis - startMillis));
        System.out.println("thread:"+ Thread.currentThread().getId() + ", init time in ms:" + parallelLFTJ.initLFTJTime);
        System.out.println("thread:"+ Thread.currentThread().getId() + ", execution time in ms:" + parallelLFTJ.executionTime);
        System.out.println("thread:"+ Thread.currentThread().getId() + ", best join order:" + Arrays.toString(optimalOrder));
        System.out.println("thread:"+ Thread.currentThread().getId() + ", most frequent join order:" + Arrays.toString(bestFreqOrder));
        System.out.println("thread:"+ Thread.currentThread().getId() + ", wait time in ms:" + parallelLFTJ.waitTime);
        System.out.println("thread:"+ Thread.currentThread().getId() + ", seek time in ms:" + parallelLFTJ.orderToLFTJ.values().stream().mapToLong(i -> {
            long ts = 0;
            for (LFTJoin join : i.joins) {
                ts += join.seekTime;
            }
            return ts;
        }).sum());

//        System.out.println("thread:"+ Thread.currentThread().getId() + ", start 1 in ms:" + parallelLFTJ.orderToLFTJ.values().stream().mapToLong(i -> i.ts1).sum());
//        System.out.println("thread:"+ Thread.currentThread().getId() + ", start 2 in ms:" + parallelLFTJ.orderToLFTJ.values().stream().mapToLong(i -> i.ts2).sum());
//        System.out.println("thread:"+ Thread.currentThread().getId() + ", start 3 in ms:" + parallelLFTJ.orderToLFTJ.values().stream().mapToLong(i -> i.ts3).sum());
//        System.out.println("thread:"+ Thread.currentThread().getId() + ", start 4 in ms:" + parallelLFTJ.orderToLFTJ.values().stream().mapToLong(i -> i.ts4).sum());
//        System.out.println("thread:"+ Thread.currentThread().getId() + ", start 5 in ms:" + parallelLFTJ.orderToLFTJ.values().stream().mapToLong(i -> i.ts5).sum());

        return new ParallelJoinResult(parallelLFTJ.resultTuple);
    }

}
