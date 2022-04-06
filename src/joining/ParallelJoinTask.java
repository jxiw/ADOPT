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

//    final DynamicLFTJ joinOp;

    final QueryInfo query;

//    public JoinResult joinResult;

//    public List<int[]> joinResult;

    private ParallelLFTJ parallelLFTJ;

    private UctNodeLFTJ root;

    static int roundCtr = 0;

    public ParallelJoinTask(QueryInfo query, UctNodeLFTJ uctNodeLFTJ) {
//        this.joinOp = joinOp;
        this.query = query;
//        this.joinResult = new ArrayList<>();
        this.parallelLFTJ = new ParallelLFTJ();
        this.root = uctNodeLFTJ;
    }

    @Override
    public ParallelJoinResult call() throws Exception {
//        UctNodeLFTJ root = new UctNodeLFTJ(0, query, true, this.parallelLFTJ);
//        long roundCtr = 0;
        // Initialize counters and variables
        int[] attributeOrder = new int[query.nrAttribute];
        // Get default action selection policy
        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
        long startMillis = System.currentTimeMillis();
//        System.out.println("start in system:" + startMillis);
        while (!this.parallelLFTJ.isFinish) {
            // sample attribute order
            synchronized(root) {
                ++roundCtr;
                root.sample(roundCtr, attributeOrder, policy);
//                System.out.println("join order:" + Arrays.toString(attributeOrder));
            }
            double reward = parallelLFTJ.execute(attributeOrder);
            synchronized (root) {
//                System.out.println("join order:" + Arrays.toString(attributeOrder));
//                System.out.println("reward:" + reward);
                root.updateReward(reward, attributeOrder, 0);
//                int[] optimalOrder = new int[query.nrAttribute];
//                Arrays.fill(optimalOrder, -1);
//                root.getOptimalOrder(optimalOrder);
//                System.out.println("current optimal join order:" + Arrays.toString(optimalOrder));
            }

//            boolean isWorking = false;
//            synchronized (HypercubeManager.isWorking) {
//                for (boolean b : HypercubeManager.isWorking.values()) {
//                    if (b) {
//                        isWorking = true;
//                        break;
//                    }
//                }
//            }

            if (HypercubeManager.nrCube.get() == 0 && HypercubeManager.isFinished()) {
                // notify other thread to terminate
//                System.out.println("end in system:" + System.currentTimeMillis());
                for (int i = 0; i < JoinConfig.NTHREAD ; i++) {
                    HypercubeManager.hypercubes.add(new Hypercube(new ArrayList<>()));
                }
                break;
            }
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
