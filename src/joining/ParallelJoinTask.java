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

    static int roundCtr = 0;

    public ParallelJoinTask(QueryInfo query) {
        this.query = query;
        this.parallelLFTJ = new ParallelLFTJ();
    }

    @Override
    public ParallelJoinResult call() throws Exception {
        UctNodeLFTJ root = new UctNodeLFTJ(0, query, true, this.parallelLFTJ);
        long roundCtr = 0;
        // Initialize counters and variables
        int[] attributeOrder = new int[query.nrAttribute];
        // Get default action selection policy
        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
        long startMillis = System.currentTimeMillis();
        while (!this.parallelLFTJ.isFinish) {
            ++roundCtr;
            root.sample(roundCtr, attributeOrder, policy);
            // obtain optimal order
            int[] optimalOrder = new int[query.nrAttribute];
            Arrays.fill(optimalOrder, -1);
            root.getOptimalOrder(optimalOrder);
            System.out.println("thread:" + Thread.currentThread().getId() + ", current optimal join order:" + Arrays.toString(optimalOrder));
            Arrays.fill(optimalOrder, -1);
            root.getMostFreqOrder(optimalOrder);
            System.out.println("thread:" + Thread.currentThread().getId() + ", current most frequent join order:" + Arrays.toString(optimalOrder));
            if (HypercubeManager.nrCube.get() == 0 && HypercubeManager.isFinished()) {
                // notify other thread to terminate
                for (int i = 0; i < JoinConfig.NTHREAD; i++) {
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
        System.out.println("thread:" + Thread.currentThread().getId() + ", total duration in ms:" + (endMillis - startMillis));
        System.out.println("thread:" + Thread.currentThread().getId() + ", init time in ms:" + parallelLFTJ.initLFTJTime);
        System.out.println("thread:" + Thread.currentThread().getId() + ", execution time in ms:" + parallelLFTJ.executionTime);
        System.out.println("thread:" + Thread.currentThread().getId() + ", best join order:" + Arrays.toString(optimalOrder));
        System.out.println("thread:" + Thread.currentThread().getId() + ", most frequent join order:" + Arrays.toString(bestFreqOrder));
        System.out.println("thread:" + Thread.currentThread().getId() + ", wait time in ms:" + parallelLFTJ.waitTime);
        System.out.println("thread:" + Thread.currentThread().getId() + ", seek time in ms:" + parallelLFTJ.orderToLFTJ.values().stream().mapToLong(i -> {
            long ts = 0;
            for (LFTJoin join : i.joins) {
                ts += join.seekTime;
            }
            return ts;
        }).sum());
        return new ParallelJoinResult(parallelLFTJ.resultTuple);
    }

}
