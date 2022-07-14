package joining;

import config.JoinConfig;
import joining.join.wcoj.Hypercube;
import joining.join.wcoj.HypercubeManager;
import joining.join.wcoj.LFTJoin;
import joining.uct.ParallelUctNodeLFTJ;
import joining.uct.SelectionPolicy;
import joining.uct.UctNodeLFTJ;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncParallelJoinTask implements Callable<ParallelJoinResult> {

    final QueryInfo query;

    final ParallelLFTJ parallelLFTJ;

    private ParallelUctNodeLFTJ root;

    public List<int[]> joinResult;

    static AtomicInteger roundCtr = new AtomicInteger(0);

    final int threadId;

    public AsyncParallelJoinTask(QueryInfo query, ParallelUctNodeLFTJ uctNodeLFTJ, int threadId) {
        this.query = query;
        this.joinResult = new ArrayList<>();
        this.parallelLFTJ = new ParallelLFTJ(this.joinResult);
        this.root = uctNodeLFTJ;
        this.threadId = threadId;
    }

    @Override
    public ParallelJoinResult call() throws Exception {
        // Initialize counters and variables
        int[] attributeOrder = new int[query.nrAttribute];
        // Get default action selection policy
        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
        long totalExecMillis = 0;
        long startMillis = System.currentTimeMillis();
        while (!this.parallelLFTJ.isFinish) {
            // sample attribute order
            long beforeSampleMillis = System.nanoTime();
            if (threadId == 0) {
                int roundCtrInt = roundCtr.incrementAndGet();
                root.sample(roundCtrInt, attributeOrder, policy, parallelLFTJ, threadId);
            } else {
                int[] optimalOrder = new int[query.nrAttribute];
                Arrays.fill(optimalOrder, -1);
                root.getOptimalOrder(optimalOrder);
                boolean existOptimalOrder = true;
                for (int attribute:optimalOrder) {
                    if (attribute == -1) {
                        existOptimalOrder = false;
                        break;
                    }
                }
                if (existOptimalOrder) {
                    parallelLFTJ.execute(optimalOrder);
                } else {
                    long roundCtrInt = roundCtr.incrementAndGet();
                    root.sample(roundCtrInt, attributeOrder, policy, parallelLFTJ, threadId);
                }
            }
            long afterSampleMillis = System.nanoTime();
            if (HypercubeManager.nrCube.get() == 0 && HypercubeManager.isFinished()) {
                // notify other thread to terminate
                for (int i = 0; i < JoinConfig.NTHREAD; i++) {
                    HypercubeManager.hypercubes.add(new Hypercube(new ArrayList<>()));
                }
                break;
            }
            totalExecMillis += (afterSampleMillis - beforeSampleMillis);
        }
        long endMillis = System.currentTimeMillis();
        int[] optimalOrder = new int[query.nrAttribute];
        int[] bestFreqOrder = new int[query.nrAttribute];
        root.getOptimalOrder(optimalOrder);
        root.getMostFreqOrder(bestFreqOrder);
        System.out.println("thread:" + Thread.currentThread().getId() + "lftj exec time in ms:" + totalExecMillis * 1e-6);
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
        return new ParallelJoinResult(this.joinResult);
    }

}
