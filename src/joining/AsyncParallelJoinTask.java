package joining;

import config.JoinConfig;
import joining.join.wcoj.Hypercube;
import joining.join.wcoj.HypercubeManager;
import joining.uct.ParallelUctNodeLFTJ;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
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

    public AsyncParallelJoinTask(QueryInfo query, Context context, ParallelUctNodeLFTJ uctNodeLFTJ, int threadId) {
        this.query = query;
        this.joinResult = new ArrayList<>();
        this.parallelLFTJ = new ParallelLFTJ(this.joinResult, query, context);
        this.root = uctNodeLFTJ;
        this.threadId = threadId;
    }

    @Override
    public ParallelJoinResult call() throws Exception {
        // Initialize counters and variables
        int[] attributeOrder = new int[query.nrAttribute];
        // Get default action selection policy
        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
        int nextForget = 1;
        while (!this.parallelLFTJ.isFinish) {
            // sample attribute order
            if (threadId == 0) {
                int roundCtrInt = roundCtr.incrementAndGet();
                root.sample(roundCtrInt, attributeOrder, policy, parallelLFTJ, threadId);
            } else {
                int[] optimalOrder = new int[query.nrAttribute];
                Arrays.fill(optimalOrder, -1);
                root.getOptimalOrder(optimalOrder);
                boolean existOptimalOrder = true;
                for (int attribute : optimalOrder) {
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

            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr.get() == nextForget) {
                root = new ParallelUctNodeLFTJ(0, query, true, JoinConfig.NTHREAD);
                nextForget *= 10;
            }

            if (HypercubeManager.nrCube.get() == 0 && HypercubeManager.isFinished()) {
                // notify other thread to terminate
                for (int i = 0; i < JoinConfig.NTHREAD; i++) {
                    HypercubeManager.hypercubes.add(new Hypercube(new ArrayList<>()));
                }
                break;
            }
        }

//        int[] optimalOrder = new int[query.nrAttribute];
//        int[] bestFreqOrder = new int[query.nrAttribute];
//        root.getOptimalOrder(optimalOrder);
//        root.getMostFreqOrder(bestFreqOrder);

        return new ParallelJoinResult(this.joinResult);
    }

}