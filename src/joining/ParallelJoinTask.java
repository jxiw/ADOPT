package joining;

import config.JoinConfig;
import joining.join.wcoj.Hypercube;
import joining.join.wcoj.HypercubeManager;
import joining.result.JoinResult;
import joining.uct.SelectionPolicy;
import joining.uct.UctNodeLFTJ;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.concurrent.Callable;

public class ParallelJoinTask implements Callable<ParallelJoinResult> {

//    final DynamicLFTJ joinOp;

    final QueryInfo query;

    public JoinResult joinResult;

    private ParallelLFTJ parallelLFTJ;

    public ParallelJoinTask(QueryInfo query) {
//        this.joinOp = joinOp;
        this.query = query;
        this.joinResult = new JoinResult(query.nrJoined);
        this.parallelLFTJ = new ParallelLFTJ(this.joinResult);
    }

    @Override
    public ParallelJoinResult call() throws Exception {
        UctNodeLFTJ root = new UctNodeLFTJ(0, query, true, this.parallelLFTJ);
        long roundCtr = 0;
        // Initialize counters and variables
        int[] attributeOrder = new int[query.nrAttribute];
        // Get default action selection policy
        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
        while (true) {
            ++roundCtr;
            double reward = root.sample(roundCtr, attributeOrder, policy);
            if (reward == -100) {
                break;
            }

            boolean isWorking = false;
            synchronized (HypercubeManager.isWorking) {
                for (boolean b : HypercubeManager.isWorking.values()) {
                    if (b) {
                        isWorking = true;
                        break;
                    }
                }
            }

            if (!isWorking && HypercubeManager.isFinished()) {
                // notify other thread to terminate
                for (int i = 0; i < JoinConfig.NTHREAD; i++) {
                    HypercubeManager.hypercubes.add(new Hypercube(new ArrayList<>()));
                }
                break;
            }
        }
        return new ParallelJoinResult(joinResult);
    }

}
