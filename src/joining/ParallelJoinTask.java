package joining;

import config.JoinConfig;
import joining.join.wcoj.Hypercube;
import joining.join.wcoj.HypercubeManager;
import joining.result.JoinResult;
import joining.uct.SelectionPolicy;
import joining.uct.UctNodeLFTJ;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class ParallelJoinTask implements Callable<ParallelJoinResult> {

//    final DynamicLFTJ joinOp;

    final QueryInfo query;

//    public JoinResult joinResult;

    public List<int[]> joinResult;

    private ParallelLFTJ parallelLFTJ;

    public ParallelJoinTask(QueryInfo query) {
//        this.joinOp = joinOp;
        this.query = query;
        this.joinResult = new ArrayList<>();
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
        long startMillis = System.currentTimeMillis();
//        System.out.println("start in system:" + startMillis);
        while (!this.parallelLFTJ.isFinish) {
            ++roundCtr;
            double reward = root.sample(roundCtr, attributeOrder, policy);

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
        System.out.println("thread:"+ Thread.currentThread().getId() + ", total duration in ms:" + (endMillis - startMillis));
        System.out.println("thread:"+ Thread.currentThread().getId() + ", init time in ms:" + parallelLFTJ.initLFTJTime);
        System.out.println("thread:"+ Thread.currentThread().getId() + ", execution time in ms:" + parallelLFTJ.executionTime);
        System.out.println("thread:"+ Thread.currentThread().getId() + ", wait time in ms:" + parallelLFTJ.waitTime);
        return new ParallelJoinResult(joinResult);
    }

}
