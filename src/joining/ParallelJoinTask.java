package joining;

import config.JoinConfig;
import joining.join.wcoj.Hypercube;
import joining.join.wcoj.HypercubeManager;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class ParallelJoinTask implements Callable<ParallelJoinResult> {

    final QueryInfo query;

    private ParallelLFTJ parallelLFTJ;

    private ParallelJoinResult joinResult;

    private int[] order;

    public ParallelJoinTask(QueryInfo query) {
        this.query = query;
        this.joinResult = new ParallelJoinResult();
        this.parallelLFTJ = new ParallelLFTJ(this.joinResult);
        this.order = JoinConfig.order;
    }

    @Override
    public ParallelJoinResult call() {
        long startMillis = System.currentTimeMillis();
//        System.out.println("lftj order:" + Arrays.stream(order).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList()));
        while (!this.parallelLFTJ.isFinish) {
            this.parallelLFTJ.execute(order);
            if (HypercubeManager.nrCube.get() == 0 && HypercubeManager.isFinished()) {
                // notify other thread to terminate
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
        System.out.println("thread:"+ Thread.currentThread().getId() + ", tuple number:" + parallelLFTJ.joinResult.result);
        return joinResult;
    }

}
