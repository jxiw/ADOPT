package joining.join.wcoj;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

import joining.join.DynamicMWJoin;
import joining.plan.AttributeOrder;
import preprocessing.Context;
import query.QueryInfo;

/**
 * Implements variant of the Leapfrog Trie Join
 * (see paper "Leapfrog Triejoin: a worst-case
 * optimal join algorithm" by T. Veldhuizen).
 *
 * @author immanueltrummer
 */
public class DynamicLFTJ extends DynamicMWJoin {
    /**
     * How many attribute orders to
     * choose from via learning.
     */
//	final int nrOrders = 3;
//	/**
//	 * All available attribute orders.
//	 */
//	List<StaticLFTJ> staticOrderOps = new ArrayList<>();

    HashMap<AttributeOrder, StaticLFTJ> orderToLFTJ = new HashMap<>();

    /**
     * Milliseconds at which joins start.
     */
    long joinStartMillis = -1;

    /**
     * Avoids redundant evaluation work by tracking evaluation progress.
     */
    public final ProgressTrackerLFTJ tracker;

//	public DynamicLFTJ(QueryInfo query,
//			Context executionContext) throws Exception {
//		super(query, executionContext);
//		// Clear cache of tuple orders
//		LFTJiter.queryOrderCache.clear();
//		// Prepare join with different attribute orders
//		long startMillis = System.currentTimeMillis();
//		for (int orderCtr=0; orderCtr<nrOrders; ++orderCtr) {
//			staticOrderOps.add(new StaticLFTJ(
//					query, executionContext, this.result));
//		}
//		long totalMillis = System.currentTimeMillis() - startMillis;
//		System.out.println("Preparation took " + totalMillis + " ms");
//		joinStartMillis = System.currentTimeMillis();
//	}

//	@Override
//	public double execute(int[] order) throws Exception {
//		int pick = order[0] % nrOrders;
//		StaticLFTJ pickedOp = staticOrderOps.get(pick);
//		pickedOp.resumeJoin(500);
//		System.out.println("results:" + pickedOp.lastNrResults);
//		return pickedOp.lastNrResults;
//	}

//	@Override
//	public boolean isFinished() {
//		// Check for timeout
//		if (System.currentTimeMillis() - joinStartMillis > 60000) {
//			return true;
//		}
//		// Check whether full result generated
//		for (StaticLFTJ staticOp : staticOrderOps) {
//			if (staticOp.isFinished()) {
//				return true;
//			}
//		}
//		return false;
//	}

    long wcjInitMillis = 0;

    long lookUpMillis = 0;

    long containMillis = 0;

    public StaticLFTJ finalConvergeStaticLFTJ;

    public boolean isFinish;

    AttributeOrder previousOrder;

    public DynamicLFTJ(QueryInfo query,
                       Context executionContext) throws Exception {
        super(query, executionContext);
        // Clear cache of tuple orders
        LFTJiter.queryOrderCache.clear();
        joinStartMillis = System.currentTimeMillis();
        this.tracker = new ProgressTrackerLFTJ(query.nrAttribute);
    }

    @Override
    public double execute(int[] order) throws Exception {
        AttributeOrder attributeOrder = new AttributeOrder(order);
        StateLFTJ state = tracker.continueFrom(attributeOrder);
//        long startCreateTime = System.currentTimeMillis();
//        StaticLFTJ pickedOp = orderToLFTJ.getOrDefault(attributeOrder, new StaticLFTJ(query, this.preSummary, this.result, attributeOrder.order));
        StaticLFTJ pickedOp;
//        long endFinishTime1 = 0;
//        long endFinishTime2 = 0;
//        long endFinishTime3 = 0;
        if (orderToLFTJ.containsKey(attributeOrder)){
//            endFinishTime1 = System.currentTimeMillis();
            pickedOp = orderToLFTJ.get(attributeOrder);
//            endFinishTime2 = System.currentTimeMillis();
//            lookUpMillis += (endFinishTime2 - endFinishTime1);
        }
        else {
//            endFinishTime1 = System.currentTimeMillis();
            pickedOp = new StaticLFTJ(query, this.preSummary, this.result, attributeOrder.order);
            orderToLFTJ.put(attributeOrder, pickedOp);
//            endFinishTime3 = System.currentTimeMillis();
//            wcjInitMillis += (endFinishTime3 - endFinishTime1);
        }
//        containMillis += (endFinishTime1 - startCreateTime);
//        long endFinishTime2 = System.currentTimeMillis();
//        System.out.println("contain:" + containMillis);
//        System.out.println("lookup Millis:" + lookUpMillis);
//        System.out.println("wcj Init Millis:" + wcjInitMillis);
//        orderToLFTJ.putIfAbsent(attributeOrder, pickedOp);

        System.out.println("lftj order:" + Arrays.stream(order).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList()));
        System.out.println("lftj order:" + Arrays.toString(order));
        System.out.println("start state:" + state);
        state.isReuse = previousOrder != null && previousOrder.equals(attributeOrder);
        double reward = pickedOp.resumeJoin(100, state);
        tracker.updateProgress(attributeOrder, state);
//        System.out.println("results:" + pickedOp.lastNrResults);
        System.out.println("end state:" + state);
        System.out.println("lftj reward:" + reward);
        previousOrder = attributeOrder;
        isFinish = pickedOp.isFinished();
        return reward;
    }

    @Override
    public boolean isFinished() {
        return isFinish;
        // Check whether full result generated
//        for (StaticLFTJ staticOp : orderToLFTJ.values()) {
//            if (staticOp.isFinished()) {
//                finalConvergeStaticLFTJ = staticOp;
//                return true;
//            }
//        }
//        return false;
    }

}
