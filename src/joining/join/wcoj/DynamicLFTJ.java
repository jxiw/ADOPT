package joining.join.wcoj;

import java.util.*;
import java.util.stream.Collectors;

import buffer.BufferManager;
import data.ColumnData;
import data.IntData;
import joining.join.DynamicMWJoin;
import joining.join.MultiWayJoin;
import joining.plan.AttributeOrder;
import net.sf.jsqlparser.schema.Column;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import util.ArrayUtil;
import util.Pair;

/**
 * Implements variant of the Leapfrog Trie Join
 * (see paper "Leapfrog Triejoin: a worst-case
 * optimal join algorithm" by T. Veldhuizen).
 *
 * @author immanueltrummer
 */
public class DynamicLFTJ extends DynamicMWJoin {

    /**
     * Avoids redundant evaluation work by tracking evaluation progress.
     */
    public final ProgressTrackerLFTJ tracker;

    /**
     * Whether finish or not.
     */
    public boolean isFinish;

    /**
     *
     */
    public List<Pair<Integer, Integer>> joinValues = new ArrayList<>();

    /**
     *
     */
    HashMap<AttributeOrder, StaticLFTJ> orderToLFTJ = new HashMap<>();

    /**
     * Milliseconds at which joins start.
     */
    long joinStartMillis = -1;

    /**
     *
     */
    AttributeOrder previousOrder;

    HypercubeManager manager;

    public DynamicLFTJ(QueryInfo query,
                       Context executionContext) throws Exception {
        super(query, executionContext);
        // Clear cache of tuple orders
        LFTJiter.queryOrderCache.clear();
        joinStartMillis = System.currentTimeMillis();
        this.tracker = new ProgressTrackerLFTJ(query.nrAttribute);
        // Init the hypercube, collect value range of each column
        for (Set<ColumnRef> joinAttributes: query.equiJoinAttribute) {
            int lb = Integer.MAX_VALUE;
            int ub = Integer.MIN_VALUE;
            for (ColumnRef attribute: joinAttributes) {
                ColumnData columnData = BufferManager.colToData.get(attribute);
                if (columnData instanceof IntData) {
                    IntData columnIntData = (IntData) columnData;
                    lb = Math.min(lb, ArrayUtil.getLowerBound(columnIntData.data));
                    ub = Math.max(ub, ArrayUtil.getUpperBound(columnIntData.data));
                    System.out.println("lb:" + lb + ", ub" + ub);
                }
            }
            joinValues.add(new Pair<>(lb, ub));
        }
        // init the hypercube
        manager = new HypercubeManager(joinValues);

    }

    @Override
    public double execute(int[] order) throws Exception {
        AttributeOrder attributeOrder = new AttributeOrder(order);
        // continue from order
        StateLFTJ state = tracker.continueFrom(attributeOrder);
        StaticLFTJ pickedOp;
        if (orderToLFTJ.containsKey(attributeOrder)){
            pickedOp = orderToLFTJ.get(attributeOrder);
        }
        else {
            pickedOp = new StaticLFTJ(query, this.preSummary, attributeOrder.order, MultiWayJoin.result, joinValues,
                    this.manager);
            orderToLFTJ.put(attributeOrder, pickedOp);
        }

//        System.out.println("lftj order:" + Arrays.stream(order).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList()));
//        System.out.println("lftj order:" + Arrays.toString(order));
//        System.out.println("start state:" + state);
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
    }

}
