package joining.join.wcoj;

import java.util.*;
import java.util.stream.Collectors;

import buffer.BufferManager;
import data.ColumnData;
import data.IntData;
import joining.join.DynamicMWJoin;
import joining.join.MultiWayJoin;
import joining.plan.AttributeOrder;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import util.ArrayUtil;
import util.Pair;

public class DynamicLFTJ extends DynamicMWJoin {

    /**
     * Avoids redundant evaluation work by tracking evaluation progress.
     */
//    public final ProgressTrackerLFTJ tracker;

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
//        this.tracker = new ProgressTrackerLFTJ(query.nrAttribute);
        // Init the hypercube, collect value range of each column
        for (Set<ColumnRef> joinAttributes: query.equiJoinAttribute) {
            // lb is the max value among all iterators lower bound
            // ub is the min value among all iterators upper bound
            int lb = Integer.MIN_VALUE;
            int ub = Integer.MAX_VALUE;
            for (ColumnRef attribute: joinAttributes) {
                // Retrieve corresponding data
                String alias = attribute.aliasName;
                String table = preSummary.aliasToDistinct.get(alias);
                String column = attribute.columnName;
                ColumnRef baseRef = new ColumnRef(table, column);
                ColumnData columnData = BufferManager.getData(baseRef);
                System.out.println(columnData.getClass().getName());
                if (columnData instanceof IntData) {
                    IntData columnIntData = (IntData) columnData;
                    lb = Math.max(lb, ArrayUtil.getLowerBound(columnIntData.data));
                    ub = Math.min(ub, ArrayUtil.getUpperBound(columnIntData.data));
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

        System.out.println("lftj order:" + Arrays.stream(order).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList()));
        System.out.println("lftj order:" + Arrays.toString(order));

        // continue from order
        StaticLFTJ pickedOp;
        if (orderToLFTJ.containsKey(attributeOrder)) {
            pickedOp = orderToLFTJ.get(attributeOrder);
        }
        else {
            pickedOp = new StaticLFTJ(query, this.preSummary, attributeOrder.order, MultiWayJoin.result, joinValues,
                    this.manager);
            orderToLFTJ.put(attributeOrder, pickedOp);
        }

        double reward = pickedOp.resumeJoin(1000);
        System.out.println("lftj reward:" + reward);
        previousOrder = attributeOrder;
        if (pickedOp.isFinished()) {
            isFinish = true;
        }
        return reward;
    }

    @Override
    public boolean isFinished() {
        return isFinish;
    }

}
