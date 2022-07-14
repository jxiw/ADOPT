package joining;

import buffer.BufferManager;
import config.JoinConfig;
import data.ColumnData;
import data.IntData;
import joining.join.wcoj.StaticLFTJ;
import joining.plan.AttributeOrder;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import util.ArrayUtil;
import util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StaticLFTJCollections {

    static ConcurrentHashMap<AttributeOrder, StaticLFTJ> staticLFTJMap;

    static QueryInfo query;

    static Context executionContext;

    static List<Pair<Integer, Integer>> joinValueBound;

    static long initTime = 0;

    public static boolean init(QueryInfo query, Context executionContext) throws Exception {
        StaticLFTJCollections.query = query;
        StaticLFTJCollections.executionContext = executionContext;
        StaticLFTJCollections.staticLFTJMap = new ConcurrentHashMap<>();
        StaticLFTJCollections.joinValueBound = new ArrayList<>();
        for (Set<ColumnRef> joinAttributes : query.equiJoinAttribute) {
            // lb is the max value among all iterators lower bound
            // ub is the min value among all iterators upper bound
            int lb = Integer.MIN_VALUE;
            int ub = Integer.MAX_VALUE;
            for (ColumnRef attribute : joinAttributes) {
                // Retrieve corresponding data
                String alias = attribute.aliasName;
                String table = executionContext.aliasToFiltered.get(alias);
                if (JoinConfig.DISTINCT_START) {
                    table = executionContext.aliasToDistinct.get(alias);
                }
                String column = attribute.columnName;
                ColumnRef baseRef = new ColumnRef(table, column);
                ColumnData columnData = BufferManager.getData(baseRef);
                System.out.println(columnData.getClass().getName());
                if (columnData instanceof IntData) {
                    IntData columnIntData = (IntData) columnData;
                    if (columnIntData.data == null || columnIntData.data.length == 0) {
                        return false;
                    }
                    lb = Math.max(lb, ArrayUtil.getLowerBound(columnIntData.data));
                    ub = Math.min(ub, ArrayUtil.getUpperBound(columnIntData.data));
                    System.out.println("lb:" + lb + ", ub:" + ub +", card:" + columnIntData.cardinality);
                }
            }
            joinValueBound.add(new Pair<>(lb, ub));
        }
        return true;
    }

    public static StaticLFTJ generateLFTJ(AttributeOrder order) throws Exception {
        synchronized (staticLFTJMap) {
            if (staticLFTJMap.contains(order)) {
                return staticLFTJMap.get(order);
            } else {
//                long startMillis = System.currentTimeMillis();
                StaticLFTJ staticLFTJ = new StaticLFTJ(query, executionContext, order.order, joinValueBound);
                staticLFTJMap.put(order, staticLFTJ);
//                long endMillis = System.currentTimeMillis();
//                System.out.println("duration for StaticLFTJ:" + (endMillis - startMillis));
                return staticLFTJ;
            }
        }
    }

}
