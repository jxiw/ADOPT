package joining;

import buffer.BufferManager;
import data.ColumnData;
import data.IntData;
import joining.join.wcoj.StaticLFTJ;
import joining.plan.AttributeOrder;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import util.ArrayUtil;
import util.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StaticLFTJCollections {

    static ConcurrentHashMap<AttributeOrder, StaticLFTJ> staticLFTJMap;

    static QueryInfo query;

    static Context executionContext;

    static List<Pair<Integer, Integer>> joinValueBound;

    public static Map<ColumnRef, int[]> joinValueBoundCache =
            new HashMap<>();

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
                String column = attribute.columnName;
                ColumnRef baseRef = new ColumnRef(table, column);
                long start = System.currentTimeMillis();
                if (joinValueBoundCache.containsKey(baseRef)) {
                    int[] bound = joinValueBoundCache.get(baseRef);
                    lb = Math.max(lb, bound[0]);
                    ub = Math.max(ub, bound[1]);
                } else {
                    ColumnData columnData = BufferManager.getData(baseRef);
                    if (columnData instanceof IntData) {
                        IntData columnIntData = (IntData) columnData;
                        if (columnIntData.data == null || columnIntData.data.length == 0) {
                            return false;
                        }
                        int ilb = ArrayUtil.getLowerBound(columnIntData.data);
                        int iub = ArrayUtil.getUpperBound(columnIntData.data);
                        lb = Math.max(lb, ilb);
                        ub = Math.min(ub, iub);
//                        if (!table.contains(FILTERED_PRE)) {
//                            joinValueBoundCache.put(baseRef, new Pair<>(ilb, iub));
//                        }
                        System.out.println("baseRef:" + baseRef + ", card:" + columnIntData.data.length);
                    }
                }
                System.out.println("baseRef:" + baseRef + ", min:" + lb  + ", max:" + ub);
                System.out.println("min max duration:" + (System.currentTimeMillis() - start));
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
