package joining.join.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.CheckConfig;
import data.ColumnData;
import data.IntData;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

public class LFTJiter {
    /**
     * Cardinality of table that we
     * are iterating over.
     */
    final int card;
    /**
     * Contains data of columns that form
     * the trie levels.
     */
    final List<IntData> trieCols;
    /**
     * Contains row IDs of rows ordered by
     * the variables in the order in which
     * they appear in the global variable
     * order.
     */
    final Integer[] tupleOrder;
    /**
     * Number of trie levels (i.e., number
     * of attributes the trie indexes).
     */
    final int nrLevels;
    /**
     * We are at this level of the trie.
     */
//    int curTrieLevel;
    /**
     * Maximally admissible tuple index
     * at current level (value in prior
     * trie levels changes for later
     * tuples).
     */
//    final int[] curUBs;
    /**
     * Contains for each trie level the current position
     * (expressed as tuple index in tuple sort order).
     */
//    final int[] curTuples;
    /**
     * Caches tuple orderings for tables after applying
     * query-specific unary predicates. Such orderings
     * can be reused across different join orders
     * for the same query.
     */
    static Map<List<ColumnRef>, Integer[]> queryOrderCache =
            new HashMap<>();
    /**
     * Caches tuple orderings for base tables that can
     * be reused across different queries.
     */
    static Map<List<ColumnRef>, Integer[]> baseOrderCache =
            new HashMap<>();
//
    static long sortTime = 0;
//
//    static long lftTime1 = 0;
//
//    static long lftTime2 = 0;
//
//    static long lftTime3 = 0;
//
//    static long lftTime4 = 0;

    /**
     * Initializes iterator for given query and
     * relation, and given (global) variable order.
     *
     * @param query          initialize for this query
     * @param context        execution context
     * @param aliasID        initialize for this join table
     * @param globalVarOrder global order of variables
     */
    public LFTJiter(QueryInfo query, Context context, int aliasID,
                    List<Set<ColumnRef>> globalVarOrder) throws Exception {
        // Get information on target table
        String alias = query.aliases[aliasID];
        String table = context.aliasToDistinct.get(alias);
//        String table = context.aliasToFiltered.get(alias);
        card = CatalogManager.getCardinality(table);
        // Extract columns used for sorting
        List<ColumnRef> localColumns = new ArrayList<>();
        trieCols = new ArrayList<>();
//        long stime1 = System.currentTimeMillis();
        for (Set<ColumnRef> eqClass : globalVarOrder) {
            for (ColumnRef colRef : eqClass) {
                if (colRef.aliasName.equals(alias)) {
                    localColumns.add(colRef);
                    String colName = colRef.columnName;
                    ColumnRef bufferRef = new ColumnRef(table, colName);
                    ColumnData colData = BufferManager.getData(bufferRef);
                    trieCols.add((IntData) colData);
                }
            }
        }
        // Initialize position array
        nrLevels = trieCols.size();

        // Retrieve cached tuple order or sort
//        long stime2 = System.currentTimeMillis();
        tupleOrder = getTupleOrder(query,
                context, aliasID, localColumns);
//        long stime3 = System.currentTimeMillis();
        // Reset internal state
//        reset();

//        long stime4 = System.currentTimeMillis();
        // Perform run time checks if activated
//        IterChecker.checkIter(query, context,
//                aliasID, globalVarOrder, this);
//        long stime5 = System.currentTimeMillis();
//        lftTime1 += (stime2 - stime1);
//        lftTime2 += (stime3 - stime2);
//        lftTime3 += (stime4 - stime3);
//        lftTime4 += (stime5 - stime4);
//        System.out.println("lftTime1:" + lftTime1);
//        System.out.println("lftTime2:" + lftTime2);
//        System.out.println("lftTime3:" + lftTime3);
//        System.out.println("lftTime4:" + lftTime4);
    }

    /**
     * Sorts tuples by their values in local columns,
     * returns tuple IDs from tuple order in array.
     *
     * @param localColumns sort by those columns
     * @return array with sorted tuple indices
     */
    Integer[] getTupleOrder(QueryInfo query, Context executionContext,
                            int aliasID, List<ColumnRef> localColumns) {
        // No unary predicates for current alias?
        String alias = query.aliases[aliasID];
        boolean notFiltered = executionContext.
                aliasToDistinct.get(alias).equals(alias);
//                aliasToFiltered.get(alias).equals(alias);
        // Did we cache tuple order for associated base tables?
        if (notFiltered && baseOrderCache.containsKey(localColumns)) {
            return baseOrderCache.get(localColumns);
        } else
            // Retrieve cached tuple order or sort
            if (queryOrderCache.containsKey(localColumns)) {
                return queryOrderCache.get(localColumns);
            } else {
                // Initialize tuple order
                Integer[] tupleOrder = new Integer[card];
                for (int i = 0; i < card; ++i) {
                    tupleOrder[i] = i;
                }

                long startCreateTime = System.currentTimeMillis();
                // Sort tuples by global variable order
                Arrays.parallelSort(tupleOrder, new Comparator<Integer>() {
                    public int compare(Integer row1, Integer row2) {
                        for (ColumnData colData : trieCols) {
                            int cmp = colData.compareRows(row1, row2);
                            if (cmp == 2) {
                                boolean row1null = colData.isNull.get(row1);
                                boolean row2null = colData.isNull.get(row2);
                                if (row1null && !row2null) {
                                    return -1;
                                } else if (!row1null && row2null) {
                                    return 1;
                                }
                            } else if (cmp != 0) {
                                return cmp;
                            }
                        }
                        return 0;
                    }
                });
                long endCreateTime = System.currentTimeMillis();
                sortTime += (endCreateTime - startCreateTime);
                System.out.println("sort time:" + sortTime);

                // build hash map, tuple order position to real position
//			ArrayList<Integer> uniqueTupleOrder = new ArrayList<>();
//			for (int i = 1; i < card; i++) {
//				int tupleIdx1 = tupleOrder[i - 1];
//				int tupleIdx2 = tupleOrder[i];
//				boolean unique = trieCols.parallelStream().anyMatch(column -> column.data[tupleIdx1] != column.data[tupleIdx2]);
//				if (unique) {
//					uniqueTupleOrder.add(i - 1);
//				} else {
//					System.out.println("same as prevous" + tupleIdx2);
//				}
//			}
//			uniqueTupleOrder.add(card - 1);
//			System.out.println("unique tuple size:" + uniqueTupleOrder.size());
//			System.out.println("tuple size:" + tupleOrder.length);
                // Distinguish by cache
                if (notFiltered) {
                    baseOrderCache.put(localColumns, tupleOrder);
                } else {
                    queryOrderCache.put(localColumns, tupleOrder);
                }
                return tupleOrder;
            }
    }

    public static void clearCache() {
        LFTJiter.queryOrderCache = new HashMap<>();
        LFTJiter.baseOrderCache = new HashMap<>();
    }

}