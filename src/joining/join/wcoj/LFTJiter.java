package joining.join.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.CheckConfig;
import config.JoinConfig;
import data.ColumnData;
import data.IntData;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

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
//    Integer[] tupleOrder;
    int[] tupleOrder;

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

//    /**
//     * Caches tuple orderings for tables after applying
//     * query-specific unary predicates. Such orderings
//     * can be reused across different join orders
//     * for the same query.
//     */
//    static Map<List<ColumnRef>, Integer[]> queryOrderCache =
//            new HashMap<>();
//    /**
//     * Caches tuple orderings for base tables that can
//     * be reused across different queries.
//     */
//    static Map<List<ColumnRef>, Integer[]> baseOrderCache =
//            new HashMap<>();

    /**
     * Caches tuple orderings for tables after applying
     * query-specific unary predicates. Such orderings
     * can be reused across different join orders
     * for the same query.
     */
    static Map<List<ColumnRef>, int[]> queryOrderCache =
            new HashMap<>();
    /**
     * Caches tuple orderings for base tables that can
     * be reused across different queries.
     */
    static Map<List<ColumnRef>, int[]> baseOrderCache =
            new HashMap<>();

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
        String table = context.aliasToFiltered.get(alias);
        if (JoinConfig.DISTINCT_START) {
            table = context.aliasToDistinct.get(alias);
        }
        card = CatalogManager.getCardinality(table);
        // Extract columns used for sorting
        List<ColumnRef> localColumns = new ArrayList<>();
        trieCols = new ArrayList<>();
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

        getTupleOrder(query,
                context, aliasID, localColumns);

    }

    /**
     * Sorts tuples by their values in local columns,
     * returns tuple IDs from tuple order in array.
     *
     * @param localColumns sort by those columns
     * @return array with sorted tuple indices
     */
    void getTupleOrder(QueryInfo query, Context executionContext,
                       int aliasID, List<ColumnRef> localColumns) {
        // No unary predicates for current alias?
//        long initMillis = System.currentTimeMillis();
        String alias = query.aliases[aliasID];
        boolean notFiltered = executionContext.
                aliasToFiltered.get(alias).equals(alias);
        if (JoinConfig.DISTINCT_START) {
            notFiltered = executionContext.aliasToFiltered.get(alias).equals(alias);
        }

        if (notFiltered && baseOrderCache.containsKey(localColumns)) {
            tupleOrder = baseOrderCache.get(localColumns);
        } else {
            // Retrieve cached tuple order or sort
            if (queryOrderCache.containsKey(localColumns)) {
                tupleOrder = queryOrderCache.get(localColumns);
            } else {
                // Initialize tuple order

                tupleOrder = IntStream.range(0, card).boxed().parallel().sorted(new Comparator<Integer>() {
                    @Override
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
                }).mapToInt(i -> i).toArray();

                // Distinguish by cache
                if (notFiltered) {
                    baseOrderCache.put(localColumns, tupleOrder);
                } else {
                    queryOrderCache.put(localColumns, tupleOrder);
                }
            }
        }
    }

    public static void clearCache() {
        LFTJiter.queryOrderCache = new HashMap<>();
        LFTJiter.baseOrderCache = new HashMap<>();
    }

}