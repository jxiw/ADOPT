//package joining.join.wcoj;
//
//import java.util.*;
//import java.util.stream.IntStream;
//
//import buffer.BufferManager;
//import catalog.CatalogManager;
//import config.JoinConfig;
//import data.ColumnData;
//import data.IntData;
//import preprocessing.Context;
//import query.ColumnRef;
//import query.QueryInfo;
//
//import static config.NamingConfig.FILTERED_PRE;
//
//public class LFTJiter {
//    /**
//     * Cardinality of table that we
//     * are iterating over.
//     */
//    final int card;
//    /**
//     * Contains data of columns that form
//     * the trie levels.
//     */
//    final List<IntData> trieCols;
//    /**
//     * Contains row IDs of rows ordered by
//     * the variables in the order in which
//     * they appear in the global variable
//     * order.
//     */
//    int[] tupleOrder;
//    /**
//     * Number of trie levels (i.e., number
//     * of attributes the trie indexes).
//     */
//    final int nrLevels;
//
//    /**
//     * Caches tuple orderings for tables after applying
//     * query-specific unary predicates. Such orderings
//     * can be reused across different join orders
//     * for the same query.
//     */
//    static Map<List<ColumnRef>, int[]> queryOrderCache =
//            new HashMap<>();
//    /**
//     * Caches tuple orderings for base tables that can
//     * be reused across different queries.
//     */
//    static Map<List<ColumnRef>, int[]> baseOrderCache =
//            new HashMap<>();
//
//    public static long sortTime = 0;
//    public static long lftTime1 = 0;
//    public static long lftTime2 = 0;
//    public static long lftTime3 = 0;
//
//    public List<String> colNames = new ArrayList();
//
//    private String table;
//
//    /**
//     * Initializes iterator for given query and
//     * relation, and given (global) variable order.
//     *
//     * @param query          initialize for this query
//     * @param context        execution context
//     * @param aliasID        initialize for this join table
//     * @param globalVarOrder global order of variables
//     */
//    public LFTJiter(QueryInfo query, Context context, int aliasID,
//                    List<Set<ColumnRef>> globalVarOrder) throws Exception {
//        // Get information on target table
//        long stime1 = System.currentTimeMillis();
//        String alias = query.aliases[aliasID];
//        table = context.aliasToFiltered.get(alias);
//        if (JoinConfig.DISTINCT_START) {
//            table = context.aliasToDistinct.get(alias);
//        }
//        card = CatalogManager.getCardinality(table);
//        long stime2 = System.currentTimeMillis();
//        // Extract columns used for sorting
//        List<ColumnRef> localColumns = new ArrayList<>();
//        trieCols = new ArrayList<>();
//        for (Set<ColumnRef> eqClass : globalVarOrder) {
//            for (ColumnRef colRef : eqClass) {
//                if (colRef.aliasName.equals(alias)) {
//                    localColumns.add(colRef);
//                    String colName = colRef.columnName;
//                    ColumnRef bufferRef = new ColumnRef(table, colName);
//                    ColumnData colData = BufferManager.getData(bufferRef);
//                    trieCols.add((IntData) colData);
//                    colNames.add(colName);
//                }
//            }
//        }
//        // Initialize position array
//        nrLevels = trieCols.size();
//
//        long stime3 = System.currentTimeMillis();
//
//        // Retrieve cached tuple order or sort
//        getTupleOrder(query,
//                context, aliasID, localColumns);
//
//        long stime4 = System.currentTimeMillis();
//
//        lftTime1 += (stime2 - stime1);
//        lftTime2 += (stime3 - stime2);
//        lftTime3 += (stime4 - stime3);
//    }
//
//    /**
//     * Sorts tuples by their values in local columns,
//     * returns tuple IDs from tuple order in array.
//     *
//     * @param localColumns sort by those columns
//     * @return array with sorted tuple indices
//     */
//    void getTupleOrder(QueryInfo query, Context executionContext,
//                       int aliasID, List<ColumnRef> localColumns) {
//        // No unary predicates for current alias?
//        String alias = query.aliases[aliasID];
//        boolean notFiltered = !executionContext.
//                aliasToFiltered.get(alias).contains(FILTERED_PRE);
//        // Did we cache tuple order for associated base tables?
//        if (notFiltered && baseOrderCache.containsKey(localColumns)) {
//            tupleOrder = baseOrderCache.get(localColumns);
//        } else {
//            // Retrieve cached tuple order or sort
//            if (queryOrderCache.containsKey(localColumns)) {
//                tupleOrder = queryOrderCache.get(localColumns);
//            } else {
//
//                long part3Millis = System.currentTimeMillis();
//                tupleOrder = IntStream.range(0, card).boxed().sorted(new Comparator<Integer>() {
//                    @Override
//                    public int compare(Integer row1, Integer row2) {
//                        for (IntData colData : trieCols) {
//                            int cmp = Integer.compare(colData.data[row1], colData.data[row2]);
//                            if (cmp != 0)
//                                return cmp;
////                            int cmp = colData.compareRows(row1, row2);
////                            if (cmp == 2) {
////                                boolean row1null = colData.isNull.get(row1);
////                                boolean row2null = colData.isNull.get(row2);
////                                if (row1null && !row2null) {
////                                    return -1;
////                                } else if (!row1null && row2null) {
////                                    return 1;
////                                }
////                            } else if (cmp != 0) {
////                                return cmp;
////                            }
//                        }
//                        return 0;
//                    }
//                }).mapToInt(i -> i).toArray();
//
//                long endCreateTime = System.currentTimeMillis();
//                System.out.println("tableName:" + table + ", colNames:" + colNames);
//                System.out.println("sort time now:" + (endCreateTime - part3Millis));
//                sortTime += (endCreateTime - part3Millis);
//
//                // Distinguish by cache
//                if (notFiltered) {
//                    baseOrderCache.put(localColumns, tupleOrder);
//                } else {
//                    queryOrderCache.put(localColumns, tupleOrder);
//                }
//            }
//        }
//    }
//
//    public static void clearCache() {
//        LFTJiter.queryOrderCache = new HashMap<>();
//    }
//
//    public Integer compareTuples(int row1, int row2) {
////        for (IntData colData : trieCols) {
////            int cmp = colData.compareRows(row1, row2);
////            if (cmp == 2) {
////                boolean row1null = colData.isNull.get(row1);
////                boolean row2null = colData.isNull.get(row2);
////                if (row1null && !row2null) {
////                    return -1;
////                } else if (!row1null && row2null) {
////                    return 1;
////                }
////            } else if (cmp != 0) {
////                return cmp;
////            }
////        }
//        for (IntData colData : trieCols) {
//            int cmp = Integer.compare(colData.data[row1], colData.data[row2]);
//            if (cmp != 0) {
//                return cmp;
//            }
//        }
//        return 0;
//    }
//
//}