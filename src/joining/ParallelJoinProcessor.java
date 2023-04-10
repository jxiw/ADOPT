package joining;

import catalog.CatalogManager;
import config.*;
import expressions.ExpressionInfo;
import joining.join.wcoj.HypercubeManager;
import joining.join.wcoj.LFTJiter;
import joining.parallel.parallelization.dpdsync.DPDSync;
import joining.parallel.parallelization.leaf.LeafParallelization;
import joining.parallel.parallelization.root.RootParallelization;
import joining.parallel.parallelization.search.SearchParallelization;
import joining.parallel.parallelization.tree.TreeParallelization;
import joining.parallel.threads.ThreadPool;
import joining.result.ResultTuple;
import joining.uct.ParallelUctNodeLFTJ;
import net.sf.jsqlparser.expression.Expression;
import operators.Materialize;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.lockfree.LockFreeParallelization;
import postprocessing.ParallelPostProcessor;
import postprocessing.PostProcessor;
import predicate.NonEquiNode;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;
import util.CartesianProduct;
import util.MergedList;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This variant of the join processor parallelize
 * data processing in joining.parallel via multiple
 * threads.
 */
public class ParallelJoinProcessor {
    /**
     * The number of join-related log entries
     * generated for the current query.
     */
    static int nrLogEntries = 0;

    /**
     * The list of final result tuples.
     */
    public static ResultTuple[] results;

    /**
     * Executes the join phase and stores result in relation.
     * Also updates mapping from query column references to
     * database columns.
     *
     * @param query   query to process
     * @param context query execution context
     */
    public static void process(QueryInfo query,
                               Context context) throws Exception {
        long startMillis = System.currentTimeMillis();
        // there is no predicate to evaluate in join phase.
        System.out.println("join size:" + query.equiJoinPreds.size());
        System.out.println("join size2:" + query.equiJoinAttribute.size());
        System.out.println("nonEquiJoinPreds:" + query.nonEquiJoinPreds);
        if (query.equiJoinPreds.size() == 0 && query.nonEquiJoinPreds.size() == 0 && PreConfig.FILTER) {
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.executeFromExistingTable(query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
            // Measure execution time for join phase
            JoinStats.exeTime = 0;
            JoinStats.subExeTime.add(JoinStats.exeTime);
            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            // Store number of join result tuples
            int skinnerJoinCard = CatalogManager.getCardinality(targetRelName);
            JoinStats.skinnerJoinCards.add(skinnerJoinCard);
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + Arrays.toString(JoinStats.subExeTime.toArray()));
        } else if (query.equiJoinPreds.size() > 0) {
            // Join condition, apply leapfrog triejoin
            Set<ResultTuple> resultTuples = new HashSet<>();
            long startJoinMillis = System.currentTimeMillis();
//            long mergeMillis = 0;
//            long joinStartMillis = System.currentTimeMillis();
            MergedList<int[]> results = new MergedList<>();
            // Initialize UCT join order search tree
            StaticLFTJCollections.init(query, context);
            HypercubeManager.init(StaticLFTJCollections.joinValueBound, JoinConfig.INITCUBE);
            ParallelUctNodeLFTJ root = new ParallelUctNodeLFTJ(0, query, true, JoinConfig.NTHREAD);
            List<AsyncParallelJoinTask> tasks = new ArrayList<>();
            System.out.println("start join");
            System.out.println("start cube number:" + HypercubeManager.hypercubes.size());
            for (int i = 0; i < JoinConfig.NTHREAD; i++) {
                tasks.add(new AsyncParallelJoinTask(query, root, i));
            }
            List<Future<ParallelJoinResult>> evaluateResults = ThreadPool.executorService.invokeAll(tasks);
//            long joinEndMillis = System.currentTimeMillis();
            for (Future<ParallelJoinResult> futureResult : evaluateResults) {
                ParallelJoinResult joinResult = futureResult.get();
//                long startMergeMillis = System.currentTimeMillis();
                if (joinResult.result.size() > 0) {
                    results.add(joinResult.result);
                }
//                long endMergeMillis = System.currentTimeMillis();
//                mergeMillis += (endMergeMillis - startMergeMillis);
            }

            System.out.println("LFTJ time:" + (System.currentTimeMillis() - startJoinMillis));
            long nonEquiStartTime = System.currentTimeMillis();

            if (query.nonEquiJoinPreds.size() > 0) {
                // number of non equality join predicates
                int nrNonEquivalentPredict = query.nonEquiJoinNodes.size();
                List<ExpressionInfo> nonEquiExpressions = query.nonEquiJoinPreds;
//                List<NonEquiNode> nonEquiNodes = query.nonEquiJoinNodes;
                // map non equality predicate id to table id
                HashMap<Integer, Integer> nonEquiTablesToAliasIndex = new HashMap<>();
                HashMap<Integer, Integer> nonEquiCards = new HashMap<>();
                Set<Integer> availableTables = new HashSet<>();

                for (int tid = 0; tid < query.nrJoined; tid++) {
                    availableTables.add(tid);
                    for (int eid = 0; eid < nrNonEquivalentPredict; eid++) {
                        if (!nonEquiTablesToAliasIndex.containsKey(eid) && availableTables.containsAll(
                                nonEquiExpressions.get(eid).aliasIdxMentioned)) {
                            nonEquiTablesToAliasIndex.put(eid, tid);
                            String alias = query.aliases[tid];
                            String table = context.aliasToFiltered.get(alias);
                            nonEquiCards.put(eid, CatalogManager.getCardinality(table));
                        }
                    }
                }

                Set<Integer> validateRowIds = new HashSet<>(results.size());
                for (int rid = 0; rid < results.size(); rid++) {
                    validateRowIds.add(rid);
                }

                System.out.println("validate size:" + results.size());

                for (int nonEquiPredictId = 0; nonEquiPredictId < nrNonEquivalentPredict; nonEquiPredictId++) {
                    int processTableId = nonEquiTablesToAliasIndex.get(nonEquiPredictId);
                    int cardinality = nonEquiCards.get(nonEquiPredictId);
                    NonEquiNode nonEquiNode = query.nonEquiJoinNodes.get(nonEquiPredictId);
                    Set<Integer> currentValidateRowIds = new HashSet<>();
                    for (int rid : validateRowIds) {
                        int[] result = results.get(rid);
                        if (result[processTableId] == -1) {
                            // non equality condition is not mentioned before, should be exist or non-exist in subqueries
                            result[processTableId] = 0;
                            while (result[processTableId] < cardinality) {
                                if (nonEquiNode.evaluate(result, processTableId, cardinality)) {
                                    // satisfy condition
                                    currentValidateRowIds.add(rid);
                                    break;
                                } else {
                                    result[processTableId]++;
                                }
                            }
                        } else {
                            // test directly
                            if (nonEquiNode.evaluate(result, processTableId, cardinality)) {
                                // satisfy condition
                                currentValidateRowIds.add(rid);
                            }
                        }
                    }
                    validateRowIds = currentValidateRowIds;
                }

                System.out.println("Non Equi Time:" + (System.currentTimeMillis() - nonEquiStartTime));

                for (int validateRowId : validateRowIds) {
                    resultTuples.add(new ResultTuple(results.get(validateRowId)));
                }

            } else if (results.size() > 0) {
                boolean applyCartesianProduct = false;
                int cartesianProductTableIndex = -1;
                for (int tid = 0; tid < results.get(0).length; tid++) {
                    int rid = results.get(0)[tid];
                    if (rid == -1) {
                        applyCartesianProduct = true;
                        cartesianProductTableIndex = tid;
                        break;
                    }
                }
                if (applyCartesianProduct) {
                    // add more results here
                    List<int[]> cardResults = new ArrayList<>();
                    String alias = query.aliases[cartesianProductTableIndex];
                    String table = context.aliasToFiltered.get(alias);
                    int card = CatalogManager.getCardinality(table);
                    for (int[] result : results) {
                        result[cartesianProductTableIndex] = 0;
                        for (int i = 1; i < card; i++) {
                            int[] newResult = Arrays.copyOf(result, result.length);
                            newResult[cartesianProductTableIndex] = i;
                            cardResults.add(newResult);
                        }
                    }
                    results.add(cardResults);
                }
                resultTuples = results.stream().map(ResultTuple::new).collect(Collectors.toSet());
            }

            long endJoinMillis = System.currentTimeMillis();
            System.out.println("Finish Parallel Join!");
            System.out.println("join time:" + (endJoinMillis - startJoinMillis));
            LFTJiter.clearCache();
//            int median = GeneralConfig.TEST_CASE / 2;
//            long[] subExe = subExes.stream().mapToLong(exe->exe).toArray();
//            Arrays.sort(subExe);
//            JoinStats.subExeTime.add((endJoinMillis - startJoinMillis));

            long materializeStart = System.currentTimeMillis();
            // Materialize result table
//            int nrTuples = resultTuples.size();
//            String resultRel = query.plainSelect.getIntoTables().get(0).getName();
//            log("Materializing join result with " + nrTuples + " tuples ...");
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.execute(resultTuples, query.aliasToIndex,
                    query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
//            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            long materializeEnd = System.currentTimeMillis();
            JoinStats.subMateriazed.add(materializeEnd - materializeStart);
            // Store number of join result tuples
            int skinnerJoinCard = resultTuples.size();
            JoinStats.skinnerJoinCards.add(skinnerJoinCard);
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + Arrays.toString(JoinStats.subExeTime.toArray()));

        } else if (query.nonEquiJoinPreds.size() > 0) {
            // only contain non equality predicates
            Set<ResultTuple> resultTuples = new HashSet<>();
            // only support two tables here
            int cardinality1 = CatalogManager.getCardinality(context.aliasToFiltered.get(query.aliases[0]));
            int cardinality2 = CatalogManager.getCardinality(context.aliasToFiltered.get(query.aliases[1]));
            int[] rowIds = new int[]{0, 0};
            for (int i = 0; i < cardinality1; i++) {
                rowIds[0] = i;
                rowIds[1] = 0;
                while (rowIds[1] < cardinality2) {
                    if (query.nonEquiJoinNodes.get(0).evaluate(rowIds, 1, cardinality2)) {
                        // satisfy condition
                        resultTuples.add(new ResultTuple(rowIds));
                    }
                    rowIds[1]++;
                }
            }


//            List<List<Integer>> resultLists = new ArrayList<>();
//            int tid = 0;
//            int cardinality = 0;
//            for (; tid < query.nrJoined; tid++) {
//                String alias = query.aliases[tid];
//                String table = context.aliasToFiltered.get(alias);
//                cardinality = CatalogManager.getCardinality(table);
//                resultLists.add(IntStream.range(0, cardinality).boxed().collect(Collectors.toList()));
//            }
//            List<List<Integer>> finalResults = CartesianProduct.constructCombinations(resultLists);
//            for (List<Integer> finalResult : finalResults) {
//                int[] result = finalResult.stream().mapToInt(Integer::intValue).toArray();
//                boolean validate = true;
//                for (NonEquiNode nonEquiNode : query.nonEquiJoinNodes) {
//                    if (!nonEquiNode.evaluate(result, tid - 1, cardinality)) {
//                        validate = false;
//                        break;
//                    }
//                }
//                if (validate) {
//                    resultTuples.add(new ResultTuple(result));
//                }
//            }

            long materializeStart = System.currentTimeMillis();
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.execute(resultTuples, query.aliasToIndex,
                    query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            long materializeEnd = System.currentTimeMillis();
            JoinStats.subMateriazed.add(materializeEnd - materializeStart);
            // Store number of join result tuples
            int skinnerJoinCard = resultTuples.size();
            JoinStats.skinnerJoinCards.add(skinnerJoinCard);
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + Arrays.toString(JoinStats.subExeTime.toArray()));
        }
        // Measure execution time for join phase
        JoinStats.joinMillis = System.currentTimeMillis() - startMillis;
    }


    /**
     * Print out log entry if the maximal number of log
     * entries has not been reached yet.
     *
     * @param logEntry log entry to print
     */
    static void log(String logEntry) {
        if (nrLogEntries < LoggingConfig.MAX_JOIN_LOGS) {
            ++nrLogEntries;
            System.out.println(logEntry);
        }
    }
}
