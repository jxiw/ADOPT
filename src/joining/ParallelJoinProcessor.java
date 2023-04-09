package joining;

import catalog.CatalogManager;
import config.*;
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
import util.MergedList;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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
        } else {
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
                int nrNonEquiPredict = query.nonEquiJoinNodes.size();
                HashMap<Integer, Integer> nonEquiTablesToAliasIndex = new HashMap<>();
                ArrayList<Integer> nonEquiCards = new ArrayList<>();
                int[] r = results.get(0);
                for (int tid = 0; tid < r.length; tid++) {
                    if (r[tid] == -1) {
                        nonEquiTablesToAliasIndex.put(nonEquiCards.size(), tid);
                        String alias = query.aliases[tid];
                        String table = context.aliasToFiltered.get(alias);
                        int cardinality = CatalogManager.getCardinality(table);
                        nonEquiCards.add(cardinality);
                    }
                }

                Set<Integer> validateRowIds = new HashSet<>(results.size());
                for (int rid = 0; rid < results.size(); rid++) {
                    validateRowIds.add(rid);
                }

                for (int nonEquiPredictId = 0; nonEquiPredictId < nrNonEquiPredict; nonEquiPredictId++) {
                    int processTableId = nonEquiTablesToAliasIndex.get(nonEquiPredictId);
                    int cardinality = nonEquiCards.get(nonEquiPredictId);
                    NonEquiNode nonEquiNode = query.nonEquiJoinNodes.get(nonEquiPredictId);
                    Set<Integer> currentValidateRowIds = new HashSet<>();
                    for (int rid : validateRowIds) {
                        int[] result = results.get(rid);
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
                    }
                    validateRowIds = currentValidateRowIds;
                }

                System.out.println("Non Equi Time" + (System.currentTimeMillis() - nonEquiStartTime));

                for (int validateRowId : validateRowIds) {
                    resultTuples.add(new ResultTuple(results.get(validateRowId)));
                }

//                for (int i = 0; i < results.size(); i++) {
//                    int[] result = results.get(i);
//                    int nonEquiTableIdx = 0;
//                    int[] nonEquiTablePositions = new int[nonEquiCards.size()];
//                    int currentProcessTableId = nonEquiTablesToAliasIndex.get(nonEquiTableIdx);
//                    int cardinality = nonEquiCards.get(nonEquiTableIdx);
//                    NonEquiNode nonEquiNode = query.nonEquiJoinNodes.get(nonEquiTableIdx);
//                    while (true) {
//                        if (nonEquiTablePositions[nonEquiTableIdx] >= cardinality) {
//                            if (nonEquiTableIdx == 0) {
//                                // finish execution
//                                break;
//                            } else {
//                                nonEquiTablePositions[nonEquiTableIdx] = 0;
//                                result[nonEquiTablesToAliasIndex.get(nonEquiTableIdx)] = 0;
//                                // reach to the end, and move to the previous node
//                                nonEquiTableIdx--;
//                                cardinality = nonEquiCards.get(nonEquiTableIdx);
//                                nonEquiNode = query.nonEquiJoinNodes.get(nonEquiTableIdx);
//                                currentProcessTableId = nonEquiTablesToAliasIndex.get(nonEquiTableIdx);
//                                // move to the next tuple
//                                nonEquiTablePositions[nonEquiTableIdx] += 1;
//                            }
//                        }
//
//                        // test validate current is valid
//                        result[nonEquiTablesToAliasIndex.get(nonEquiTableIdx)] = nonEquiTablePositions[nonEquiTableIdx];
//                        if (nonEquiNode.evaluate(result, currentProcessTableId, cardinality)) {
//                            assert nonEquiTableIdx < nrNonEquiTable;
//                            if (nonEquiTableIdx == nrNonEquiTable - 1) {
//                                // finish all validate testing, add into final result
//                                resultTuples.add(new ResultTuple(result));
//                                // move to next tuple
//                                nonEquiTablePositions[nonEquiTableIdx] += 1;
//                                break;
//                            } else {
//                                // proceed to next node
//                                nonEquiTableIdx += 1;
//                                // move to next validate node, and start from beginning
//                                nonEquiTablePositions[nonEquiTableIdx] = 0;
//                                cardinality = nonEquiCards.get(nonEquiTableIdx);
//                                nonEquiNode = query.nonEquiJoinNodes.get(nonEquiTableIdx);
//                                currentProcessTableId = nonEquiTablesToAliasIndex.get(nonEquiTableIdx);
//                            }
//
//                        } else {
//                            // move to next tuple
//                            nonEquiTablePositions[nonEquiTableIdx] += 1;
//                        }
//                    }
//                }

            } else {
                resultTuples = results.stream().map(ResultTuple::new).collect(Collectors.toSet());
            }


            long endJoinMillis = System.currentTimeMillis();
            System.out.println("Finish Parallel Join!");
            System.out.println("join time:" + (endJoinMillis - startJoinMillis));
            LFTJiter.clearCache();
//            int median = GeneralConfig.TEST_CASE / 2;
//            long[] subExe = subExes.stream().mapToLong(exe->exe).toArray();
//            Arrays.sort(subExe);
            JoinStats.subExeTime.add((endJoinMillis - startJoinMillis));

            long materializeStart = System.currentTimeMillis();
            // Materialize result table
            int nrTuples = resultTuples.size();
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
