package joining;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import catalog.CatalogManager;
import config.LoggingConfig;
import config.NamingConfig;
import config.JoinConfig;
import joining.join.wcoj.Hypercube;
import joining.join.wcoj.HypercubeManager;
import joining.join.wcoj.LFTJiter;
import joining.result.JoinResult;
import operators.Distinct;
import operators.Materialize;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import util.CartesianProduct;

/**
 * Controls the join phase.
 *
 * @author immanueltrummer
 */
public class JoinProcessor {
    /**
     * The number of join-related log entries
     * generated for the current query.
     */
    static int nrLogEntries = 0;

    public static final ExecutorService executorService = Executors.newFixedThreadPool(JoinConfig.NTHREAD);
//

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

        // Initialize statistics
        long startMillis = System.currentTimeMillis();
        // distinct if enable
        // decomposition
        log("Creating unique join keys ...");
        if (JoinConfig.DISTINCT_START) {
            query.aliasToTable.keySet().parallelStream().forEach(alias -> {
                try {
                    List<String> joinRequiredCols = new ArrayList<String>();
                    for (ColumnRef joinRequiredCol : query.colsForJoins) {
                        if (joinRequiredCol.aliasName.equals(alias)) {
                            joinRequiredCols.add(joinRequiredCol.columnName);
                        }
                    }
                    String distinctName = NamingConfig.DISTINCT_PRE + alias;
                    Distinct.execute(context.aliasToFiltered.get(alias), joinRequiredCols, distinctName);
                    context.aliasToDistinct.put(alias, distinctName);
                } catch (Exception e) {
                    System.err.println("Error distincting " + alias);
                    e.printStackTrace();
                }
            });
        }

        // join phrase
        long joinStartMillis = System.currentTimeMillis();
        // Initialize UCT join order search tree
        StaticLFTJCollections.init(query, context);
        HypercubeManager.init(StaticLFTJCollections.joinValueBound, JoinConfig.NTHREAD);
        JoinResult result = new JoinResult(query.nrJoined);
        System.out.println("start cube number:" + HypercubeManager.hypercubes.size());

        List<Future<ParallelJoinResult>> evaluateResults = new ArrayList<>();
        for (int i = 0; i < JoinConfig.NTHREAD; i++) {
            evaluateResults.add(executorService.submit(new ParallelJoinTask(query)));
        }

        for (Future<ParallelJoinResult> futureResult : evaluateResults) {
//            System.out.println("merge prev start:" + System.currentTimeMillis());
            ParallelJoinResult joinResult = futureResult.get();
//            System.out.println("merge start:" + System.currentTimeMillis());
            result.merge(joinResult.result);
//            System.out.println("merge end:" + System.currentTimeMillis());
        }

//        executorService.shutdown();
//        try {
//            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        System.out.println("hypercubes:" + HypercubeManager.hypercubes.size());
        long joinEndMillis = System.currentTimeMillis();
        System.out.println("join time:" + (joinEndMillis - joinStartMillis));

        LFTJiter.clearCache();

        String targetRelName = NamingConfig.JOINED_NAME;

        if (JoinConfig.DISTINCT_END) {
            List<int[]> tuples = result.getTuples();
            List<int[]> realTuples = new ArrayList<>();
            for (int[] tuple : tuples) {
                List<List<Integer>> realIndices = new ArrayList<>();
                for (int aliasCtr = 0; aliasCtr < query.nrJoined; ++aliasCtr) {
                    String distinctTableName = context.aliasToDistinct.get(query.aliases[aliasCtr]);
                    realIndices.add(Distinct.tableNamesToUniqueIndexes.get(distinctTableName).get(tuple[aliasCtr]));
                }
                List<List<Integer>> realIndicesFlatten = CartesianProduct.constructCombinations(realIndices);
                realIndicesFlatten.forEach(realIndex -> realTuples.add(
                        realIndex.stream().mapToInt(Integer::intValue).toArray()));
            }

            int nrTuples = realTuples.size();
            log("Materializing join result with " + nrTuples + " tuples ...");
            Materialize.execute(realTuples, query.aliasToIndex,
                    query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
        } else {
            // Materialize result table
            List<int[]> tuples = result.getTuples();
            int nrTuples = tuples.size();
            System.out.println("Materializing join result with " + nrTuples + " tuples ...");
            Materialize.execute(tuples, query.aliasToIndex,
                    query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
        }

        // Update processing context
        context.columnMapping.clear();
        for (ColumnRef postCol : query.colsForPostProcessing) {
            String newColName = postCol.aliasName + "." + postCol.columnName;
            ColumnRef newRef = new ColumnRef(targetRelName, newColName);
            context.columnMapping.put(postCol, newRef);
        }
        // Store number of join result tuples
        JoinStats.skinnerJoinCard = CatalogManager.
                getCardinality(NamingConfig.JOINED_NAME);
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
