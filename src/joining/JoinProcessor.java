package joining;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import config.LoggingConfig;
import config.NamingConfig;
import config.JoinConfig;
import joining.join.wcoj.*;
import joining.uct.ParallelUctNodeLFTJ;
import operators.Distinct;
import operators.Materialize;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import util.CartesianProduct;

public class JoinProcessor {
    /**
     * The number of join-related log entries
     * generated for the current query.
     */
    static int nrLogEntries = 0;

    public static final ExecutorService executorService = Executors.newFixedThreadPool(JoinConfig.NTHREAD);

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

        System.out.println(IntStream.range(0, query.nrAttribute).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList()));

        // join phrase
        long mergeMillis = 0;
        long joinStartMillis = System.currentTimeMillis();
        // Initialize UCT join order search tree
        boolean notContainEmpty = StaticLFTJCollections.init(query, context);
        if (!notContainEmpty) {
            return;
        }
        HypercubeManager.init(StaticLFTJCollections.joinValueBound, JoinConfig.INITCUBE);

        ParallelUctNodeLFTJ root = new ParallelUctNodeLFTJ(0, query, true, JoinConfig.NTHREAD);

        MergedList<int[]> result = new MergedList<>();
        List<AsyncParallelJoinTask> tasks = new ArrayList<>();
        System.out.println("start join");
        System.out.println("start cube number:" + HypercubeManager.hypercubes.size());
        for (int i = 0; i < JoinConfig.NTHREAD; i++) {
            tasks.add(new AsyncParallelJoinTask(query, root, i));
        }

        List<Future<ParallelJoinResult>> evaluateResults = executorService.invokeAll(tasks);
        long joinEndMillis = System.currentTimeMillis();
        for (Future<ParallelJoinResult> futureResult : evaluateResults) {
            ParallelJoinResult joinResult = futureResult.get();
            long startMergeMillis = System.currentTimeMillis();
            if (joinResult.result.size() > 0) {
                result.add(joinResult.result);
            }
            long endMergeMillis = System.currentTimeMillis();
            mergeMillis += (endMergeMillis - startMergeMillis);
        }

        System.out.println("merge result time:" + mergeMillis);
        System.out.println("join time:" + (joinEndMillis - joinStartMillis));
        System.out.println("part 1:" + StaticLFTJ.part1);
        System.out.println("part 2:" + StaticLFTJ.part2);
        System.out.println("sort time:" + LFTJiter.sortTime);
        System.out.println("uniquify join value:" + (joinStartMillis - startMillis));
        System.out.println("LFTJiter 1:" + LFTJiter.lftTime1);
        System.out.println("LFTJiter 2:" + LFTJiter.lftTime2);
        System.out.println("LFTJiter 3:" + LFTJiter.lftTime3);
        System.out.println("LFTJiter 4:" + LFTJiter.lftTime4);

        LFTJiter.sortTime = 0;
        LFTJiter.lftTime1 = 0;
        LFTJiter.lftTime2 = 0;
        LFTJiter.lftTime3 = 0;
        LFTJiter.lftTime4 = 0;
        StaticLFTJ.part1 = 0;
        StaticLFTJ.part2 = 0;

        LFTJiter.clearCache();
        ParallelJoinTask.roundCtr = 0;

        String targetRelName = NamingConfig.JOINED_NAME;

        if (JoinConfig.DISTINCT_END) {
            long startDistinctMills = System.currentTimeMillis();
//            List<int[]> realTuples = result.parallelStream().flatMap(tuple -> {
//                List<List<Integer>> realIndices = new ArrayList<>();
//                for (int aliasCtr = 0; aliasCtr < query.nrJoined; ++aliasCtr) {
//                    // for each table
//                    String distinctTableName = context.aliasToDistinct.get(query.aliases[aliasCtr]);
//                    realIndices.add(Distinct.tableNamesToUniqueIndexes.get(distinctTableName).get(tuple[aliasCtr]));
//                }
//                List<List<Integer>> realIndicesFlatten = CartesianProduct.constructCombinations(realIndices);
//                return realIndicesFlatten.stream().map(realIndex -> realIndex.stream().mapToInt(Integer::intValue).toArray());
//            }).collect(Collectors.toList());

            List<int[]> realTuples = new ArrayList<>();
            for (int[] tuple : result) {
                List<List<Integer>> realIndices = new ArrayList<>();
                for (int aliasCtr = 0; aliasCtr < query.nrJoined; ++aliasCtr) {
                    String distinctTableName = context.aliasToDistinct.get(query.aliases[aliasCtr]);
                    realIndices.add(Distinct.tableNamesToUniqueIndexes.get(distinctTableName).get(tuple[aliasCtr]));
                }
                List<List<Integer>> realIndicesFlatten = CartesianProduct.constructCombinations(realIndices);
                realIndicesFlatten.forEach(realIndex -> realTuples.add(
                        realIndex.stream().mapToInt(Integer::intValue).toArray()));
            }

            long endDistinctMills = System.currentTimeMillis();
            System.out.println("distinct operator time:" + (endDistinctMills - startDistinctMills));

            int nrTuples = realTuples.size();
            System.out.println("Materializing join result with " + nrTuples + " tuples ...");
            Materialize.execute(realTuples, query.aliasToIndex,
                    query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
        } else {
            // Materialize result table
            int nrTuples = result.size();
            System.out.println("Materializing join result with " + nrTuples + " tuples ...");
            Materialize.execute(result, query.aliasToIndex,
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
