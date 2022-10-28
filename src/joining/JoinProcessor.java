package joining;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.JoinConfig;
import expressions.aggregates.AggInfo;
import expressions.aggregates.SQLaggFunction;
import joining.join.wcoj.*;
import joining.uct.ParallelUctNodeLFTJ;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import types.SQLtype;

public class JoinProcessor {

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
//        long startMillis = System.currentTimeMillis();
        // distinct if enable
        // decomposition
//        log("Creating unique join keys ...");
//        if (JoinConfig.DISTINCT_START) {
//            query.aliasToTable.keySet().parallelStream().forEach(alias -> {
//                try {
//                    List<String> joinRequiredCols = new ArrayList<String>();
//                    for (ColumnRef joinRequiredCol : query.colsForJoins) {
//                        if (joinRequiredCol.aliasName.equals(alias)) {
//                            joinRequiredCols.add(joinRequiredCol.columnName);
//                        }
//                    }
//                    String distinctName = NamingConfig.DISTINCT_PRE + alias;
//                    Distinct.execute(context.aliasToFiltered.get(alias), joinRequiredCols, distinctName);
//                    context.aliasToDistinct.put(alias, distinctName);
//                } catch (Exception e) {
//                    System.err.println("Error distincting " + alias);
//                    e.printStackTrace();
//                }
//            });
//        }

//        System.out.println(IntStream.range(0, query.nrAttribute).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList()));

        // join phrase
        long mergeMillis = 0;
        long joinStartMillis = System.currentTimeMillis();
        // Initialize UCT join order search tree
        StaticLFTJCollections.init(query, context);
        HypercubeManager.init(StaticLFTJCollections.joinValueBound, JoinConfig.INITCUBE);

        int aggregateNum = query.aggregates.size();
        AggregateData[] aggregateDatas = new AggregateData[aggregateNum];
        SQLtype[] sqLtypes = new SQLtype[aggregateNum];
        Map<Integer, List<Integer>> aggregateInfo = new HashMap<>();
//        BufferManager.colToData.forEach((key, value) -> System.out.println(key + ":" + value));
//        System.out.println("query.aliases:" + Arrays.toString(query.aliases));
        int i = 0;
        for (AggInfo aggregate : query.aggregates) {
            AggregateData aggregateData = new AggregateData();
            aggregateData.isMin = (aggregate.aggFunction == SQLaggFunction.MIN);
            ColumnRef columnRef = aggregate.aggInput.columnsMentioned.iterator().next();
            ColumnRef filterColumnRef = new ColumnRef(context.aliasToFiltered.get(columnRef.aliasName), columnRef.columnName);
            aggregateData.columnData = BufferManager.getData(filterColumnRef);
            aggregateData.tid = aggregate.aggInput.aliasIdxMentioned.iterator().next();
            aggregateDatas[i] = aggregateData;
            aggregateInfo.putIfAbsent(aggregateData.tid, new ArrayList<>());
            aggregateInfo.get(aggregateData.tid).add(i);
            sqLtypes[i] = CatalogManager.getColumn(filterColumnRef).type;
            i++;
        }

//        System.out.println("sqLtypes:" + Arrays.toString(sqLtypes));
//        aggregateInfo.forEach((key, value) -> System.out.println(key + ":" + value));

        ParallelUctNodeLFTJ root = new ParallelUctNodeLFTJ(0, query, true, JoinConfig.NTHREAD);

        List<AsyncParallelJoinTask> tasks = new ArrayList<>();
        System.out.println("start join");
        System.out.println("start cube number:" + HypercubeManager.hypercubes.size());
        for (i = 0; i < JoinConfig.NTHREAD; i++) {
            tasks.add(new AsyncParallelJoinTask(query.nrAttribute, root, i, aggregateDatas, aggregateInfo));
        }

        List<Future<ParallelJoinResult>> evaluateResults = executorService.invokeAll(tasks);
        long joinEndMillis = System.currentTimeMillis();
        int[] queryResult = new int[aggregateDatas.length];
        Arrays.fill(queryResult, -1);
        for (Future<ParallelJoinResult> futureResult : evaluateResults) {
            ParallelJoinResult joinResult = futureResult.get();
            long startMergeMillis = System.currentTimeMillis();
            for (int j = 0; j < queryResult.length; j++) {
                if (queryResult[j] != -1 && joinResult.result[j] != -1) {
                    if ((joinResult.result[j] < queryResult[j] && aggregateDatas[j].isMin)
                            || (joinResult.result[j] > queryResult[j] && !aggregateDatas[j].isMin))
                        queryResult[j] = joinResult.result[j];
                } else if (joinResult.result[j] != -1) {
                    queryResult[j] = joinResult.result[j];
                }
            }
            long endMergeMillis = System.currentTimeMillis();
            mergeMillis += (endMergeMillis - startMergeMillis);
        }

        System.out.println("merge result time:" + mergeMillis);
        System.out.println("join time:" + (joinEndMillis - joinStartMillis));

        LFTJiter.clearCache();

        // print final result
        String[] outputs = new String[aggregateNum];
        for (int j = 0; j < aggregateNum; j++) {
            SQLtype type = sqLtypes[j];
            if (queryResult[j] != -1) {
                switch (type) {
                    case INT:
                    case DOUBLE: {
                        outputs[j] = String.valueOf(queryResult[j]);
                        break;
                    }
                    case STRING_CODE: {
                        outputs[j] = BufferManager.dictionary.getString(queryResult[j]);
                        break;
                    }
                }
            }
        }
        System.out.println("------------------------");
        System.out.println(String.join("\t", outputs));
        System.out.println("------------------------");
    }
}