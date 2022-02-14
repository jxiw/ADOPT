package joining.join.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import buffer.BufferManager;
import config.CheckConfig;
import config.JoinConfig;
import data.ColumnData;
import data.IntData;
import joining.join.MultiWayJoin;
import joining.result.JoinResult;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import util.Pair;

public class StaticLFTJ extends MultiWayJoin {
//    /**
//     * Maps alias IDs to corresponding iterator.
//     */
//    final Map<String, LFTJiter> aliasToIter;
    /**
     * Contains at i-th position iterator over
     * i-th element in query from clause.
     */
    final LFTJiter[] idToIter;
    /**
     * Order of variables (i.e., equivalence classes
     * of join attributes connected via equality
     * predicates).
     */
    final List<Set<ColumnRef>> varOrder;
    /**
     * Contains at i-th position the iterators
     * involved in obtaining keys for i-th
     * variable (consistent with global
     * variable order).
     */
    final List<List<LFTJiter>> itersByVar;

    final List<List<Integer>> itersNumberByVar;
    /**
     * Number of variables in input query (i.e.,
     * number of equivalence classes of join columns
     * connected via equality predicates).
     */
    final int nrVars;
    /**
     * Whether entire result was generated.
     */
    boolean finished = false;
    /**
     * Counds iterations of the main loop.
     */
    long roundCtr = 0;

    HypercubeManager manager;

    final int[] attributeOrder;

    List<Pair<Integer, Integer>> attributeValueBound;

//    boolean isCache = false;

//    Map<String, Set<Integer>> joinTableToAttributeIdx;

//    Map<CacheAttribute, Set<Integer>> cacheAttributes;

    /**
     * Initialize join for given query.
     *
     * @param query            join query to process via LFTJ
     * @param executionContext summarizes procesing context
     * @throws Exception
     */
    public StaticLFTJ(QueryInfo query, Context executionContext, int[] order,
                      JoinResult joinResult, List<Pair<Integer, Integer>> attributeValueBound, HypercubeManager manager) throws Exception {
        // Initialize query and context variables
        super(query, executionContext, joinResult);
        // Choose variable order arbitrarily
//		varOrder = new ArrayList<>();
//		varOrder.addAll(query.equiJoinClasses);
//		Collections.shuffle(varOrder);
        attributeOrder = order;
        varOrder = Arrays.stream(order).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList());
        nrVars = query.equiJoinClasses.size();
//        System.out.println("Variable Order: " + varOrder);
//        attributesCardinality = new ArrayList();
        // Initialize iterators
//        long stime2 = System.currentTimeMillis();
        Map<String, LFTJiter> aliasToIter = new HashMap<>();
        HashMap<String, Integer> aliasToNumber = new HashMap<>();
        idToIter = new LFTJiter[nrJoined];
        for (int aliasCtr = 0; aliasCtr < nrJoined; ++aliasCtr) {
            String alias = query.aliases[aliasCtr];
            LFTJiter iter = new LFTJiter(query,
                    executionContext, aliasCtr, varOrder);
            aliasToIter.put(alias, iter);
            aliasToNumber.put(alias, aliasCtr);
            idToIter[aliasCtr] = iter;
        }
//        long stime3 = System.currentTimeMillis();
        // Group iterators by variable
        itersByVar = new ArrayList<>();
        itersNumberByVar = new ArrayList<>();
        for (Set<ColumnRef> var : varOrder) {
            List<LFTJiter> curVarIters = new ArrayList<>();
            List<Integer> curNumberIters = new ArrayList<>();
//            List<Integer> attributeCardinality = new ArrayList<>();
            for (ColumnRef colRef : var) {
                String alias = colRef.aliasName;
                LFTJiter iter = aliasToIter.get(alias);
                curVarIters.add(iter);
                curNumberIters.add(aliasToNumber.get(alias));
//                attributeCardinality.add(iter.card);
            }
            itersByVar.add(curVarIters);
            itersNumberByVar.add(curNumberIters);
//            attributesCardinality.add(attributeCardinality);
        }


//        long stime4 = System.currentTimeMillis();

//        if (isCache) {
//
//            // for cache
//            // gather table in each attribute
//            joinTableToAttributeIdx = new HashMap<>();
////            cacheAttributes = new HashMap<Set<Integer>, Set<Integer>>();
//            for (int aliasCtr = 0; aliasCtr < nrVars; ++aliasCtr) {
//                for (ColumnRef columnRef : varOrder.get(aliasCtr)) {
//                    joinTableToAttributeIdx.putIfAbsent(columnRef.aliasName, new HashSet<>());
//                    joinTableToAttributeIdx.get(columnRef.aliasName).add(aliasCtr);
//                }
//            }
//
//            // construct dependency set
//            Map<Integer, Set<Integer>> dependencySet = new HashMap<>();
//            for (int aliasCtr = nrVars - 1; aliasCtr > 0; --aliasCtr) {
//                Set<Integer> dependency = new HashSet<>();
//                for (ColumnRef columnRef : varOrder.get(aliasCtr)) {
//                    Set<Integer> attributeIdx = joinTableToAttributeIdx.get(columnRef.aliasName);
//                    dependency.addAll(attributeIdx);
//                }
//                if (aliasCtr < nrVars - 1) {
//                    dependency.addAll(dependencySet.get(aliasCtr + 1));
//                }
//                // remove key in front of this key
//                for (int i = nrVars - 1; i >= aliasCtr; --i) {
//                    dependency.remove(i);
//                }
//                dependencySet.put(aliasCtr, dependency);
//            }
//
//            // collect which attribute to cache
//            for (int aliasCtr = 1; aliasCtr < nrVars; aliasCtr++) {
//                // dependency
//                Set<Integer> dependency = dependencySet.get(aliasCtr);
//                if (dependency.size() < aliasCtr) {
//                    // validate cache key and value
//                    Set<Integer> attributeLater = new HashSet<>();
//                    for (int i = aliasCtr; i < nrVars; i++) {
//                        attributeLater.add(i);
//                    }
//
//                    // map the local order to the global order
//                    // dependency: example, key: 1,3, value: 2, 4
//                    Set<Integer> globalKey = dependency.stream().map(key -> order[key]).collect(Collectors.toSet());
//                    Set<Integer> globalValue = attributeLater.stream().map(key -> order[key]).collect(Collectors.toSet());
//
////                    System.out.println("key+++++");
//                    for (int k : globalKey) {
//                        System.out.println(query.equiJoinAttribute.get(k));
//                    }
////                    System.out.println("value+++++");
//                    for (int v : globalValue) {
//                        System.out.println(query.equiJoinAttribute.get(v));
//                    }
////                    System.out.println("+++++");
////                    cacheAttributes.put(globalKey, attributeLater);
//
//                }
//            }
//
//            // finish cache
////            System.out.println("joinTableToAttributeIdx:" + joinTableToAttributeIdx);
//        }

        this.manager = manager;
        this.attributeValueBound = Arrays.stream(order).mapToObj(attributeValueBound::get).collect(Collectors.toList());
    }



    /**
     * Resumes join operation for a fixed number of steps.
     *
     * @param budget how many iterations are allowed
     */
    double resumeJoin(long budget) throws Exception {
        // check available budget
        System.out.println("attribute value bound:" + attributeValueBound);
        double reward = 0;
        List<Hypercube> selectCubes = manager.allocateNHypercube(JoinConfig.NTHREAD);
        if (selectCubes.size() == 0) {
            finished = true;
            return 0;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(JoinConfig.NTHREAD);
        List<Future<HyperCubeEvaluationResult>> evaluateResults = new ArrayList<>();
        for (int i = 0; i < selectCubes.size(); i++) {
            Hypercube selectCube = selectCubes.get(i);
            evaluateResults.add(executorService.submit(new HyperCubeEvaluationTask(budget, selectCube, attributeOrder, idToIter, itersNumberByVar)));
        }
        for (Future<HyperCubeEvaluationResult> futureResult : evaluateResults) {
            HyperCubeEvaluationResult result = futureResult.get();
            boolean isFinish = result.isFinish;
            Hypercube selectCube = result.selectCube;
            System.out.println("isFinish:" + isFinish);
            if (isFinish) {
                reward += selectCube.getVolume() / manager.totalVolume;
                manager.finishHyperCube(selectCube);
                MultiWayJoin.result.merge(result.joinResult);
            } else {
                double processVolume = manager.updateInterval(selectCube, result.endValues, attributeOrder);
                reward += processVolume / manager.totalVolume;
                MultiWayJoin.result.merge(result.joinResult);
            }
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return reward;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }
}