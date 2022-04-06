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
import util.Pair;

public class StaticLFTJ {
    /**
     * Contains at i-th position iterator over
     * i-th element in query from clause.
     */
    public final LFTJiter[] idToIter;
    /**
     * Order of variables (i.e., equivalence classes
     * of join attributes connected via equality
     * predicates).
     */
    final List<Set<ColumnRef>> varOrder;

    public final List<List<Integer>> itersNumberByVar;
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

    final int nrJoined;

    final int[] attributeOrder;

    List<Pair<Integer, Integer>> attributeValueBound;

    /**
     * Initialize join for given query.
     *
     * @param query            join query to process via LFTJ
     * @param executionContext summarizes procesing context
     * @throws Exception
     */
    public StaticLFTJ(QueryInfo query, Context executionContext, int[] order, List<Pair<Integer, Integer>> attributeValueBound) throws Exception {
        // Initialize query and context variables
//        super(query, executionContext);
        attributeOrder = order;
        varOrder = Arrays.stream(order).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList());
        nrVars = query.equiJoinClasses.size();
        nrJoined = query.nrJoined;
//        long startMillis1 = System.currentTimeMillis();
        // Initialize iterators
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

//        long startMillis2 = System.currentTimeMillis();
        // Group iterators by variable
        itersNumberByVar = new ArrayList<>();
        for (Set<ColumnRef> var : varOrder) {
            List<Integer> curNumberIters = new ArrayList<>();
            for (ColumnRef colRef : var) {
                String alias = colRef.aliasName;
                curNumberIters.add(aliasToNumber.get(alias));
            }
            itersNumberByVar.add(curNumberIters);
        }


//
//        part1 += (startMillis2 - startMillis1);
//        part2 += (startMillis3 - startMillis2);
//
//        System.out.println("static lftj 1:" + part1);
//        System.out.println("static lftj 2:" + part2);

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

        this.attributeValueBound = Arrays.stream(order).mapToObj(attributeValueBound::get).collect(Collectors.toList());
//        long startMillis3 = System.currentTimeMillis();
//        part1 += (startMillis2 - startMillis1);
//        part2 += (startMillis3 - startMillis2);
    }

//    /**
//     * Resumes join operation for a fixed number of steps.
//     *
//     * @param budget how many iterations are allowed
//     */
//    double resumeJoin(long budget) throws Exception {
//        // check available budget
//
//
//    }

    public boolean isFinished() {
        return finished;
    }
}