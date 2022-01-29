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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import buffer.BufferManager;
import config.CheckConfig;
import data.ColumnData;
import data.IntData;
import joining.join.MultiWayJoin;
import joining.result.JoinResult;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import util.Pair;

/**
 * An LFTJ operator with a fixed join order
 * that is chosen at initialization. Allows
 * to resume execution for single time slices.
 *
 * @author immanueltrummer
 */
public class StaticLFTJ extends MultiWayJoin {
    /**
     * Maps alias IDs to corresponding iterator.
     */
    final Map<String, LFTJiter> aliasToIter;
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
    /**
     * Bookkeeping information associated
     * with attributes (needed to resume join).
     */
    List<JoinFrame> joinFrames = new ArrayList<>();
    /**
     * Index of current variable in attribute order.
     */
    int curVariableID = -1;
    /**
     * Whether we backtracked in the last iteration
     * of the main loop (requires certain actions
     * at the beginning of loop).
     */
    boolean backtracked = false;
    /**
     * Number of result tuples produced
     * in last invocation (used for reward
     * calculations).
     */
    public int lastNrResults = -1;

    final int[] attributeOrder;

    boolean isCache = true;

    Map<String, Set<Integer>> joinTableToAttributeIdx;

//    Map<CacheAttribute, Set<Integer>> cacheAttributes;

    HypercubeManager manager;

    List<Pair<Integer, Integer>> attributeValueBound;

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
        aliasToIter = new HashMap<>();
        idToIter = new LFTJiter[nrJoined];
        for (int aliasCtr = 0; aliasCtr < nrJoined; ++aliasCtr) {
            String alias = query.aliases[aliasCtr];
            LFTJiter iter = new LFTJiter(query,
                    executionContext, aliasCtr, varOrder);
            aliasToIter.put(alias, iter);
            idToIter[aliasCtr] = iter;
        }
//        long stime3 = System.currentTimeMillis();
        // Group iterators by variable
        itersByVar = new ArrayList<>();
        for (Set<ColumnRef> var : varOrder) {
            List<LFTJiter> curVarIters = new ArrayList<>();
//            List<Integer> attributeCardinality = new ArrayList<>();
            for (ColumnRef colRef : var) {
                String alias = colRef.aliasName;
                LFTJiter iter = aliasToIter.get(alias);
                curVarIters.add(iter);
//                attributeCardinality.add(iter.card);
            }
            itersByVar.add(curVarIters);
//            attributesCardinality.add(attributeCardinality);
        }
//        long stime4 = System.currentTimeMillis();

        // Initialize stack for LFTJ algorithm
        curVariableID = 0;
        for (int varCtr = 0; varCtr < nrVars; ++varCtr) {
            JoinFrame joinFrame = new JoinFrame();
            joinFrames.add(joinFrame);
        }

        if (isCache) {

            // for cache
            // gather table in each attribute
            joinTableToAttributeIdx = new HashMap<>();
//            cacheAttributes = new HashMap<Set<Integer>, Set<Integer>>();
            for (int aliasCtr = 0; aliasCtr < nrVars; ++aliasCtr) {
                for (ColumnRef columnRef : varOrder.get(aliasCtr)) {
                    joinTableToAttributeIdx.putIfAbsent(columnRef.aliasName, new HashSet<>());
                    joinTableToAttributeIdx.get(columnRef.aliasName).add(aliasCtr);
                }
            }

            // construct dependency set
            Map<Integer, Set<Integer>> dependencySet = new HashMap<>();
            for (int aliasCtr = nrVars - 1; aliasCtr > 0; --aliasCtr) {
                Set<Integer> dependency = new HashSet<>();
                for (ColumnRef columnRef : varOrder.get(aliasCtr)) {
                    Set<Integer> attributeIdx = joinTableToAttributeIdx.get(columnRef.aliasName);
                    dependency.addAll(attributeIdx);
                }
                if (aliasCtr < nrVars - 1) {
                    dependency.addAll(dependencySet.get(aliasCtr + 1));
                }
                // remove key in front of this key
                for (int i = nrVars - 1; i >= aliasCtr; --i) {
                    dependency.remove(i);
                }
                dependencySet.put(aliasCtr, dependency);
            }

            // collect which attribute to cache
            for (int aliasCtr = 1; aliasCtr < nrVars; aliasCtr++) {
                // dependency
                Set<Integer> dependency = dependencySet.get(aliasCtr);
                if (dependency.size() < aliasCtr) {
                    // validate cache key and value
                    Set<Integer> attributeLater = new HashSet<>();
                    for (int i = aliasCtr; i < nrVars; i++) {
                        attributeLater.add(i);
                    }

                    // map the local order to the global order
                    // dependency: example, key: 1,3, value: 2, 4
                    Set<Integer> globalKey = dependency.stream().map(key -> order[key]).collect(Collectors.toSet());
                    Set<Integer> globalValue = attributeLater.stream().map(key -> order[key]).collect(Collectors.toSet());

//                    System.out.println("key+++++");
                    for (int k : globalKey) {
                        System.out.println(query.equiJoinAttribute.get(k));
                    }
//                    System.out.println("value+++++");
                    for (int v : globalValue) {
                        System.out.println(query.equiJoinAttribute.get(v));
                    }
//                    System.out.println("+++++");
//                    cacheAttributes.put(globalKey, attributeLater);

                }
            }

            // finish cache
//            System.out.println("joinTableToAttributeIdx:" + joinTableToAttributeIdx);
        }

        this.manager = manager;
        this.attributeValueBound = Arrays.stream(order).mapToObj(attributeValueBound::get).collect(Collectors.toList());
    }

//    /**
//     * Initializes iterators and checks for
//     * quick termination.
//     *
//     * @param iters iterators for current attribute
//     * @return true if join continues
//     * @throws Exception
//     */
//    boolean leapfrogInit(List<LFTJiter> iters) throws Exception {
//        // Advance to next trie level (iterators are
//        // initially positioned before first trie level).
//        for (LFTJiter iter : iters) {
//            iter.open();
//        }
//        // Check for early termination
//        for (LFTJiter iter : iters) {
//            if (iter.atEnd()) {
//                return false;
//            }
//        }
//        // Sort iterators by their keys
//        Collections.sort(iters, new Comparator<LFTJiter>() {
//            @Override
//            public int compare(LFTJiter o1, LFTJiter o2) {
//                return Integer.compare(o1.key(), o2.key());
//            }
//        });
//        // Must continue with join
//        return true;
//    }

    /**
     * Add join result tuple based on current
     * iterator positions.
     */
    void addResultTuple() throws Exception {
        // Update reward-related statistics
        ++lastNrResults;
        // Generate result tuple
        int[] resultTuple = new int[nrJoined];
        // Iterate over all joined tables
        for (int aliasCtr = 0; aliasCtr < nrJoined; ++aliasCtr) {
            LFTJiter iter = idToIter[aliasCtr];
            resultTuple[aliasCtr] = iter.rid();
        }
//        System.out.println("addResultTuple:"+ Arrays.toString(resultTuple));
        // Add new result tuple
        result.add(resultTuple);
        // Verify result tuple if activated
        if (CheckConfig.CHECK_LFTJ_RESULTS) {
            if (!testResult(resultTuple)) {
                System.out.println(
                        "Error - inconsistent result tuple: "
                                + Arrays.toString(resultTuple));
            }
        }
    }

    /**
     * Returns true iff given result tuples satisfies
     * all binary join equality predicates.
     *
     * @param resultTuple check this result tuple
     * @return true iff tuple passes checks
     * @throws Exception
     */
    boolean testResult(int[] resultTuple) throws Exception {
        // Iterate over equality join conditions
        for (Set<ColumnRef> equiPair : query.equiJoinPairs) {
            Set<Integer> keyVals = new HashSet<>();
            // Iterate over columns in equality condition
            for (ColumnRef colRef : equiPair) {
                // Retrieve tuple index
                String alias = colRef.aliasName;
                int aliasIdx = query.aliasToIndex.get(alias);
                int tupleIdx = resultTuple[aliasIdx];
                // Retrieve corresponding data
                String table = preSummary.aliasToDistinct.get(alias);
//                String table = preSummary.aliasToFiltered.get(alias);
                String column = colRef.columnName;
                ColumnRef baseRef = new ColumnRef(table, column);
                ColumnData data = BufferManager.getData(baseRef);
                IntData intData = (IntData) data;
                int key = intData.data[tupleIdx];
                keyVals.add(key);
            }
            // Check whether key values collapse
            if (keyVals.size() > 1) {
                System.out.println(
                        "Equality not satisfied: " +
                                equiPair.toString());
                return false;
            }
        }
        // No inconsistencies were found - passed check
        return true;
    }

    /**
     * Advance to next variable in join order.
     */
    void advance() {
        curVariableID++;
    }

    /**
     * Backtrack to previous variable in join order.
     */
    void backtrack() {
        curVariableID--;
        backtracked = true;
    }

    /**
     * Resumes join operation for a fixed number of steps.
     *
     * @param budget how many iterations are allowed
     * @throws Exception
     */
    double resumeJoin(long budget) throws Exception {
        // check available budget
        double reward = 0;
        while (budget > 0) {
            // if we have enough budget
            Hypercube selectCube = manager.allocateHypercube();
            if (selectCube == null) {
                // here we finish the execution
                finished = true;
                return 0;
            }
            // swap the dimension of hypercube according to the current order
            // exploreDomain is the domain for current order
            List<Pair<Integer, Integer>> exploreDomain = selectCube.unfoldCube(attributeOrder);
            System.out.println("select hypercube:" + selectCube);
            System.out.println("select explore domain:" + exploreDomain);
            System.out.println("attribute value bound:" + attributeValueBound);

            // step one: reset the iterator
            for (LFTJiter curIters : idToIter) {
                curIters.reset();
            }

            // set the start position of LFTJ
            curVariableID = 0;
            // step two: move iterator to start of unexplored part
            // case 1: [10], [10], [1, 100]
            // case 2: [1, 100], [10], [10]
            for (int i = 0; i < attributeOrder.length; i++) {
                int startKey = exploreDomain.get(i).getFirst();
                int lowerBound = attributeValueBound.get(i).getFirst();
                System.out.println("startKey:" + startKey + ", lowerBound:" + lowerBound);
                // if the start key is not the lowest value of that attribute, set the start position
                if (startKey > lowerBound) {
                    List<LFTJiter> curIters = itersByVar.get(i);
                    for (LFTJiter curIter : curIters) {
                        curIter.open();
                        curIter.seek(startKey);
                    }
                    // sort the LFTJ iterator
                    Collections.sort(curIters, new Comparator<LFTJiter>() {
                        @Override
                        public int compare(LFTJiter o1, LFTJiter o2) {
                            return Integer.compare(o1.key(), o2.key());
                        }
                    });
                    // init join frames
                    joinFrames.get(i).p = 0;
                    joinFrames.get(i).curIters = curIters;
                    joinFrames.get(i).nrCurIters = curIters.size();
                    joinFrames.get(i).maxKey = curIters.get(curIters.size() - 1).key();
                    joinFrames.get(i).maxIterPos = curIters.size() - 1;
                    curVariableID++;
                } else {
                    break;
                }
            }

            // step three, start the join
            // Initialize reward-related statistics
            lastNrResults = 0;

            // We had at least one iteration
            roundCtr += 1;
            // Until we finish processing (break)
            while (true) {

                // Did we finish processing?
                if (curVariableID < 0) {
                    // finish the current hypercube
                    reward += selectCube.getVolume() / manager.totalVolume;
                    break;
                }

                // current position
                JoinFrame joinFrame = curVariableID >= nrVars ?
                        null : joinFrames.get(curVariableID);

                // Go directly to point of interrupt?
                if (backtracked) {
                    // if it is backtracked
                    backtracked = false;
                    LFTJiter minIter = joinFrame.curIters.get(joinFrame.p);
                    minIter.seek(joinFrame.maxKey + 1);
                    // Check for early termination
                    // if iterator reach to the end of select hypercube
                    if (minIter.atEnd() || minIter.key() > exploreDomain.get(curVariableID).getSecond()) {
                        // Go one level up in each trie
                        for (LFTJiter iter : joinFrame.curIters) {
                            iter.up();
                        }
                        backtrack();
                        continue;
                    }
                    // does not reach to the end of select hypercube
                    joinFrame.maxKey = minIter.key();
                    joinFrame.p = (joinFrame.p + 1) % joinFrame.nrCurIters;

                } else {
                    // go to next level
                    // Have we completed a result tuple?
                    if (curVariableID >= nrVars) {
                        addResultTuple();
                        backtrack();
                        continue;
                    }

                    // Collect relevant iterators
                    joinFrame.curIters = itersByVar.get(curVariableID);
                    joinFrame.nrCurIters = joinFrame.curIters.size();

                    List<LFTJiter> iters = joinFrame.curIters;

                    int startKey = exploreDomain.get(curVariableID).getFirst();
                    int endKey = exploreDomain.get(curVariableID).getSecond();
                    // open lftj iterator
                    for (LFTJiter iter : iters) {
                        iter.open();
                        iter.seek(startKey);
                    }

                    // Check for early termination
                    boolean reachEnd = false;
                    for (LFTJiter iter : iters) {
                        if (iter.atEnd() || iter.key() > endKey) {
                            reachEnd = true;
                            break;
                        }
                    }

                    if (reachEnd) {
                        for (LFTJiter iter : iters) {
                            iter.up();
                        }
                        backtrack();
                        // skip the execution join
                        continue;
                    } else {
                        // Sort iterators by their keys
                        Collections.sort(iters, new Comparator<LFTJiter>() {
                            @Override
                            public int compare(LFTJiter o1, LFTJiter o2) {
                                return Integer.compare(o1.key(), o2.key());
                            }
                        });

                        // Execute search procedure
                        joinFrame.p = 0;
                        joinFrame.maxIterPos = (joinFrame.nrCurIters + joinFrame.p - 1) % joinFrame.nrCurIters;
                        joinFrame.maxKey = joinFrame.curIters.get(joinFrame.maxIterPos).key();
                    }
                }

                // execute join
                while (true) {
                    // Count current round
                    --budget;
                    JoinStats.nrIterations++;
                    // Check for timeout and not in the last end
                    if (budget <= 0) {
                        // timeout, save final state
                        // hypercube
                        List<Integer> endValues = new ArrayList<>();
                        for (int i = 0; i < curVariableID; i++) {
                            int endValue = joinFrames.get(i).maxKey;
                            endValues.add(endValue);
                        }
                        // for position curVariableID
                        endValues.add(joinFrames.get(curVariableID).maxKey - 1);
                        for (int i = curVariableID + 1; i < nrVars; i++) {
                            endValues.add(attributeValueBound.get(i).getFirst());
                        }
                        System.out.println("end values:" + endValues);
                        // update interval
                        double processVolume = manager.updateInterval(selectCube, endValues, attributeOrder);
                        reward += processVolume / manager.totalVolume;
                        return reward;
                    }

                    // Get current key
                    LFTJiter minIter = joinFrame.curIters.get(joinFrame.p);
                    int minKey = minIter.key();

                    // Did we find a match between iterators?
                    if (minKey == joinFrame.maxKey) {
                        // test whether key are cached or not
                        advance();
                        // go to next level
                        break;
                    } else {
                        // min key not equal max key
                        minIter.seek(joinFrame.maxKey);
                        if (minIter.atEnd()) {
                            // Go one level up in each trie
                            for (LFTJiter iter : joinFrame.curIters) {
                                iter.up();
                            }
                            backtrack();
                            break;
                        } else {
                            // Min-iter to max-iter
                            joinFrame.maxKey = minIter.key();
                            joinFrame.p = (joinFrame.p + 1) % joinFrame.nrCurIters;
                        }
                    }
                }

            }
        }
        return 0;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }
}