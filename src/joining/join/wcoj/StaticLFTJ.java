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
    /**
     * Percentage of work done for first
     * attribute in attribute order (used
     * for reward calculations).
     */
    public double firstCovered = -1;

    final int[] attributeOrder;

//    final public List<List<Integer>> attributesCardinality;

//    public static long t1 = 0;
//
//    public static long t2 = 0;
//
//    public static long t3 = 0;
//
//    public static long t4 = 0;

    /**
     * Initialize join for given query.
     *
     * @param query            join query to process via LFTJ
     * @param executionContext summarizes procesing context
     * @throws Exception
     */
    public StaticLFTJ(QueryInfo query, Context executionContext,
                      JoinResult joinResult, int[] order) throws Exception {
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
//        long stime5 = System.currentTimeMillis();

//        t1 += (stime3 - stime2);
//        t2 += (stime4 - stime3);
//        t3 += (stime5 - stime4);
//        System.out.println("t1:" + t1);
//        System.out.println("t2:" + t2);
//        System.out.println("t3:" + t3);
    }

    /**
     * Initializes iterators and checks for
     * quick termination.
     *
     * @param iters iterators for current attribute
     * @return true if join continues
     * @throws Exception
     */
    boolean leapfrogInit(List<LFTJiter> iters) throws Exception {
        // Advance to next trie level (iterators are
        // initially positioned before first trie level).
        for (LFTJiter iter : iters) {
            iter.open();
        }
        // Check for early termination
        for (LFTJiter iter : iters) {
            if (iter.atEnd()) {
                return false;
            }
        }
        // Sort iterators by their keys
        Collections.sort(iters, new Comparator<LFTJiter>() {
            @Override
            public int compare(LFTJiter o1, LFTJiter o2) {
                return Integer.compare(o1.key(), o2.key());
            }
        });
        // Must continue with join
        return true;
    }

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
			/*
			else {
				System.out.println(
						"Equality satisfied: " +
						equiPair.toString());
			}
			*/
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

    double rewardTuple(List<List<Integer>> attributesIndexStart) {
        // end of join, get the progress of each iteration
        Map<LFTJiter, Integer> tableTrieLevel = new HashMap<>();
        List<List<Integer>> attributesIndexEnd = new ArrayList<>();
//        List<List<Integer>> attributesIndexOffset = new ArrayList<>();
        List<List<Integer>> attributesCardinality = new ArrayList<>();
        for (int attributeId = 0; attributeId < nrVars; attributeId++) {
            List<LFTJiter> curIters = itersByVar.get(attributeId);
            List<Integer> attributeIndexEnd = new ArrayList<>();
//            List<Integer> attributeIndexOffset = new ArrayList<>();
            List<Integer> attributeCardinality = new ArrayList<>();
            for (int iterId = 0; iterId < curIters.size(); iterId++) {
                LFTJiter curIter = curIters.get(iterId);
                int trieLevel = tableTrieLevel.getOrDefault(curIter, 0);
                attributeIndexEnd.add(curIter.curTuples[trieLevel]);
//                attributeIndexOffset.add(curIter.card - curIter.curTuples[trieLevel]);
                attributeCardinality.add(curIter.card);
                tableTrieLevel.put(curIter, trieLevel + 1);
            }
            attributesIndexEnd.add(attributeIndexEnd);
//            attributesIndexOffset.add(attributeIndexOffset);
            attributesCardinality.add(attributeCardinality);
        }
//        System.out.println("attributesIndexStart:" + attributesIndexStart);
//        System.out.println("attributesIndexEnd:" + attributesIndexEnd);
//        System.out.println("attributesCardinality:" + attributesCardinality);
        // calculate reward based on the offset and delta tuples
//        double scaledReward = 1;
//        for (int i = 0; i < attributesCardinality.get(0).size(); i++) {
//            double deltaReward = ((double) (attributesIndexEnd.get(0).get(i)) - attributesIndexStart.get(0).get(i)) / ((double) (attributesCardinality.get(0).get(i)) - attributesIndexEnd.get(0).get(i));
//            scaledReward = Double.min(deltaReward, scaledReward);
//        }
//        return scaledReward;

        double scaledReward = 0;
        double scale = 1;
        for (int i = 0; i < attributesCardinality.size(); i++) {
            int nrAttribute = attributesCardinality.get(i).size();
            List<Double> deltaRewards = new ArrayList<>();
            double totalReward = 0;
            for (int j = 0; j < nrAttribute; j++) {
                double deltaReward = ((double) (attributesIndexEnd.get(i).get(j)) - attributesIndexStart.get(i).get(j)) / ((double) (attributesCardinality.get(i).get(j)) - attributesIndexEnd.get(i).get(j) + 1);
                deltaRewards.add(deltaReward);
                totalReward += deltaReward;
            }
            double reward = Collections.min(deltaRewards);
            if (i > 0) {
                scale *= attributesCardinality.get(i).get(deltaRewards.indexOf(reward));
            }
            scaledReward += totalReward / scale;
        }
        scaledReward = (scaledReward >  0) ? scaledReward : 0;
        return scaledReward;
    }

    double rewardValue(List<List<Integer>> attributesValueStart) {
        // end of join, get the progress of each iteration
        Map<LFTJiter, Integer> tableTrieLevel = new HashMap<>();
        List<List<Integer>> attributesValueEnd = new ArrayList<>();
//        List<List<Integer>> attributesValueLower = new ArrayList<>();
        List<List<Integer>> attributesValueUpper = new ArrayList<>();
        for (int attributeId = 0; attributeId < nrVars; attributeId++) {
            List<LFTJiter> curIters = itersByVar.get(attributeId);
            List<Integer> attributeValueEnd = new ArrayList<>();
            List<Integer> attributeValueUpper = new ArrayList<>();
//            List<Integer> attributeValueLower = new ArrayList<>();
            for (int iterId = 0; iterId < curIters.size(); iterId++) {
                LFTJiter curIter = curIters.get(iterId);
                int trieLevel = tableTrieLevel.getOrDefault(curIter, 0);
                attributeValueEnd.add(curIter.keyAtLevel(trieLevel));
//                attributeValueLower.add(curIter.minValueAtLevel(trieLevel));
                attributeValueUpper.add(curIter.maxValueAtLevel(trieLevel));
//                System.out.println("attributeId, iterId:" + attributeId + "," + iterId);
//                System.out.println("current start:" + attributesValueStart.get(attributeId).get(iterId));
//                System.out.println("current current:" + curIter.keyAtLevel(trieLevel));
//                System.out.println("max value:" + curIter.maxValueAtLevel(trieLevel));
                tableTrieLevel.put(curIter, trieLevel + 1);
            }
            attributesValueEnd.add(attributeValueEnd);
//            attributesValueLower.add(attributeValueLower);
            attributesValueUpper.add(attributeValueUpper);
        }
        System.out.println("attributesValueStart:" + attributesValueStart);
        System.out.println("attributesValueEnd:" + attributesValueEnd);
//        System.out.println("attributesValueLower:" + attributesValueLower);
        System.out.println("attributesValueUpper:" + attributesValueUpper);
        // calculate reward based on the offset and delta tuples
//        double scaledReward = 1;
//        for (int i = 0; i < attributesValueDelta.get(0).size(); i++) {
//            System.out.println("attributesValueOffset.get(0).get(i)" + attributesValueOffset.get(0).get(i));
//            double deltaReward = ((double) (attributesValueDelta.get(0).get(i))) / ( attributesValueOffset.get(0).get(i) + 1);
//            scaledReward *= deltaReward;
//        }
//        return scaledReward;

//        double scaledReward = Double.MAX_VALUE;
//        for (int i = 0; i < attributesValueDelta.get(0).size(); i++) {
//            System.out.println("attributesValueOffset.get(0).get(i)" + attributesValueOffset.get(0).get(i));
//            double deltaReward = ((double) (attributesValueDelta.get(0).get(i))) / ( attributesValueOffset.get(0).get(i) + 1);
//            scaledReward = Double.min(deltaReward, scaledReward);
//        }

//        double scaledReward = 1;
//        List<Integer> curProgress = new ArrayList<>();
//        List<Integer> maxProgress = new ArrayList<>();
//        for (int i = 0; i < 1; i++) {
//            double minStart = Collections.min(attributesValueStart.get(i));
//            double minEnd = Collections.min(attributesValueEnd.get(i));
//            double upperBound = Collections.min(attributesValueUpper.get(i));
//            scaledReward = (minEnd - minStart) / (upperBound - minEnd + 1);
//        }
//        return scaledReward;

        double scaledReward = 0;
//        List<Integer> curProgress = new ArrayList<>();
//        List<Integer> maxProgress = new ArrayList<>();
        double scale = 1;
        for (int i = 0; i < attributesValueStart.size(); i++) {
            double minStart = Collections.min(attributesValueStart.get(i));
            double minEnd = Collections.min(attributesValueEnd.get(i));
            double upperBound = Collections.min(attributesValueUpper.get(i));
//            double lowerBound = Collections.min(attributesValueLower.get(i));
            if (i > 0) {
                scale *= upperBound;
            }
            System.out.println("progress:" + (minEnd - minStart));
            System.out.println("scale:" + scale);
            scaledReward += (minEnd - minStart) / ((upperBound - minEnd + 1) * scale);
        }
        return scaledReward;

    }

    /**
     * Resumes join operation for a fixed number of steps.
     *
     * @param budget how many iterations are allowed
     * @throws Exception
     */
    double resumeJoin(long budget, StateLFTJ state) throws Exception {

//        System.out.println("isAhead:" + state.isAhead);
        // Do we freshly resume after being suspended?
        boolean afterSuspension = (roundCtr > 0);
        boolean init = false;
//        boolean[] startAttribute = new boolean[nrVars];
//        for (int i = 0; i < nrVars; i++) {
//            int tupleValue = state.tupleValues[i];
//            afterSuspension &= (tupleValue > 0);
////            startAttribute[i] = (tupleValue > 0);
//        }

        afterSuspension &= state.isAhead;

        // Initialize state and flags to prepare budgeted execution
        if (state.lastIndex >= 0 && !afterSuspension) {
            // if we share progress from another order
            // init the max key of join
            curVariableID = state.lastIndex;
//            System.out.println("curVariableID:" + curVariableID);
//            for (int attrCtr = 0; attrCtr < curVariableID; ++attrCtr) {
//                joinFrames.get(attrCtr).maxKey = state.tupleValues[attributeOrder[attrCtr]];
//                // move the join key to
//            }

//            System.out.println("reuse here");
            // move the LFTJiter to corresponding place
            for (LFTJiter curIters : idToIter) {
                curIters.reset();
            }

//            for (LFTJiter curIter : itersByVar.get(0)) {
//                int prevKey = state.tupleValues[attributeOrder[0]];
//                curIter.seek(prevKey);
//            }

            for (int i = 0; i <= curVariableID; i++) {
                int prevKey = state.tupleValues[attributeOrder[i]];
                List<LFTJiter> curIters = itersByVar.get(i);
                for (LFTJiter curIter : curIters) {
                    curIter.open();
                    curIter.seek(prevKey);
                    if (curIter.atEnd()) {
                        curIter.backward();
                    }
                }
                Collections.sort(curIters, new Comparator<LFTJiter>() {
                    @Override
                    public int compare(LFTJiter o1, LFTJiter o2) {
                        return Integer.compare(o1.key(), o2.key());
                    }
                });
                joinFrames.get(i).p = 0;
                joinFrames.get(i).curIters = curIters;
                joinFrames.get(i).nrCurIters = curIters.size();
                joinFrames.get(i).maxKey = curIters.get(curIters.size() - 1).key();
                joinFrames.get(i).maxIterPos = curIters.size() - 1;
            }

            init = true;

            // reset depth based on curVariableID value
            //        for (LFTJiter curIters : idToIter) {
            //            curIters.reset();
            //            curIters.open();
            //        }

//            Map<LFTJiter, Integer> tableTrieLevel = new HashMap<>();
//            for (int j = 0; j <= curVariableID; j++) {
//                List<LFTJiter> curIters = itersByVar.get(j);
//                for (LFTJiter curIter : curIters) {
//                    int trieLevel = tableTrieLevel.getOrDefault(curIter, -1);
//                    curIter.curTrieLevel = trieLevel;
//                    tableTrieLevel.put(curIter, trieLevel + 1);
//                    System.out.println("trieLevel:" + trieLevel);
//                }
//                System.out.println("=======");
//            }
        }

        // start position of each table iterate
        List<List<Integer>> attributesIndexStart = new ArrayList<>();
        List<List<Integer>> attributesValuesStart = new ArrayList<>();
        Map<LFTJiter, Integer> tableTrieLevel = new HashMap<>();
        for (List<LFTJiter> curIters : itersByVar) {
            List<Integer> attributeIndexStart = new ArrayList<>();
            List<Integer> attributeValueStart = new ArrayList<>();
            for (LFTJiter curIter : curIters) {
                int trieLevel = tableTrieLevel.getOrDefault(curIter, 0);
                attributeIndexStart.add(curIter.curTuples[trieLevel]);
                attributeValueStart.add(curIter.keyAtLevel(trieLevel));
                tableTrieLevel.put(curIter, trieLevel + 1);
            }
            attributesIndexStart.add(attributeIndexStart);
            attributesValuesStart.add(attributeValueStart);
        }

        // Initialize reward-related statistics
        lastNrResults = 0;

        // We had at least one iteration
        roundCtr += 1;
        // Until we finish processing (break)
        while (true) {
            // Did we finish processing?
            if (curVariableID < 0) {
//                System.out.println("end");
                finished = true;
                break;
            }
            JoinFrame joinFrame = curVariableID >= nrVars ?
                    null : joinFrames.get(curVariableID);
            // Go directly to point of interrupt?
            if (afterSuspension) {
//                System.out.println("afterSuspension: afterSuspension");
                afterSuspension = false;
            } else {
                if (backtracked) {
                    backtracked = false;
                    LFTJiter minIter = joinFrame.curIters.get(joinFrame.p);
                    minIter.seek(joinFrame.maxKey + 1);
                    if (minIter.atEnd()) {
                        // Go one level up in each trie
                        for (LFTJiter iter : joinFrame.curIters) {
                            iter.up();
                        }
                        backtrack();
                        continue;
                    }
                    joinFrame.maxKey = minIter.key();
                    joinFrame.p = (joinFrame.p + 1) % joinFrame.nrCurIters;
                } else {
                    // Have we completed a result tuple?
                    if (curVariableID >= nrVars) {
                        addResultTuple();
                        backtrack();
                        continue;
                    }
                    // Collect relevant iterators
                    joinFrame.curIters = itersByVar.get(curVariableID);
                    joinFrame.nrCurIters = joinFrame.curIters.size();

                    if(init){
//                        System.out.println("init:");
                        init = false;
                    } else {
                        // Order iterators and check for early termination
                        if (!leapfrogInit(joinFrame.curIters)) {
                            // Go one level up in each trie
                            for (LFTJiter iter : joinFrame.curIters) {
                                iter.up();
                            }
                            backtrack();
                            continue;
                        }
                    }
//                    System.out.println("curVariableID:" + curVariableID);
                    // Execute search procedure
                    joinFrame.p = 0;
                    joinFrame.maxIterPos = (joinFrame.nrCurIters + joinFrame.p - 1) % joinFrame.nrCurIters;
                    joinFrame.maxKey = joinFrame.curIters.get(joinFrame.maxIterPos).key();
//                    System.out.println("here");
//                    if (!startAttribute[curVariableID]) {
//                        joinFrame.maxIterPos = (joinFrame.nrCurIters + joinFrame.p - 1) % joinFrame.nrCurIters;
//                        joinFrame.maxKey = joinFrame.curIters.get(joinFrame.maxIterPos).key();
//                        System.out.println("here");
//                    } else {
//                        startAttribute[curVariableID] = false;
//                        System.out.println("dddd");
//                    }
                }
            }
            while (true) {
                // Count current round
//                ++roundCtr;
                --budget;
                JoinStats.nrIterations++;
                // Check for timeout
                if (budget <= 0) {
                    // Save final state
                    state.lastIndex = curVariableID;
                    Arrays.fill(state.tupleValues, 0);
                    for (int attrCtr = 0; attrCtr <= curVariableID; ++attrCtr) {
                        int currentKey = joinFrames.get(attrCtr).maxKey;
                        if (currentKey > 0) {
                            state.tupleValues[attributeOrder[attrCtr]] = currentKey;
                        }

//                        JoinFrame currentFrame = joinFrames.get(attrCtr);
//                        if (currentFrame.p >= 0) {
//                            int currentKey = currentFrame.curIters.get(currentFrame.p).key();
//                            if (currentKey > 0) {
//                                state.tupleValues[attributeOrder[attrCtr]] = currentKey;
//                            }
//                        }

//                        int currentKey = joinFrames.get(attrCtr).maxKey;
//                        if (currentKey > 0) {
//                            state.tupleValues[attributeOrder[attrCtr]] = currentKey;
//                        }
                    }
//                    return rewardTuple(attributesIndexStart);
                    return rewardValue(attributesValuesStart);
                }
                // Get current key
                LFTJiter minIter = joinFrame.curIters.get(joinFrame.p);
                int minKey = minIter.key();
                // Generate debugging output
//                if (roundCtr < 10) {
//                    System.out.println("--- Current variable ID: " + curVariableID);
//                    System.out.println("p: " + joinFrame.p);
//                    System.out.println("minKey: " + minKey);
//                    System.out.println("maxKey: " + joinFrame.maxKey);
//                    for (LFTJiter iter : joinFrame.curIters) {
//                        System.out.println(iter.rid() + ":" + iter.key());
//                    }
//                }
                // Did we find a match between iterators?
                if (minKey == joinFrame.maxKey) {
                    advance();
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

        state.lastIndex = curVariableID;
        Arrays.fill(state.tupleValues, 0);
        for (int attrCtr = 0; attrCtr <= curVariableID; ++attrCtr) {
            int currentKey = joinFrames.get(attrCtr).maxKey;
            if (currentKey > 0) {
                state.tupleValues[attributeOrder[attrCtr]] = currentKey;
            }
        }
//        return rewardTuple(attributesIndexStart);
        return rewardValue(attributesValuesStart);
    }

    @Override
    public boolean isFinished() {
        return finished;
    }
}
