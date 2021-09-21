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

//    final public List<List<Integer>> attributesCardinality;

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
        varOrder = Arrays.stream(order).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList());
        nrVars = query.equiJoinClasses.size();
        System.out.println("Variable Order: " + varOrder);
//        attributesCardinality = new ArrayList();
        // Initialize iterators
        aliasToIter = new HashMap<>();
        idToIter = new LFTJiter[nrJoined];
        for (int aliasCtr = 0; aliasCtr < nrJoined; ++aliasCtr) {
            String alias = query.aliases[aliasCtr];
            LFTJiter iter = new LFTJiter(query,
                    executionContext, aliasCtr, varOrder);
            aliasToIter.put(alias, iter);
            idToIter[aliasCtr] = iter;
        }
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
        // Initialize stack for LFTJ algorithm
        curVariableID = 0;
        for (int varCtr = 0; varCtr < nrVars; ++varCtr) {
            JoinFrame joinFrame = new JoinFrame();
            joinFrames.add(joinFrame);
        }

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
                String table = preSummary.aliasToFiltered.get(alias);
//                String table = preSummary.aliasToDistinct.get(alias);
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

//    double reward(List<List<Integer>> attributesIndexStart) {
//        // end of join, get the progress of each iteration
//        Map<LFTJiter, Integer> tableTrieLevel = new HashMap<>();
//        List<List<Integer>> attributesIndexDelta = new ArrayList<>();
//        List<List<Integer>> attributesIndexOffset = new ArrayList<>();
//        List<List<Integer>> attributesCardinality = new ArrayList<>();
//        for (int attributeId = 0; attributeId < nrVars; attributeId++) {
//            List<LFTJiter> curIters = itersByVar.get(attributeId);
//            List<Integer> attributeIndexDelta = new ArrayList<>();
//            List<Integer> attributeIndexOffset = new ArrayList<>();
//            List<Integer> attributeCardinality = new ArrayList<>();
//            for (int iterId = 0; iterId < curIters.size(); iterId++) {
//                LFTJiter curIter = curIters.get(iterId);
//                int trieLevel = tableTrieLevel.getOrDefault(curIter, 0);
//                attributeIndexDelta.add(curIter.curTuples[trieLevel] - attributesIndexStart.get(attributeId).get(iterId));
//                attributeIndexOffset.add(curIter.card - curIter.curTuples[trieLevel]);
//                attributeCardinality.add(curIter.card);
//                tableTrieLevel.put(curIter, trieLevel + 1);
//            }
//            attributesIndexDelta.add(attributeIndexDelta);
//            attributesIndexOffset.add(attributeIndexOffset);
//            attributesCardinality.add(attributeCardinality);
//        }
//        System.out.println("attributesIndexDelta:" + attributesIndexDelta);
//        System.out.println("attributesIndexOffset:" + attributesIndexOffset);
//        System.out.println("attributesCardinality:" + attributesCardinality);
//        // calculate reward based on the offset and delta tuples
//        double scaledReward = 1;
//        for (int i = 0; i < attributesCardinality.get(0).size(); i++) {
//            double deltaReward = ((double) (attributesIndexDelta.get(0).get(i))) / attributesIndexOffset.get(0).get(i);
//            scaledReward *= deltaReward;
//        }
//        return scaledReward;
//    }

    double reward(List<List<Integer>> attributesValueStart) {
        // end of join, get the progress of each iteration
        Map<LFTJiter, Integer> tableTrieLevel = new HashMap<>();
        List<List<Integer>> attributesValueDelta = new ArrayList<>();
        List<List<Integer>> attributesValueOffset = new ArrayList<>();
        for (int attributeId = 0; attributeId < nrVars; attributeId++) {
            List<LFTJiter> curIters = itersByVar.get(attributeId);
            List<Integer> attributeValueDelta = new ArrayList<>();
            List<Integer> attributeValueOffset = new ArrayList<>();
            for (int iterId = 0; iterId < curIters.size(); iterId++) {
                LFTJiter curIter = curIters.get(iterId);
                int trieLevel = tableTrieLevel.getOrDefault(curIter, 0);
                attributeValueDelta.add(curIter.keyAtLevel(trieLevel) - attributesValueStart.get(attributeId).get(iterId));
                attributeValueOffset.add(curIter.maxValueAtLevel(trieLevel) - curIter.keyAtLevel(trieLevel));
                System.out.println("attributeId, iterId:" + attributeId + "," + iterId);
                System.out.println("current start:" + attributesValueStart.get(attributeId).get(iterId));
                System.out.println("current current:" + curIter.keyAtLevel(trieLevel));
                System.out.println("max value:" + curIter.maxValueAtLevel(trieLevel));
                tableTrieLevel.put(curIter, trieLevel + 1);
            }
            attributesValueDelta.add(attributeValueDelta);
            attributesValueOffset.add(attributeValueOffset);
        }
        System.out.println("attributesValueDelta:" + attributesValueDelta);
        System.out.println("attributesValueOffset:" + attributesValueOffset);
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

        double scaledReward = Double.MAX_VALUE;
        List<Integer> curProgress = new ArrayList<>();
        List<Integer> maxProgress = new ArrayList<>();
        for (int i = 0; i < attributesValueDelta.size(); i++) {
            curProgress.add(Collections.min(attributesValueDelta.get(i)));
            maxProgress.add(Collections.min(attributesValueOffset.get(i)));
//            System.out.println("attributesValueOffset.get(0).get(i)" + attributesValueOffset.get(0).get(i));
//            double deltaReward = ((double) (attributesValueDelta.get(0).get(i))) / ( attributesValueOffset.get(0).get(i) + 1);
//            scaledReward = Double.min(deltaReward, scaledReward);
        }

        return scaledReward;
    }

    /**
     * Resumes join operation for a fixed number of steps.
     *
     * @param budget how many iterations are allowed
     * @throws Exception
     */
    double resumeJoin(long budget) throws Exception {
        // start position of each table iterate
        List<List<Integer>> attributesIndexStart = new ArrayList<>();
        List<List<Integer>> attributesValuesStart = new ArrayList<>();
        Map<LFTJiter, Integer> tableTrieLevel = new HashMap<>();
        for (List<LFTJiter> curIters: itersByVar) {
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
        // Do we freshly resume after being suspended?
        boolean afterSuspension = roundCtr > 0;
        // We had at least one iteration
        roundCtr += 1;
        // Until we finish processing (break)
        while (true) {
            // Did we finish processing?
            if (curVariableID < 0) {
                finished = true;
                break;
            }
            JoinFrame joinFrame = curVariableID >= nrVars ?
                    null : joinFrames.get(curVariableID);
            // Go directly to point of interrupt?
            if (afterSuspension) {
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
                    // Order iterators and check for early termination
                    if (!leapfrogInit(joinFrame.curIters)) {
                        // Go one level up in each trie
                        for (LFTJiter iter : joinFrame.curIters) {
                            iter.up();
                        }
                        backtrack();
                        continue;
                    }
                    // Execute search procedure
                    joinFrame.p = 0;
                    joinFrame.maxIterPos = (joinFrame.nrCurIters + joinFrame.p - 1) % joinFrame.nrCurIters;
                    joinFrame.maxKey = joinFrame.curIters.get(joinFrame.maxIterPos).key();
                }
            }
            while (true) {
                // Count current round
                ++roundCtr;
                --budget;
                JoinStats.nrIterations++;
                // Check for timeout
                if (budget <= 0) {
                    return reward(attributesValuesStart);
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

        return reward(attributesValuesStart);
    }

    @Override
    public boolean isFinished() {
        return finished;
    }
}
