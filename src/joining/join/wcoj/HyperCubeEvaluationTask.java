package joining.join.wcoj;

import data.IntData;
import joining.AggregateData;
import util.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class HyperCubeEvaluationTask {

    public final LFTJoin[] joins;

    private final List<Pair<Integer, Integer>> attributeValueBound;

    /**
     * Index of current variable in attribute order.
     */
    int curVariableID = 0;

    /**
     * Number of variables in input query (i.e.,
     * number of equivalence classes of join columns
     * connected via equality predicates).
     */
    final int nrVars;

    private final int nrJoined;

    /**
     * Bookkeeping information associated
     * with attributes (needed to resume join).
     */
    List<JoinFrame> joinFrames = new ArrayList<>();

    /**
     * Contains at i-th position the iterators
     * involved in obtaining keys for i-th
     * variable (consistent with global
     * variable order).
     */
    final List<List<LFTJoin>> joinsByVar;

    final AggregateData[] aggregateDatas;

    final Map<Integer, List<Integer>> aggregateInfo;

    /**
     * Whether we backtracked in the last iteration
     * of the main loop (requires certain actions
     * at the beginning of loop).
     */
    boolean backtracked = false;

    final int[] joinResult;

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

    public HyperCubeEvaluationTask(LFTJiter[] idToIter, List<List<Integer>> iterNumberByVar, List<Pair<Integer, Integer>> attributeValueBound, AggregateData[] aggregateDatas, Map<Integer, List<Integer>> aggregateInfo) {
        // for every table in from clause
        this.nrJoined = idToIter.length;
        this.joins = new LFTJoin[nrJoined];
        for (int i = 0; i < nrJoined; i++) {
            this.joins[i] = new LFTJoin(idToIter[i]);
        }
        // Initialize stack for LFTJ algorithm
        this.nrVars = iterNumberByVar.size();
        this.curVariableID = 0;
        for (int varCtr = 0; varCtr < nrVars; ++varCtr) {
            JoinFrame joinFrame = new JoinFrame();
            joinFrames.add(joinFrame);
        }
        this.joinsByVar = new ArrayList<>();
        for (List<Integer> iters : iterNumberByVar) {
            ArrayList<LFTJoin> joinByVar = new ArrayList<LFTJoin>();
            for (Integer iter : iters) {
                joinByVar.add(joins[iter]);
            }
            joinsByVar.add(joinByVar);
        }
        this.attributeValueBound = attributeValueBound;
        // Init aggregation columns
        this.aggregateDatas = aggregateDatas;
        this.aggregateInfo = aggregateInfo;
        this.joinResult = new int[this.aggregateDatas.length];
        Arrays.fill(this.joinResult, -1);
    }

    double rewardFirstValueScale(List<Integer> attributesValueStart, List<Integer> attributesValueEnd, List<Integer> hypercubeValueEnd) {
        double startInDim0 = attributesValueStart.get(0);
        double endInDim0 = attributesValueEnd.get(0);
        double scaledReward = (endInDim0 - startInDim0);
        scaledReward = scaledReward / (attributeValueBound.get(0).getSecond() - attributeValueBound.get(0).getFirst() + 1);
        for (int i = 1; i < hypercubeValueEnd.size(); i++) {
            double hypercubeRange = hypercubeValueEnd.get(i) - attributesValueStart.get(i) + 1;
            double dimRange = attributeValueBound.get(i).getSecond() - attributeValueBound.get(i).getFirst() + 1;
            scaledReward = scaledReward * (hypercubeRange / dimRange);
        }
        return scaledReward;
    }

    double rewardNrTuple(long budget, long nrProcessTuple) {
        return ((double) nrProcessTuple) / ((double) budget);
    }

    void getAggregateResult() {
        // Update reward-related statistics
        // Generate result tuple
        // Iterate over all joined tables
        for (int aliasCtr = 0; aliasCtr < nrJoined; ++aliasCtr) {
            LFTJoin iter = joins[aliasCtr];
            List<Integer> rids = iter.rids();
            if (aggregateInfo.containsKey(aliasCtr)) {
                for(int aggregateColumnCtr : aggregateInfo.get(aliasCtr)) {
                    IntData columnData = (IntData) this.aggregateDatas[aggregateColumnCtr].columnData;
                    for (int row : rids) {
                        int value = columnData.data[row];
                        if (joinResult[aggregateColumnCtr] != -1) {
                            if ((value < joinResult[aggregateColumnCtr] && aggregateDatas[aggregateColumnCtr].isMin) || (value > joinResult[aggregateColumnCtr] && !aggregateDatas[aggregateColumnCtr].isMin))
                                this.joinResult[aggregateColumnCtr] = value;
                        } else {
                            this.joinResult[aggregateColumnCtr] = value;
                        }
                    }
                }
            }
        }
    }

    public Pair<Double, int[]> execute(int budget, int[] attributeOrder, Hypercube selectCube) {

        List<Pair<Integer, Integer>> exploreDomain = selectCube.unfoldCube(attributeOrder);

        List<Integer> cubeStartValues = exploreDomain.stream().map(Pair::getFirst).collect(Collectors.toList());
        List<Integer> cubeEndValues = exploreDomain.stream().map(Pair::getSecond).collect(Collectors.toList());
//        int estimateBudget = budget;

        List<Integer>[] startTuplePosition = new ArrayList[nrVars];

        // step one: reset the iterator
        for (LFTJoin join : joins) {
            join.reset();
        }

        curVariableID = 0;
        // set the start position of LFTJ
        backtracked = false;
        // step two: move iterator to start of unexplored part
        // case 1: [10], [10], [1, 100]
        // case 2: [1, 100], [10], [10]

        // step three, start the join
        // Initialize reward-related statistics

        // Until we finish processing (break)
        // finish the current hypercube
        while (curVariableID >= 0) {

            // Did we finish processing?
            // current position
            JoinFrame joinFrame = curVariableID >= nrVars ?
                    null : joinFrames.get(curVariableID);

            // Go directly to point of interrupt?
            if (backtracked) {
                // if it is backtracked
                backtracked = false;
                LFTJoin minIter = joinFrame.curIters.get(joinFrame.p);
                minIter.seek(joinFrame.maxKey + 1);
//                budget -= minIter.seek(joinFrame.maxKey + 1);
                // Check for early termination
                // if iterator reach to the end of select hypercube
                if (minIter.atEnd() || minIter.key() > exploreDomain.get(curVariableID).getSecond()) {
                    // Go one level up in each trie
                    for (LFTJoin iter : joinFrame.curIters) {
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
                    getAggregateResult();
                    backtrack();
                    continue;
                }

                // Collect relevant iterators
                joinFrame.curIters = joinsByVar.get(curVariableID);
                joinFrame.nrCurIters = joinFrame.curIters.size();

                List<LFTJoin> iters = joinFrame.curIters;

                int startKey = exploreDomain.get(curVariableID).getFirst();
                int endKey = exploreDomain.get(curVariableID).getSecond();
                // open lftj iterator
                for (LFTJoin iter : iters) {
                    iter.open();
                    iter.seek(startKey);
//                    budget -= iter.open();
//                    budget -= iter.seek(startKey);
                }

                // Check for early termination
                boolean reachEnd = false;
                for (LFTJoin iter : iters) {
                    if (iter.atEnd() || iter.key() > endKey) {
                        reachEnd = true;
                        break;
                    }
                }

                if (reachEnd) {
                    for (LFTJoin iter : iters) {
                        iter.up();
                    }
                    backtrack();
                    // skip the execution join
                    continue;
                } else {

                    if (startTuplePosition[curVariableID] == null) {
                        ArrayList<Integer> tuplePosition = new ArrayList<>();
                        for (LFTJoin curJoin : joinsByVar.get(curVariableID)) {
                            tuplePosition.add(curJoin.curTuples[curJoin.curTrieLevel]);
                        }
                        startTuplePosition[curVariableID] = tuplePosition;
                    }

                    // Sort iterators by their keys
                    Collections.sort(iters, new Comparator<LFTJoin>() {
                        @Override
                        public int compare(LFTJoin o1, LFTJoin o2) {
                            return Integer.compare(o1.key(), o2.key());
                        }
                    });

                    // Execute search procedure
                    joinFrame.p = 0;
                    joinFrame.maxIterPos = (joinFrame.nrCurIters + joinFrame.p - 1) % joinFrame.nrCurIters;
                    joinFrame.maxKey = joinFrame.curIters.get(joinFrame.maxIterPos).key();
                }
            }

            int endKey = exploreDomain.get(curVariableID).getSecond();
            // execute join
            while (true) {

                // Count current round
                // Check for timeout and not in the last end
                if (budget <= 0) {

                    // timeout, save final state
                    // hypercube
                    List<Integer> endValues = new ArrayList<>();
                    for (int i = 0; i < curVariableID; i++) {
                        int endValue = joinFrames.get(i).maxKey;
                        endValues.add(endValue);
                    }
                    // for position curVariableID, [5, 9 ...], [5, 8, max, max]
                    // todo
                    endValues.add(joinFrames.get(curVariableID).maxKey - 1);
                    for (int i = curVariableID + 1; i < nrVars; i++) {
                        endValues.add(exploreDomain.get(i).getSecond());
                    }

                    HypercubeManager.updateInterval(selectCube, endValues, attributeOrder);
                    return new Pair<>(Math.max(rewardFirstValueScale(cubeStartValues, endValues, cubeEndValues), 0), this.joinResult);
                }

                // Get current key
                LFTJoin minIter = joinFrame.curIters.get(joinFrame.p);
                int minKey = minIter.key();

                // Did we find a match between iterators?
                if (minKey == joinFrame.maxKey) {
                    // test whether key are cached or not
                    advance();
                    // go to next level
                    break;
                } else {
                    // min key not equal max key
//                    minIter.seek(joinFrame.maxKey);
                    budget -= minIter.seek(joinFrame.maxKey);
                    if (minIter.atEnd() || minIter.key() > endKey) {
                        // Go one level up in each trie
                        for (LFTJoin iter : joinFrame.curIters) {
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

        //  finish query
        HypercubeManager.finishHyperCube();

        return new Pair<>(Math.max(rewardFirstValueScale(cubeStartValues, cubeEndValues, cubeEndValues), 0), this.joinResult);
    }
}