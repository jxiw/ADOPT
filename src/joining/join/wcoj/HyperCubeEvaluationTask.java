package joining.join.wcoj;

import joining.result.JoinResult;
import util.Pair;

import java.util.*;
import java.util.concurrent.Callable;

public class HyperCubeEvaluationTask implements Callable<HyperCubeEvaluationResult> {

    private final List<Pair<Integer, Integer>> exploreDomain;

    private final LFTJoin[] joins;

    private final Hypercube selectCube;

    private final int[] attributeOrder;

    private final int nrJoined;

    private final JoinResult joinResult;

    private long budget;

    /**
     * Index of current variable in attribute order.
     */
    int curVariableID = 0;

    /**
     * Number of result tuples produced
     * in last invocation (used for reward
     * calculations).
     */
    public int lastNrResults = -1;
    /**
     * Number of variables in input query (i.e.,
     * number of equivalence classes of join columns
     * connected via equality predicates).
     */
    final int nrVars;

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

    /**
     * Whether we backtracked in the last iteration
     * of the main loop (requires certain actions
     * at the beginning of loop).
     */
    boolean backtracked = false;

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

    public HyperCubeEvaluationTask(long budget, Hypercube selectCube, int[] attributeOrder,
                                   LFTJiter[] idToIter, List<List<Integer>> iterNumberByVar) {
        this.selectCube = selectCube;
        this.attributeOrder = attributeOrder;
        this.exploreDomain = selectCube.unfoldCube(attributeOrder);
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
        this.joinResult = new JoinResult(nrJoined);
        this.budget = budget;
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
            LFTJoin iter = joins[aliasCtr];
            resultTuple[aliasCtr] = iter.rid();
        }

        // Add new result tuple
        joinResult.add(resultTuple);
    }

    @Override
    public HyperCubeEvaluationResult call() throws Exception {
        System.out.println("exploreDomain:" + exploreDomain);
        // step one: reset the iterator
        for (LFTJoin join : joins) {
            join.reset();
        }

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
                    addResultTuple();
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

            // execute join
            while (true) {
                // Count current round
                --budget;
//                JoinStats.nrIterations++;
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
//                        endValues.add(joinFrames.get(curVariableID).maxKey);
                    for (int i = curVariableID + 1; i < nrVars; i++) {
                        endValues.add(exploreDomain.get(i).getSecond());
                    }

//                    System.out.println("endValues:" + endValues);
                    HyperCubeEvaluationResult result = new HyperCubeEvaluationResult(selectCube, false, joinResult, endValues);
                    return result;
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
                    minIter.seek(joinFrame.maxKey);
                    if (minIter.atEnd()) {
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
        System.out.println("dfssdd");
        //  finish query
        HyperCubeEvaluationResult result = new HyperCubeEvaluationResult(selectCube, true, joinResult, null);
        return result;
    }
}
