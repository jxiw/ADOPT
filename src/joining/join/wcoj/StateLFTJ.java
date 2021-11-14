package joining.join.wcoj;

import java.util.Arrays;

public class StateLFTJ {
    /**
     * Last position index in join order
     * over whose tuples we have iterated.
     */
    public int lastIndex;
    /**
     * Last combination of tuple indices that
     * we have seen.
     */
    public int[] tupleValues;

    public boolean isAhead;

    /**
     * Initializes tuple indices to appropriate size.
     *
     * @param nrAttribute	number of joined tables
     */
    public StateLFTJ(int nrAttribute) {
        tupleValues = new int[nrAttribute];
        Arrays.fill(tupleValues, 0);
        lastIndex = -1;
        isAhead = true;
    }

    /**
     * Returns true iff the other state is ahead in evaluation,
     * considering only the shared join order prefix.
     *
     * @param order			join order of tables
     * @param otherState	processing state for join order with same prefix
     * @param prefixLength	length of shared prefix
     * @return				true iff the other state is ahead
     */
    boolean isAhead(int[] order, StateLFTJ otherState, int prefixLength) {
        // Determine up to which join order index we compare
        int nrCompared = prefixLength;
        //int nrCompared = Math.min(prefixLength, lastIndex);
        //nrCompared = Math.min(nrCompared, otherState.lastIndex);
        // Whichever state had higher progress in the first table
        // (in shared prefix in join order) wins.
        for (int joinCtr=0; joinCtr<nrCompared; ++joinCtr) {
            int attribute = order[joinCtr];
            if (tupleValues[attribute] > otherState.tupleValues[attribute]) {
                // this state is ahead
                return false;
            } else if (otherState.tupleValues[attribute] > tupleValues[attribute]) {
                // other state is ahead
                isAhead = false;
                return true;
            }
        }
        // Equal progress in both states for shared prefix
        return false;
    }
    /**
     * Considers another state achieved for a join order
     * sharing the same prefix in the table ordering.
     * Updates ("fast forwards") this state if the
     * other state is ahead.
     *
     * @param order			join order for which other state was achieved
     * @param otherState	evaluation state achieved for join order sharing prefix
     * @param prefixLength	length of prefix shared with other join order
     */
    public void fastForward(int[] order, StateLFTJ otherState, int prefixLength) {
        // Fast forward is only executed if other state is more advanced
        if (isAhead(order, otherState, prefixLength)) {
            // Determine up to which join order index we compare
            //int nrUsed = Math.min(prefixLength, otherState.lastIndex);
            int nrUsed = prefixLength;
            // Adopt tuple indices from other state for common prefix -
            // set the remaining indices to zero.
            int nrTables = order.length;
            for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
                int table = order[joinCtr];
                int otherIndex = otherState.tupleValues[table];
                //int newIndex = joinCtr < nrUsed ? otherIndex : -1;
                int newIndex = joinCtr < nrUsed ? otherIndex : 0;
                tupleValues[table] = newIndex;
            }
            // Start from last position that was changed
            lastIndex = nrUsed-1;
            // Current tuple is considered "fresh"
            //lastMove = JoinMove.DOWN;
        }
    }
    /**
     * Checks whether processing is finished.
     *
     * @return	true if result was generated
     */
    public boolean isFinished() {
        return lastIndex < 0;
    }

    @Override
    public String toString() {
        return "Last index " + lastIndex + " on " + Arrays.toString(tupleValues);
    }
}
