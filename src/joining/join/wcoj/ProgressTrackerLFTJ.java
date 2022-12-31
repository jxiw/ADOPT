package joining.join.wcoj;

import joining.plan.AttributeOrder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ProgressTrackerLFTJ {
    /**
     * Number of tables in the query.
     */
    final int nrAttributes;
    /**
     * Stores progress made for each join order prefix.
     */
    final ProgressLFTJ sharedProgress;
    /**
     * Indicates whether processing is finished.
     */
    public boolean isFinished = false;
    /**
     * For each attribute the last value that was fully treated.
     */
    public final int[] attributeOffset;
    /**
     * Maps join orders to the last state achieved during evaluation.
     */
    Map<AttributeOrder, StateLFTJ> orderToState = new HashMap<AttributeOrder, StateLFTJ>();

    /**
     * Initializes progress tracking for given tables.
     *
     * @param nrAttributes 		number of tables joined
     */
    public ProgressTrackerLFTJ(int nrAttributes) {
        this.nrAttributes = nrAttributes;
        sharedProgress = new ProgressLFTJ(nrAttributes);
        attributeOffset = new int[nrAttributes];
        Arrays.fill(attributeOffset, 0);
    }

    /**
     * Integrates final state achieved when evaluating one specific
     * join order.
     *
     * @param attributeOrder a join order evaluated with a specific time budget
     * @param state     final state achieved when evaluating join order
     */
    public void updateProgress(AttributeOrder attributeOrder, StateLFTJ state) {
        // Update termination flag
        isFinished = state.isFinished();
        state.isAhead = true;
        // Update state for specific join order
        orderToState.put(attributeOrder, state);
        // Update state for all join order prefixes
        ProgressLFTJ curPrefixProgress = sharedProgress;
        // Iterate over position in join order
        int nrAttributes = attributeOrder.nrAttributes;
        for (int attrCtr = 0; attrCtr < nrAttributes; ++attrCtr) {
            int table = attributeOrder.order[attrCtr];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ProgressLFTJ(nrAttributes);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress.latestState == null) {
                curPrefixProgress.latestState = new StateLFTJ(nrAttributes);
            }
            curPrefixProgress.latestState.fastForward(
                    attributeOrder.order, state, attrCtr + 1);
        }

        int firstAttribute = attributeOrder.order[0];
        int lastTreatedValue = state.tupleValues[firstAttribute] - 1;
        attributeOffset[firstAttribute] = Math.max(lastTreatedValue,
                attributeOffset[firstAttribute]);

    }
    /**
     * Returns state from which evaluation of the given join order
     * must start in order to guarantee that all results are generated.
     *
     * @param attributeOrder a join order
     * @return start state for evaluating join order
     */
    public StateLFTJ continueFrom(AttributeOrder attributeOrder) {
        int nrAttributes = attributeOrder.nrAttributes;
        int[] order = attributeOrder.order;
        StateLFTJ state = orderToState.get(attributeOrder);
        if (state == null) {
            state = new StateLFTJ(nrAttributes);
        }
        // Integrate progress from join orders with same prefix
        ProgressLFTJ curPrefixProgress = sharedProgress;
        for (int joinCtr = 0; joinCtr < nrAttributes; ++joinCtr) {
            int attribute = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[attribute];
            if (curPrefixProgress == null) {
                break;
            }
            state.fastForward(order, curPrefixProgress.latestState, joinCtr + 1);
//            if (joinCtr == nrAttributes - 1){
//                System.out.println("share same");
//            }
        }
        // Integrate table offset
        /*
		int firstTable = order[0];
		int offset = Math.max(state.tupleIndices[firstTable], tableOffset[firstTable]);
		state.tupleIndices[firstTable] = offset;
		*/
		/*
		int firstChange = nrTables + 1;
		for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
			int table = order[joinCtr];
			int offset = Math.max(state.tupleIndices[table], tableOffset[table]);
			if (offset > state.tupleIndices[table]) {
				firstChange = Math.min(firstChange, joinCtr);
			}
			state.tupleIndices[table] = offset;
		}
		state.lastIndex = 0;
		*/
        //state.lastIndex = Math.min(state.lastIndex, firstChange);
        return state;
    }

}
