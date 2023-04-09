package joining.join.wcoj;

public class ProgressLFTJ {
    /**
     * Latest state reached by any join order sharing
     * a certain table prefix.
     */
    StateLFTJ latestState;
    /**
     * Points to nodes describing progress for next table.
     */
    final ProgressLFTJ[] childNodes;

    public ProgressLFTJ(int nrAttributes) {
        childNodes = new ProgressLFTJ[nrAttributes];
    }
}
