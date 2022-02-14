package joining.join.wcoj;

import data.IntData;

import java.util.Arrays;

public class LFTJoin {

    /**
     * We are at this level of the trie.
     */
    int curTrieLevel;
    /**
     * Maximally admissible tuple index
     * at current level (value in prior
     * trie levels changes for later
     * tuples).
     */
    final int[] curUBs;
    /**
     * Contains for each trie level the current position
     * (expressed as tuple index in tuple sort order).
     */
    final int[] curTuples;

    final int card;

    final LFTJiter lftJiter;

    public LFTJoin(LFTJiter lftJiter) {
        int nrLevels = lftJiter.nrLevels;
        this.curTuples = new int[nrLevels];
        this.curUBs = new int[nrLevels];
        this.lftJiter = lftJiter;
        this.card = lftJiter.card;
    }

    /**
     * Return key in current level at given tuple.
     *
     * @param tuple return key of this tuple (in sort order)
     * @return key of specified tuple
     */
    int keyAt(int tuple) {
        int row = lftJiter.tupleOrder[tuple];
        IntData curCol = lftJiter.trieCols.get(curTrieLevel);
        return curCol.data[row];
    }

    public int key() {
        return keyAt(curTuples[curTrieLevel]);
    }

    public int keyAtLevel(int curTrieLevel, int[] curTuples) {
        int row = 0;
        if (curTuples[curTrieLevel] == card) {
            row = lftJiter.tupleOrder[card - 1];
        } else {
            row = lftJiter.tupleOrder[curTuples[curTrieLevel]];
        }
        IntData curCol = lftJiter.trieCols.get(curTrieLevel);
        return curCol.data[row];
    }

    public void seek(int seekKey) {
        // Search next tuple in current range
        int next = seekInRange(seekKey, curUBs[curTrieLevel]);
        // Did we find a tuple?
        if (next < 0) {
            curTuples[curTrieLevel] = card;
        } else {
            curTuples[curTrieLevel] = next;
        }
    }

    /**
     * Resets all internal variables to state
     * before first invocation.
     */
    void reset() {
        Arrays.fill(curTuples, 0);
        Arrays.fill(curUBs, card - 1);
        curTrieLevel = -1;
    }

    public int seekInRange(int seekKey, int ub) {
        // Count search in trie
        int lb = curTuples[curTrieLevel];
        // Try exponential search
        int pos = 1;
        int stepSize = 2;
        if (keyAt(lb) >= seekKey) {
            return lb;
        } else if (keyAt(ub) < seekKey) {
            return -1;
        }
        while ((lb + pos) <= ub && keyAt(lb + pos) < seekKey) {
            pos = pos * stepSize;
        }
        // the key belongs to [lb + pos/2 + 1, lb + pos]
        int start = lb + pos / stepSize + 1;
        int end = Math.min(lb + pos, ub);
        while (start < end) {
            int middle = (start + end) / 2;
            if (keyAt(middle) >= seekKey) {
                end = middle;
            } else {
                start = middle + 1;
            }
        }
        return start;
    }

    /**
     * Advance to next trie level and reset
     * iterator to first associated position.
     */
    public void open() throws Exception {
        int curTuple = curTrieLevel < 0 ? 0 : curTuples[curTrieLevel];
        int nextUB = card - 1;
        if (curTrieLevel >= 0) {
            for (int i = 0; i <= curTrieLevel; ++i) {
                nextUB = Math.min(curUBs[i], nextUB);
            }
            int curKey = key();
            int nextPos = seekInRange(curKey + 1, nextUB);
            if (nextPos >= 0) {
                nextUB = Math.min(nextPos - 1, nextUB);
            }
        }
        ++curTrieLevel;
        curUBs[curTrieLevel] = nextUB;
        curTuples[curTrieLevel] = curTuple;
    }

    /**
     * Proceeds to next key in current trie level.
     */
    public void next() {
        seek(key() + 1);
    }

    /**
     * Returns true iff the iterator is at the end.
     *
     * @return true iff iterator is beyond last tuple
     */
    public boolean atEnd() {
        return curTuples[curTrieLevel] == card;
    }

    public void backward() {
        curTuples[curTrieLevel] = card - 1;
    }

    public int maxValueAtLevel() {
        IntData curCol = lftJiter.trieCols.get(this.curTrieLevel);
        int tupleIndex = this.curUBs[this.curTrieLevel];
        return curCol.data[tupleIndex];
    }

    /**
     * Returns (actual) index of currently
     * considered tuple in its base table.
     *
     * @return record ID of current tuple
     */
    public int rid() {
        return lftJiter.tupleOrder[this.curTuples[this.curTrieLevel]];
    }

    /**
     * Return to last trie level without
     * changing iterator position.
     */
    public void up() {
        --curTrieLevel;
    }

}
