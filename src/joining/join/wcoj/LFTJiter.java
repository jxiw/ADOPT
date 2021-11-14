package joining.join.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.CheckConfig;
import data.ColumnData;
import data.IntData;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

/**
 * Implements the iterator used by the LFTJ.
 *
 * @author immanueltrummer
 */
public class LFTJiter {
    /**
     * Cardinality of table that we
     * are iterating over.
     */
    final int card;
    /**
     * Contains data of columns that form
     * the trie levels.
     */
    final List<IntData> trieCols;
    /**
     * Contains row IDs of rows ordered by
     * the variables in the order in which
     * they appear in the global variable
     * order.
     */
    final Integer[] tupleOrder;
    /**
     * Number of trie levels (i.e., number
     * of attributes the trie indexes).
     */
    final int nrLevels;
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
    /**
     * Caches tuple orderings for tables after applying
     * query-specific unary predicates. Such orderings
     * can be reused across different join orders
     * for the same query.
     */
    final static Map<List<ColumnRef>, Integer[]> queryOrderCache =
            new HashMap<>();
    /**
     * Caches tuple orderings for base tables that can
     * be reused across different queries.
     */
    final static Map<List<ColumnRef>, Integer[]> baseOrderCache =
            new HashMap<>();
//
//    static long sortTime = 0;
//
//    static long lftTime1 = 0;
//
//    static long lftTime2 = 0;
//
//    static long lftTime3 = 0;
//
//    static long lftTime4 = 0;

    /**
     * Initializes iterator for given query and
     * relation, and given (global) variable order.
     *
     * @param query          initialize for this query
     * @param context        execution context
     * @param aliasID        initialize for this join table
     * @param globalVarOrder global order of variables
     */
    public LFTJiter(QueryInfo query, Context context, int aliasID,
                    List<Set<ColumnRef>> globalVarOrder) throws Exception {
        // Get information on target table
        String alias = query.aliases[aliasID];
        String table = context.aliasToDistinct.get(alias);
//        String table = context.aliasToFiltered.get(alias);
        card = CatalogManager.getCardinality(table);
        // Extract columns used for sorting
        List<ColumnRef> localColumns = new ArrayList<>();
        trieCols = new ArrayList<>();
//        long stime1 = System.currentTimeMillis();
        for (Set<ColumnRef> eqClass : globalVarOrder) {
            for (ColumnRef colRef : eqClass) {
                if (colRef.aliasName.equals(alias)) {
                    localColumns.add(colRef);
                    String colName = colRef.columnName;
                    ColumnRef bufferRef = new ColumnRef(table, colName);
                    ColumnData colData = BufferManager.getData(bufferRef);
                    trieCols.add((IntData) colData);
                }
            }
        }
        // Initialize position array
        nrLevels = trieCols.size();
        curTuples = new int[nrLevels];
        curUBs = new int[nrLevels];
        // Retrieve cached tuple order or sort
//        long stime2 = System.currentTimeMillis();
        tupleOrder = getTupleOrder(query,
                context, aliasID, localColumns);
//        long stime3 = System.currentTimeMillis();
        // Reset internal state
        reset();
//        long stime4 = System.currentTimeMillis();
        // Perform run time checks if activated
        IterChecker.checkIter(query, context,
                aliasID, globalVarOrder, this);
//        long stime5 = System.currentTimeMillis();
//        lftTime1 += (stime2 - stime1);
//        lftTime2 += (stime3 - stime2);
//        lftTime3 += (stime4 - stime3);
//        lftTime4 += (stime5 - stime4);
//        System.out.println("lftTime1:" + lftTime1);
//        System.out.println("lftTime2:" + lftTime2);
//        System.out.println("lftTime3:" + lftTime3);
//        System.out.println("lftTime4:" + lftTime4);
    }

    /**
     * Sorts tuples by their values in local columns,
     * returns tuple IDs from tuple order in array.
     *
     * @param localColumns sort by those columns
     * @return array with sorted tuple indices
     */
    Integer[] getTupleOrder(QueryInfo query, Context executionContext,
                            int aliasID, List<ColumnRef> localColumns) {
        // No unary predicates for current alias?
        String alias = query.aliases[aliasID];
        boolean notFiltered = executionContext.
                aliasToDistinct.get(alias).equals(alias);
//                aliasToFiltered.get(alias).equals(alias);
        // Did we cache tuple order for associated base tables?
        if (notFiltered && baseOrderCache.containsKey(localColumns)) {
            return baseOrderCache.get(localColumns);
        } else
            // Retrieve cached tuple order or sort
            if (queryOrderCache.containsKey(localColumns)) {
                return queryOrderCache.get(localColumns);
            } else {
                // Initialize tuple order
                Integer[] tupleOrder = new Integer[card];
                for (int i = 0; i < card; ++i) {
                    tupleOrder[i] = i;
                }

//                long startCreateTime = System.currentTimeMillis();
                // Sort tuples by global variable order
                Arrays.parallelSort(tupleOrder, new Comparator<Integer>() {
                    public int compare(Integer row1, Integer row2) {
                        for (ColumnData colData : trieCols) {
                            int cmp = colData.compareRows(row1, row2);
                            if (cmp == 2) {
                                boolean row1null = colData.isNull.get(row1);
                                boolean row2null = colData.isNull.get(row2);
                                if (row1null && !row2null) {
                                    return -1;
                                } else if (!row1null && row2null) {
                                    return 1;
                                }
                            } else if (cmp != 0) {
                                return cmp;
                            }
                        }
                        return 0;
                    }
                });
//                long endCreateTime = System.currentTimeMillis();
//                sortTime += (endCreateTime - startCreateTime);
//                System.out.println("sort time:" + sortTime);

                // build hash map, tuple order position to real position
//			ArrayList<Integer> uniqueTupleOrder = new ArrayList<>();
//			for (int i = 1; i < card; i++) {
//				int tupleIdx1 = tupleOrder[i - 1];
//				int tupleIdx2 = tupleOrder[i];
//				boolean unique = trieCols.parallelStream().anyMatch(column -> column.data[tupleIdx1] != column.data[tupleIdx2]);
//				if (unique) {
//					uniqueTupleOrder.add(i - 1);
//				} else {
//					System.out.println("same as prevous" + tupleIdx2);
//				}
//			}
//			uniqueTupleOrder.add(card - 1);
//			System.out.println("unique tuple size:" + uniqueTupleOrder.size());
//			System.out.println("tuple size:" + tupleOrder.length);
                // Distinguish by cache
                if (notFiltered) {
                    baseOrderCache.put(localColumns, tupleOrder);
                } else {
                    queryOrderCache.put(localColumns, tupleOrder);
                }
                return tupleOrder;
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

    /**
     * Return key in current level at given tuple.
     *
     * @param tuple return key of this tuple (in sort order)
     * @return key of specified tuple
     */
    int keyAt(int tuple) {
        int row = tupleOrder[tuple];
        IntData curCol = trieCols.get(curTrieLevel);
        return curCol.data[row];
    }

    /**
     * Returns key at current iterator position
     * (we currently assume integer keys only).
     *
     * @return key at current level and position
     */
    public int key() {
        return keyAt(curTuples[curTrieLevel]);
    }

    public int keyAtLevel(int curTrieLevel) {
        int row = 0;
        if (curTuples[curTrieLevel] == card) {
            row = tupleOrder[card - 1];
        } else {
            row = tupleOrder[curTuples[curTrieLevel]];
        }
        IntData curCol = trieCols.get(curTrieLevel);
        return curCol.data[row];
    }

    public int maxValueAtLevel(int curTrieLevel) {
        IntData curCol = trieCols.get(curTrieLevel);
        return curCol.data[tupleOrder[card - 1]];
    }

    public int minValueAtLevel(int curTrieLevel) {
        IntData curCol = trieCols.get(curTrieLevel);
        return curCol.data[tupleOrder[0]];
    }

    /**
     * Returns (actual) index of currently
     * considered tuple in its base table.
     *
     * @return record ID of current tuple
     */
    public int rid() {
        return tupleOrder[curTuples[curTrieLevel]];
    }

    /**
     * Proceeds to next key in current trie level.
     */
    public void next() throws Exception {
        seek(key() + 1);
    }

    /**
     * Seek first tuple whose key in current column
     * is not below seek key, consider range up to
     * and including tuple index ub. Returns -1 if
     * no such tuple can be found.
     *
     * @param seekKey find tuple with key at least that
     * @param ub      search for tuples up to this index
     * @return next tuple index or -1 if none found
     */
    public int seekInRange(int seekKey, int ub) throws Exception {
        // Count search in trie
        JoinStats.nrIndexLookups++;
        // Current tuple position is lower bound
        int lb = curTuples[curTrieLevel];
        // Until search bounds collapse
//        while (lb < ub) {
//            int middle = (lb + ub) / 2;
//            if (keyAt(middle) >= seekKey) {
//                ub = middle;
//            } else {
//                lb = middle + 1;
//            }
//        }
//
//        // Debugging check
//        if (lb != ub) {
//            System.out.println("Error - lb " +
//                    lb + " and ub " + ub);
//        }
//        // Check that prior keys did not change
//        if (CheckConfig.CHECK_LFTJ_ITERS && keyAt(lb) >= seekKey) {
//            for (int level = 0; level < curTrieLevel; ++level) {
//                int curTuple = curTuples[curTrieLevel];
//                IntData intData = trieCols.get(level);
//                int cmp = intData.compareRows(
//                        tupleOrder[curTuple], tupleOrder[lb]);
//                if (cmp != 0) {
//                    throw new Exception(
//                            "Inconsistent keys at level " + level +
//                                    " for seek at level " + curTrieLevel +
//                                    "; upper bounds: " +
//                                    Arrays.toString(curUBs) +
//                                    "; current tuples: " +
//                                    Arrays.toString(curTuples) +
//                                    "; lb: " + lb +
//                                    "; key1: " +
//                                    intData.data[tupleOrder[curTuple]] +
//                                    "; key2: " +
//                                    intData.data[tupleOrder[lb]]);
//                }
//            }
//        }
//        // Return next tuple position or -1
//        return keyAt(lb) >= seekKey ? lb : -1;

        // Try exponential search
        int pos = 1;
        int stepSize = 2;
        if (keyAt(ub) < seekKey) {
            return -1;
        }
        while ((lb + pos) <= ub && keyAt(lb + pos) < seekKey) {
            pos = pos * stepSize;
        }
        // the key belongs to [lb + pos/2 + 1, lb + pos]
        int start = lb + pos / stepSize + 1;
        int end = (lb + pos < ub) ? lb + pos : ub;
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
	/*
	public int seekInRange(int seekKey, int ub) {
		// Prepare for "galloping"
		int step = 1;
		int UBtuple = curTuples[curTrieLevel] + step;
		// Don't exceed relation boundaries
		UBtuple = Math.min(UBtuple, ub);
		int UBkey = keyAt(UBtuple);
		// Until key changed or end reached
		while (UBkey < seekKey && UBtuple<ub) {
			UBkey = keyAt(UBtuple);
			step *= 2;
			UBtuple += step;
		}
		UBtuple = Math.min(UBtuple, ub);
		// Set to end position if not found
		if (keyAt(UBtuple) < seekKey) {
			return -1;
		}
		// Otherwise apply binary search
		int LBtuple = Math.max(UBtuple-step, 0);
		// Search next tuple in tuple range
		int searchLB = LBtuple;
		int searchUB = UBtuple;
		while (searchLB < searchUB) {
			int middle = (searchLB + searchUB)/2;
			if (keyAt(middle)>=seekKey) {
				searchUB = middle;
			} else {
				searchLB = middle+1;
			}
		}
		// Debugging check
		if (searchLB != searchUB) {
			System.out.println("Error - searchLB " + 
					searchLB + " and searchUB " + searchUB);
		}
		// Return index of next tuple
		return searchLB;
	}
	*/

    /**
     * Place iterator at first element whose
     * key is at or above the seek key.
     *
     * @param seekKey lower bound for next key
     */
    public void seek(int seekKey) throws Exception {
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
		/*
		System.out.println("--- Opening iterator");
		System.out.println(Arrays.toString(curUBs));
		System.out.println(Arrays.toString(curTuples));
		*/
    }

    /**
     * Return to last trie level without
     * changing iterator position.
     */
    public void up() {
        --curTrieLevel;
    }
}