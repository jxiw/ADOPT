package config;

/**
 * Configuration parameters related to
 * the way in which processing is
 * parallelized.
 * 
 * @author immanueltrummer
 *
 */
public class ParallelConfig {
	/**
	 * Maximal number of tuples per batch during pre-processing.
	 */
	public final static int PRE_BATCH_SIZE = 1000;
	/**
	 * Maximal number of tuples per batch during index generation.
	 */
	public final static int PRE_INDEX_SIZE = 100000;
	/**
	 * Maximal number of tuples if joining.parallel method is applied.
	 */
	public final static int PARALLEL_SIZE = 10000;
	/**
	 * Maximal exe thread
	 */
	public static int EXE_THREADS = 64;
	/**
	 * Maximal pre thread
	 */
	public static int PRE_THREADS = 64;
	/**
	 * The minimal size of sparse columns.
	 */
	public static int SPARSE_KEY_SIZE = 10000;
	/**
	 * The minimum size of partitioned table
	 */
	public static int PARTITION_SIZE = 10000;
	/**
	 * Parallel specification:
	 * 0: DPDasync
	 * 1: DPDsync
	 * 2: PSS
	 * 3: PSA
	 * 4: Root parallelization
	 * 5: Leaf parallelization
	 * 6: Tree parallelization
	 *
	 */
	public static int PARALLEL_SPEC = 3;

	public static final boolean HEURISTIC_SHARING = false;
	public static final boolean HEURISTIC_STOP = true;
}
