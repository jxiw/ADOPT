package config;

/**
 * Configuration parameters influencing the
 * pre-processing phase.
 * 
 * 
 *
 */
public class PreConfig {
	/**
	 * Whether to apply unary predicates for filtering
	 * during pre-processing step.
	 */
	public static final boolean PRE_FILTER = true;
	/**
	 * Whether to consider using indices for evaluating
	 * unary equality predicates.
	 */
	public static final boolean CONSIDER_INDICES = true;
	/**
	 * Whether to make the join key distinct
	 */
	public static final boolean PRE_DISTINCT = true;
}
