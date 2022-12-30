package config;

/**
 * Configures behavior of SkinnerDB during startup.
 * 
 * 
 *
 */
public class StartupConfig {
	/**
	 * How to select columns on which to create indices at startup.
	 */
	public static final IndexingMode INDEX_CRITERIA = IndexingMode.ALL;
}
