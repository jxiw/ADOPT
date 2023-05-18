package benchmark;

import java.util.Map;
import java.util.Map.Entry;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.JoinConfig;
import config.StartupConfig;
import diskio.PathUtil;
import indexing.Indexer;
import joining.JoinProcessor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.QueryInfo;

/**
 * Benchmarks pre-, join, and post-processing stage and compares the output
 * sizes against the sizes of results produced by Postgres.
 *
 */
public class BenchAndVerify {
	/**
	 * Processes all queries in given directory.
	 *
	 * @param args first argument is Skinner DB directory, second argument is query
	 *             directory third argument is Postgres database name fourth
	 *             argument is Postgres user name fifth argument is Postgres user
	 *             password
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Check for command line parameters
		// Initialize database
		String SkinnerDbDir = args[0];
		String queryDir = args[1];
		int budget = Integer.parseInt(args[2]);
		int nThread = Integer.parseInt(args[3]);
		double lr = Double.parseDouble(args[4]);
		JoinConfig.NTHREAD = nThread;
		JoinConfig.BUDGET_PER_EPISODE = budget;
		JoinConfig.EXPLORATION_WEIGHT = lr;
		PathUtil.initSchemaPaths(SkinnerDbDir);
		CatalogManager.loadDB(PathUtil.schemaPath);
		PathUtil.initDataPaths(CatalogManager.currentDB);
		System.out.println("Loading data ...");
		GeneralConfig.inMemory = true;
		BufferManager.loadDB();
		System.out.println("Data loaded.");
		Indexer.buildSortIndices();
//		Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
		// Read all queries from files
		Map<String, PlainSelect> nameToQuery = BenchUtil.readAllQueries(queryDir);
		// Measure pre-processing time for each query
		for (Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
			System.out.println(entry.getKey());
			System.out.println(entry.getValue().toString());
			long startMillis = System.currentTimeMillis();
			QueryInfo query = new QueryInfo(entry.getValue(), false, -1, -1, null);
			Context preSummary = Preprocessor.process(query);
			long preMillis = System.currentTimeMillis() - startMillis;
			System.out.println("preMillis:" + preMillis);
			long joinStartMillis = System.currentTimeMillis();
			JoinProcessor.process(query, preSummary);
			long joinEndMillis = System.currentTimeMillis();
			System.out.println("join time:" + (joinEndMillis - joinStartMillis));
			long totalMillis = System.currentTimeMillis() - startMillis;
			System.out.println("totalMillis:" + totalMillis);
			// Clean up
			BufferManager.unloadTempData();
			CatalogManager.removeTempTables();
		}
		JoinProcessor.executorService.shutdown();
	}
}
