package benchmark;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import config.JoinConfig;

import java.util.Map.Entry;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.NamingConfig;
import config.StartupConfig;
import diskio.PathUtil;
import indexing.Indexer;
import joining.JoinProcessor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import print.RelationPrinter;
import query.QueryInfo;

/**
 * Benchmarks pre-, join, and post-processing stage and compares
 * the output sizes against the sizes of results produced by
 * Postgres.
 *
 * @author immanueltrummer
 */
public class BenchAndVerifySubquery {
    /**
     * Processes all queries in given directory.
     *
     * @param args first argument is Skinner DB directory,
     *             second argument is query directory
     *             third argument is Postgres database name
     *             fourth argument is Postgres user name
     *             fifth argument is Postgres user password
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Check for command line parameters
//		if (args.length != 2 && args.length != 3) {
//			System.out.println("Specify Skinner DB dir, "
//					+ "query directory, Postgres DB name, "
//					+ "Postgres user, and Postgres password!");
//			return;
//		}
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
        Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
        // Read all queries from files
        Map<String, List<PlainSelect>> nameToQuery =
                BenchUtil.readAllSubqueries(queryDir);
        // Open benchmark result file
        for (Entry<String, List<PlainSelect>> entry : nameToQuery.entrySet()) {
            System.out.println("key:" + entry.getKey());
            System.out.println("value:" + entry.getValue().toString());
            long startMillis = System.currentTimeMillis();
            for (PlainSelect subquery : entry.getValue()) {
                QueryInfo query = new QueryInfo(subquery,
                        false, -1, -1, null);
                Context preSummary = Preprocessor.process(query);
                long preMillis = System.currentTimeMillis() - startMillis;
                long joinStartMillis = System.currentTimeMillis();
                System.out.println("preMillis:" + preMillis);
                JoinProcessor.process(query, preSummary);
                long joinEndMillis = System.currentTimeMillis();
                PostProcessor.process(query, preSummary,
                        NamingConfig.FINAL_RESULT_NAME, true);
                long totalMillis = System.currentTimeMillis() - startMillis;
                System.out.println("totalMillis:" + totalMillis);
            }
            String resultRel = NamingConfig.FINAL_RESULT_NAME;
            RelationPrinter.print(resultRel);
            BufferManager.unloadTempData();
            CatalogManager.removeTempTables();
        }
        JoinProcessor.executorService.shutdown();
    }

}
