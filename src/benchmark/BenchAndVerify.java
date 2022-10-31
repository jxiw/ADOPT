package benchmark;

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
public class BenchAndVerify {
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
        int nCube = Integer.parseInt(args[5]);
        JoinConfig.NTHREAD = nThread;
        JoinConfig.BUDGET_PER_EPISODE = budget;
        JoinConfig.EXPLORATION_WEIGHT = lr;
        JoinConfig.INITCUBE = nCube;
//		String PgDB = args[2];
//		String PgUser = args[3];
//		String PgPassword = args.length==5?args[4]:"";
        PathUtil.initSchemaPaths(SkinnerDbDir);
        CatalogManager.loadDB(PathUtil.schemaPath);
        PathUtil.initDataPaths(CatalogManager.currentDB);
        System.out.println("Loading data ...");
        GeneralConfig.inMemory = true;
        BufferManager.loadDB();
        System.out.println("Data loaded.");
        Indexer.buildSortIndices();
        Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
        // Read all queries from files
        Map<String, PlainSelect> nameToQuery =
                BenchUtil.readAllQueries(queryDir);
        // Open connection to Postgres
//		String url = "jdbc:postgresql:" + PgDB;
//		Properties props = new Properties();
//		props.setProperty("user",PgUser);
//		props.setProperty("password",PgPassword);
//		Connection connection = DriverManager.getConnection(url, props);
//		java.sql.Statement pgStatement = connection.createStatement();
        // Open benchmark result file
//        PrintWriter benchOut = new PrintWriter("bench.txt");
//		PrintStream pgOut = new PrintStream("pgResults.txt");
//        PrintStream skinnerOut = new PrintStream("skinnerResults.txt");
//        PrintStream console = System.out;
        // Measure pre-processing time for each query
//        BenchUtil.writeBenchHeader(benchOut);
        for (Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue().toString());
            long startMillis = System.currentTimeMillis();
            QueryInfo query = new QueryInfo(entry.getValue(),
                    false, -1, -1, null);
            Context preSummary = Preprocessor.process(query);
            long preMillis = System.currentTimeMillis() - startMillis;
            long joinStartMillis = System.currentTimeMillis();
            System.out.println("preMillis:" + preMillis);
            JoinProcessor.process(query, preSummary);
            long joinEndMillis = System.currentTimeMillis();
            System.out.println("joinMillis:" + (joinEndMillis - joinStartMillis));
//            long postStartMillis = System.currentTimeMillis();
//            PostProcessor.process(query, preSummary,
//                    NamingConfig.FINAL_RESULT_NAME, true);
//            long postMillis = System.currentTimeMillis() - postStartMillis;
            long totalMillis = System.currentTimeMillis() - startMillis;
//            System.out.println("postMillis:" + postMillis);
            System.out.println("totalMillis:" + totalMillis);
//            String resultRel = NamingConfig.FINAL_RESULT_NAME;
//            RelationPrinter.print(resultRel);
            // Check consistency with Postgres results: unary preds
//			for (ExpressionInfo expr : query.unaryPredicates) {
//				// Unary predicates must refer to one table
//				if (expr.aliasesMentioned.size() != 1) {
//					throw new Exception("Alias " + expr + " must mention one table!");
//				}
//				// Get cardinality after PG filtering
//				String alias = expr.aliasesMentioned.iterator().next();
//				String table = query.aliasToTable.get(alias);
//				StringBuilder sqlBuilder = new StringBuilder();
//				sqlBuilder.append("SELECT COUNT(*) FROM ");
//				sqlBuilder.append(table);
//				sqlBuilder.append(" AS ");
//				sqlBuilder.append(alias);
//				sqlBuilder.append(" WHERE ");
//				CollationVisitor collator = new CollationVisitor();
//				expr.originalExpression.accept(collator);
//				sqlBuilder.append(collator.exprStack.pop().toString());
//				String sql = sqlBuilder.toString().replace("STRING", "TEXT");
//				System.out.println(sql);
//				ResultSet result = pgStatement.executeQuery(sql);
//				result.next();
//				int pgCardinality = result.getInt(1);
//				System.out.println("PG cardinality:\t" + pgCardinality);
//				// Get cardinality after Skinner filtering
//				String filteredName = preSummary.aliasToFiltered.get(alias);
//				TableInfo filteredTable = CatalogManager.currentDB.
//						nameToTable.get(filteredName);
//				String columnName = filteredTable.nameToCol.keySet().iterator().next();
//				ColumnRef colRef = new ColumnRef(filteredName, columnName);
//				int skinnerCardinality = BufferManager.colToData.get(colRef).getCardinality();
//				System.out.println("Skinner card:\t" + skinnerCardinality);
//				if (pgCardinality != skinnerCardinality) {
//					throw new Exception("Inconsistent cardinality for "
//							+ "expression " + expr + "!");
//				}
//			}
//			// Check consistency with Postgres: join result size
//			StringBuilder sqlBuilder = new StringBuilder();
//			sqlBuilder.append("SELECT COUNT(*) FROM ");
//			List<String> fromItems = query.aliasToTable.entrySet().stream().
//					map(e -> e.getValue() + " " + e.getKey()).
//					collect(Collectors.toList());
//			String fromClause = StringUtils.join(fromItems, ", ");
//			sqlBuilder.append(fromClause);
//			if (!query.wherePredicates.isEmpty()) {
//				sqlBuilder.append(" WHERE ");
//				String whereCondition = StringUtils.join(
//						query.wherePredicates, " AND ");
//				sqlBuilder.append(whereCondition);
//			}
//			String joinSql = sqlBuilder.toString().replace("STRING", "TEXT");
//			System.out.println("Join query: " + joinSql);
//			ResultSet joinResult = pgStatement.executeQuery(joinSql);
//			joinResult.next();
//			int pgJoinCard = joinResult.getInt(1);
//			// Get cardinality of Skinner join result
//			int skinnerJoinCard = CatalogManager.getCardinality(
//					NamingConfig.JOINED_NAME);
//			System.out.println("PG Card: " + pgJoinCard +
//					"; Skinner card: " + skinnerJoinCard);
//			if (pgJoinCard != skinnerJoinCard) {
//				throw new Exception("Inconsistent join result"
//						+ "cardinality!");
//			}
//			// Output final result for Postgres
//			StringBuilder pgBuilder = new StringBuilder();
//			PlainSelect plainSelect = entry.getValue();
//			pgBuilder.append("SELECT ");
//			boolean firstSelectItem = true;
//			for (ExpressionInfo selExpr : query.selectExpressions) {
//				if (firstSelectItem) {
//					firstSelectItem = false;
//				} else {
//					pgBuilder.append(", ");
//				}
//				PgPrinter pgPrinter = new PgPrinter(query);
//				pgPrinter.setBuffer(pgBuilder);
//				selExpr.afterNormalization.accept(pgPrinter);
//			}
//			pgBuilder.append(" FROM ");
//			pgBuilder.append(fromClause);
//			pgBuilder.append(" WHERE ");
//			CollationVisitor collator = new CollationVisitor();
//			plainSelect.getWhere().accept(collator);
//			pgBuilder.append(collator.exprStack.pop().toString());
//			String pgQuery = pgBuilder.toString().replace("STRING", "TEXT");
//			System.out.println("PG Query: " + pgQuery);
//			ResultSet queryResult = pgStatement.executeQuery(pgQuery);
//			int nrPgCols = queryResult.getMetaData().getColumnCount();
//			while (queryResult.next()) {
//				for (int colCtr=1; colCtr<=nrPgCols; ++colCtr) {
//					pgOut.print(queryResult.getString(colCtr) + "\t");
//				}
//				pgOut.println();
//			}
//			pgOut.flush();
//			// Output final result for Skinner
//            String resultRel = NamingConfig.FINAL_RESULT_NAME;
//			System.setOut(skinnerOut);
//            RelationPrinter.print(resultRel);
//            skinnerOut.flush();
//			System.setOut(console);
//			// Generate output
//			benchOut.print(entry.getKey() + "\t");
//			benchOut.print(totalMillis + "\t");
//			benchOut.print(preMillis + "\t");
//			benchOut.print(postMillis + "\t");
//			benchOut.print(JoinStats.nrTuples + "\t");
//			benchOut.print(JoinStats.nrIterations + "\t");
//			benchOut.print(JoinStats.nrIndexLookups + "\t");
//			benchOut.print(JoinStats.nrIndexEntries + "\t");
//			benchOut.print(JoinStats.nrUniqueIndexLookups + "\t");
//			benchOut.print(JoinStats.nrUctNodes + "\t");
//			benchOut.print(JoinStats.nrPlansTried + "\t");
//			benchOut.print(skinnerJoinCard + "\t");
//			benchOut.print(JoinStats.nrSamples + "\t");
//			benchOut.print(JoinStats.avgReward + "\t");
//			benchOut.print(JoinStats.maxReward + "\t");
//			benchOut.println(JoinStats.totalWork);
//			benchOut.flush();
            // Clean up
            BufferManager.unloadTempData();
            CatalogManager.removeTempTables();
        }
//		connection.close();
//      benchOut.close();
//		pgOut.close();
//      skinnerOut.close();
        JoinProcessor.executorService.shutdown();
    }

}