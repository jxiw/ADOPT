package preprocessing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import config.GeneralConfig;
import config.LoggingConfig;
import config.NamingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import indexing.Index;
import indexing.Indexer;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import operators.Filter;
import operators.IndexFilter;
import operators.IndexTest;
import operators.Materialize;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PreStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Filters query tables via unary predicates and stores
 * result in newly created tables. Creates hash tables
 * for columns with binary equality join predicates.
 * 
 * @author immanueltrummer
 *
 */
public class Preprocessor {
	/**
	 * Whether an error occurred during last invocation.
	 * This flag is used in cases where an error occurs
	 * without an exception being thrown.
	 */
	public static boolean hadError = false;
	/**
	 * Whether to calculate performance.
	 */
	public static boolean performance = false;
	/**
	 * Whether to calculate performance.
	 */
	public static boolean terminated = false;
	/**
	 * Translates a column reference using a table
	 * alias into one using the original table.
	 * 
	 * @param query		meta-data about query
	 * @param queryRef	reference to alias column
	 * @return 			resolved column reference
	 */
	static ColumnRef DBref(QueryInfo query, ColumnRef queryRef) {
		String alias = queryRef.aliasName;
		String table = query.aliasToTable.get(alias);
		String colName = queryRef.columnName;
		return new ColumnRef(table, colName);
	}
	/**
	 * Executes pre-processing. 
	 * 
	 * @param query			the query to pre-process
	 * @return 				summary of pre-processing steps
	 */
	public static Context process(QueryInfo query) throws Exception {
		// Start counter
		long startMillis = System.currentTimeMillis();
		// Reset error flag
		hadError = false;
		terminated = false;
		// Collect columns required for joins and post-processing
		Set<ColumnRef> requiredCols = new HashSet<>();
		requiredCols.addAll(query.colsForJoins);
		requiredCols.addAll(query.colsForPostProcessing);
		log("Required columns: " + requiredCols);
		// Initialize pre-processing summary
		Context preSummary = new Context();
		// Initialize mapping for join and post-processing columns
		for (ColumnRef queryRef : requiredCols) {
			preSummary.columnMapping.put(queryRef, 
					DBref(query, queryRef));
		}
		// Initialize column mapping for unary predicate columns
		for (ExpressionInfo unaryPred : query.unaryPredicates) {
			for (ColumnRef queryRef : unaryPred.columnsMentioned) {
				preSummary.columnMapping.put(queryRef, 
						DBref(query, queryRef));
			}
		}
		// Initialize mapping from query alias to DB tables
		preSummary.aliasToFiltered.putAll(query.aliasToTable);
		log("Column mapping:\t" + preSummary.columnMapping.toString());
		// Iterate over query aliases
		for (String alias: query.aliasToTable.keySet()) {
			long s1 = System.currentTimeMillis();
			// Collect required columns (for joins and post-processing) for this table
			List<ColumnRef> curRequiredCols = new ArrayList<>();
			for (ColumnRef requiredCol : requiredCols) {
				if (requiredCol.aliasName.equals(alias)) {
					curRequiredCols.add(requiredCol);
				}
			}
			// Get applicable unary predicates
			ExpressionInfo curUnaryPred = null;
			for (ExpressionInfo exprInfo : query.unaryPredicates) {
				if (exprInfo.aliasesMentioned.contains(alias)) {
					curUnaryPred = exprInfo;
				}
			}
			// Filter and project if enabled
			if (curUnaryPred != null && PreConfig.FILTER) {
				try {
					//check if the predicate is in the cache
					List<Integer> inCacheRows = null;
					if (PreConfig.IN_CACHE) {
						inCacheRows = applyCache(curUnaryPred);
					}
					if (inCacheRows == null) {
						// Apply index to prune rows if possible
						ExpressionInfo remainingPred = applyIndex(
								query, curUnaryPred, preSummary);
						// TODO: reinsert index usage
						//ExpressionInfo remainingPred = curUnaryPred;
						// Filter remaining rows by remaining predicate
						if (remainingPred != null) {
							List<Integer> rows = filterProject(query, alias, remainingPred,
									curRequiredCols, preSummary);
							if (rows.size() > 0 && PreConfig.IN_CACHE) {
								BufferManager.indexCache.putIfAbsent(curUnaryPred.pid, rows);
							}
							String filteredName = NamingConfig.FILTERED_PRE + alias;
							int cardinality = CatalogManager.getCardinality(filteredName);
							if (cardinality == 0) {
								terminated = true;
								break;
							}
						}
						else {
							String filteredName = NamingConfig.IDX_FILTERED_PRE + alias;
							int cardinality = CatalogManager.getCardinality(filteredName);
							if (cardinality == 0) {
								terminated = true;
								break;
							}
						}
					}
					else {
						// Materialize relevant rows and columns
						String tableName = preSummary.aliasToFiltered.get(alias);
						String filteredName = NamingConfig.FILTERED_PRE + alias;
						List<String> columnNames = new ArrayList<>();
						for (ColumnRef colRef : curRequiredCols) {
							columnNames.add(colRef.columnName);
						}
						Materialize.execute(tableName, columnNames,
								inCacheRows, null, filteredName, true);
						// Update pre-processing summary
						for (ColumnRef srcRef : curRequiredCols) {
							String columnName = srcRef.columnName;
							ColumnRef resRef = new ColumnRef(filteredName, columnName);
							preSummary.columnMapping.put(srcRef, resRef);
						}
						preSummary.aliasToFiltered.put(alias, filteredName);
						int cardinality = CatalogManager.getCardinality(filteredName);
						if (cardinality == 0) {
							terminated = true;
							break;
						}
						log("Cache hit using " + curUnaryPred);
					}
				} catch (Exception e) {
					System.err.println("Error filtering " + alias);
					e.printStackTrace();
					hadError = true;
				}
			} else {
				String table = query.aliasToTable.get(alias);
				preSummary.aliasToFiltered.put(alias, table);
			}
			long s2 = System.currentTimeMillis();
			if (curUnaryPred != null) {
				System.out.println("Predicate: " + curUnaryPred.toString() + "\tTime: " + (s2 - s1));
			}
		}
		// Abort pre-processing if filtering error occurred
		if (hadError) {
			throw new Exception("Error in pre-processor.");
		}

		// Create missing indices for columns involved in equi-joins.
//		log("Creating indices ...");
		if (terminated) {
			return preSummary;
		}


		PreStats.filterTime = System.currentTimeMillis() - startMillis;
		long startIndexMillis = System.currentTimeMillis();

		if (query.nonEquiJoinPreds.size() > 0) {
			createJoinIndices(query, preSummary);
			// construct mapping from join tables to index for each join predicate
			query.equiJoinPreds.forEach(expressionInfo -> {
				expressionInfo.extractIndex(preSummary);
				expressionInfo.setColumnType();
			});
		}

		PreStats.indexTime = System.currentTimeMillis() - startIndexMillis;

		// Measure processing time
//		if (performance) {
			PreStats.preMillis += System.currentTimeMillis() - startMillis;
			PreStats.subPreMillis.add(PreStats.preMillis);
//		}
		System.out.println("preprocessing time:" + (System.currentTimeMillis() - startMillis));

		return preSummary;
	}
	/**
	 * Forms a conjunction between given conjuncts.
	 * 
	 * @param conjuncts	list of conjuncts
	 * @return	conjunction between all conjuncts or null
	 * 			(iff the input list of conjuncts is empty)
	 */
	static Expression conjunction(List<Expression> conjuncts) {
		Expression result = null;
		for (Expression conjunct : conjuncts) {
			if (result == null) {
				result = conjunct;
			} else {
				result = new AndExpression(
						result, conjunct);
			}
		}
		return result;
	}

	/**
	 * Check whether thee given predicate has some satisfied rows saved in the cache.
	 *
	 * @param curUnaryPred		current evaluating unary predicate.
	 * @return					satisfied rows corresponding to given predicate.
	 */
	static List<Integer> applyCache(ExpressionInfo curUnaryPred) {
		List<Integer> rows = BufferManager.indexCache.getOrDefault(curUnaryPred.pid, null);
		return rows;
	}

	/**
	 * Search for applicable index and use it to prune rows. Redirect
	 * column mappings to index-filtered table if possible.
	 * 
	 * @param query			query to pre-process
	 * @param unaryPred		unary predicate on that table
	 * @param preSummary	summary of pre-processing steps
	 * @return	remaining unary predicate to apply afterwards
	 */
	static ExpressionInfo applyIndex(QueryInfo query, ExpressionInfo unaryPred,
			Context preSummary) throws Exception {
		log("Searching applicable index for " + unaryPred + " ...");
		// Divide predicate conjuncts depending on whether they can
		// be evaluated using indices alone.
		log("Conjuncts for " + unaryPred + ": " + unaryPred.conjuncts.toString());
		IndexTest indexTest = new IndexTest(query);
		List<Expression> indexedConjuncts = new ArrayList<>();
		List<Expression> nonIndexedConjuncts = new ArrayList<>();
		List<Expression> sortedConjuncts = new ArrayList<>();
		List<Expression> unsortedConjuncts = new ArrayList<>();
		for (Expression conjunct : unaryPred.conjuncts) {
			// Re-initialize index test
			indexTest.canUseIndex = true;
			indexTest.constantQueue.clear();
			indexTest.sorted = true;
			// Compare predicate against indexes
			conjunct.accept(indexTest);
			// Can conjunct be evaluated only from indices?
			if (indexTest.canUseIndex && PreConfig.CONSIDER_INDICES) {
				if (indexTest.sorted) {
					sortedConjuncts.add(conjunct);
				}
				else {
					unsortedConjuncts.add(conjunct);
				}
			} else {
				nonIndexedConjuncts.add(conjunct);
			}
		}
		if (LoggingConfig.PREPROCESSING_VERBOSE) {
			log("Indexed:\t" + indexedConjuncts.toString() +
					"; other: " + nonIndexedConjuncts.toString());
		}
		// Create remaining predicate expression
		if (unsortedConjuncts.size() > 0) {
			indexedConjuncts.addAll(unsortedConjuncts);
			nonIndexedConjuncts.addAll(sortedConjuncts);
		}
		else {
			indexedConjuncts.addAll(sortedConjuncts);
		}
		Expression remainingExpr = conjunction(nonIndexedConjuncts);
		// Evaluate indexed predicate part
		if (!indexedConjuncts.isEmpty()) {
			IndexFilter indexFilter = new IndexFilter(query);
			Expression indexedExpr = conjunction(indexedConjuncts);
			indexedExpr.accept(indexFilter);
			List<Integer> rows = indexFilter.qualifyingRows.pop();
			// Create filtered table
			String alias = unaryPred.aliasesMentioned.iterator().next();
			String table = query.aliasToTable.get(alias);
			Set<ColumnRef> colSuperset = new HashSet<>();
			colSuperset.addAll(query.colsForJoins);
			colSuperset.addAll(query.colsForPostProcessing);
			// Need to keep columns for evaluating remaining predicates, if any
			ExpressionInfo remainingInfo = null;
			if (remainingExpr != null) {
				remainingInfo = new ExpressionInfo(query, remainingExpr);
				colSuperset.addAll(remainingInfo.columnsMentioned);				
			}
			List<String> requiredCols = colSuperset.stream().
					filter(c -> c.aliasName.equals(alias)).
					map(c -> c.columnName).collect(Collectors.toList());
			String targetRelName = NamingConfig.IDX_FILTERED_PRE + alias;
			long timer1 = System.currentTimeMillis();
			if (indexFilter.isFull) {
				Materialize.execute(table, requiredCols, rows,
						null, targetRelName, true);
			}
			else if (!indexFilter.equalFull && rows.size() == 1) {
				Materialize.executeEqualPos(table, requiredCols, rows,
						indexFilter.lastIndex, targetRelName, true);
			}
			else {
				Materialize.executeRange(table, requiredCols, rows,
						indexFilter.lastIndex, targetRelName, true);
			}
			long timer2 = System.currentTimeMillis();
			System.out.println("Materializing: " + targetRelName + " took " + (timer2 - timer1) + " ms");
			// Update pre-processing summary
			for (String colName : requiredCols) {
				ColumnRef queryRef = new ColumnRef(alias, colName);
				ColumnRef dbRef = new ColumnRef(targetRelName, colName);
				preSummary.columnMapping.put(queryRef, dbRef);
			}
			preSummary.aliasToFiltered.put(alias, targetRelName);
			return remainingInfo;
		} else {
			return unaryPred;
		}
	}
	/**
	 * Creates a new temporary table containing remaining tuples
	 * after applying unary predicates, project on columns that
	 * are required for following steps.
	 * 
	 * @param query			query to pre-process
	 * @param alias			alias of table to filter
	 * @param unaryPred		unary predicate on that table
	 * @param requiredCols	project on those columns
	 * @param preSummary	summary of pre-processing steps
	 */
	static List<Integer> filterProject(QueryInfo query, String alias, ExpressionInfo unaryPred,
			List<ColumnRef> requiredCols, Context preSummary) throws Exception {
		long startMillis = 0;
		if (LoggingConfig.PERFORMANCE_VERBOSE) {
			startMillis = System.currentTimeMillis();
			log("Filtering and projection for " + alias + " ...");
		}
		String tableName = preSummary.aliasToFiltered.get(alias);
		if (LoggingConfig.PERFORMANCE_VERBOSE) {
			log("Table name for " + alias + " is " + tableName);
		}
		// Determine rows satisfying unary predicate
//		long s1 = System.currentTimeMillis();
		List<Integer> satisfyingRows = Filter.executeToList(
				unaryPred, tableName, preSummary.columnMapping, query);
//		long s2 = System.currentTimeMillis();
		// Materialize relevant rows and columns
		String filteredName = NamingConfig.FILTERED_PRE + alias;
		List<String> columnNames = new ArrayList<>();
		for (ColumnRef colRef : requiredCols) {
			columnNames.add(colRef.columnName);
		}
		Materialize.execute(tableName, columnNames, 
				satisfyingRows, null, filteredName, true);
//		long s3 = System.currentTimeMillis();
//		System.out.println("Filtering using " + unaryPred + " took " + (s2 - s1) + "\t" + (s3 - s2));
		// Update pre-processing summary
		for (ColumnRef srcRef : requiredCols) {
			String columnName = srcRef.columnName;
			ColumnRef resRef = new ColumnRef(filteredName, columnName);
			preSummary.columnMapping.put(srcRef, resRef);
		}
		preSummary.aliasToFiltered.put(alias, filteredName);
//		if (LoggingConfig.PERFORMANCE_VERBOSE) {
//			long totalMillis = System.currentTimeMillis() - startMillis;
//			log("Filtering using " + unaryPred + " took " + totalMillis + " milliseconds");
//		}
		// Print out intermediate result table if logging is enabled
//		if (LoggingConfig.PRINT_INTERMEDIATES) {
//			RelationPrinter.print(filteredName);
//		}
		return satisfyingRows;
	}
	/**
	 * Create indices on equality join columns if not yet available.
	 *
	 * @param query			query for which to create indices
	 * @param preSummary	summary of pre-processing steps executed so far
	 * @throws Exception
	 */
	static void createJoinIndices(QueryInfo query, Context preSummary)
			throws Exception {
		// Iterate over columns in equi-joins
		long startMillis = System.currentTimeMillis();
		if (LoggingConfig.PREPROCESSING_VERBOSE) {
			startMillis = System.currentTimeMillis();
		}
		if (GeneralConfig.isParallel) {
			query.indexCols.parallelStream().forEach(queryRef -> {
				try {
					// Resolve query-specific column reference
					ColumnRef dbRef = preSummary.columnMapping.get(queryRef);
//					if (LoggingConfig.PREPROCESSING_VERBOSE) {
//						log("Creating index for " + queryRef +
//								" (query) - " + dbRef + " (DB)");
//					}
					// Create index (unless it exists already)
//					if (GeneralConfig.isParallel) {
//						ColumnInfo columnInfo = query.colRefToInfo.get(queryRef);
//						String tableName = query.aliasToTable.get(queryRef.aliasName);
//						String columnName = queryRef.columnName;
//						ColumnRef columnRef = new ColumnRef(tableName, columnName);
//						Index index = BufferManager.colToIndex.getOrDefault(columnRef, null);
//						PartitionIndex partitionIndex = index == null ? null : (PartitionIndex) index;
//						// Get index generation policy according to statistics.
//						// Create index (unless it exists already)
//						Indexer.partitionIndex(dbRef, queryRef, partitionIndex, columnInfo.isPrimary, false, false);
//					}
//					else {
//						Indexer.index(dbRef, false);
//					}
					ColumnInfo columnInfo = query.colRefToInfo.get(queryRef);
					String tableName = query.aliasToTable.get(queryRef.aliasName);
					String columnName = queryRef.columnName;
					ColumnRef columnRef = new ColumnRef(tableName, columnName);
					Index index = BufferManager.colToIndex.getOrDefault(columnRef, null);
					PartitionIndex partitionIndex = index == null ? null : (PartitionIndex) index;
					// Get index generation policy according to statistics.
					// Create index (unless it exists already)
					Indexer.partitionIndex(dbRef, queryRef, partitionIndex, columnInfo.isPrimary, false, false);
				} catch (Exception e) {
					System.err.println("Error creating index for " + queryRef);
					e.printStackTrace();
				}
			});
		}
		else {
			query.indexCols.forEach(queryRef -> {
				try {
					// Resolve query-specific column reference
					ColumnRef dbRef = preSummary.columnMapping.get(queryRef);
					if (LoggingConfig.PREPROCESSING_VERBOSE) {
						log("Creating index for " + queryRef +
								" (query) - " + dbRef + " (DB)");
					}
					ColumnInfo columnInfo = query.colRefToInfo.get(queryRef);
					String tableName = query.aliasToTable.get(queryRef.aliasName);
					String columnName = queryRef.columnName;
					ColumnRef columnRef = new ColumnRef(tableName, columnName);
					Index index = BufferManager.colToIndex.getOrDefault(columnRef, null);
					PartitionIndex partitionIndex = index == null ? null : (PartitionIndex) index;
					// Get index generation policy according to statistics.
					// Create index (unless it exists already)
					Indexer.partitionIndex(dbRef, queryRef, partitionIndex, columnInfo.isPrimary, true, false);
				} catch (Exception e) {
					System.err.println("Error creating index for " + queryRef);
					e.printStackTrace();
				}
			});
		}
		long totalMillis = System.currentTimeMillis() - startMillis;
		System.out.println("Created all indices in " + totalMillis + " ms.");
		log("Created all indices in " + totalMillis + " ms.");
	}
	/**
	 * Output logging message if pre-processing logging activated.
	 * 
	 * @param toLog		text to display if logging is activated
	 */
	static void log(String toLog) {
		if (LoggingConfig.PREPROCESSING_VERBOSE) {
			System.out.println(toLog);
		}
	}
}