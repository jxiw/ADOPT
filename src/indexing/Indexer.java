package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.IndexingMode;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import joining.join.wcoj.LFTJiter;
import operators.Materialize;
import query.ColumnRef;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Features utility functions for creating indexes.
 */
public class Indexer {
	/**
	 * Create an index on the specified column.
	 *
	 * @param colRef create index on this column
	 */
	public static void index(ColumnRef colRef) throws Exception {
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData) data;
				IntIndex index = new IntIndex(intData);
				BufferManager.colToIndex.put(colRef, index);
			} else if (data instanceof DoubleData) {
				DoubleData doubleData = (DoubleData) data;
				DoubleIndex index = new DoubleIndex(doubleData);
				BufferManager.colToIndex.put(colRef, index);
			}
		}
	}

	/**
	 * Creates an index for each key/foreign key column.
	 *
	 * @param mode determines on which columns to create indices
	 */
	public static void indexAll(IndexingMode mode) {
		System.out.println("Indexing all key columns ...");
		long startMillis = System.currentTimeMillis();
		CatalogManager.currentDB.nameToTable.values().parallelStream().forEach(
				tableInfo -> {
					tableInfo.nameToCol.values().parallelStream().forEach(
							columnInfo -> {
								try {
									if (mode.equals(IndexingMode.ALL) ||
											(mode.equals(IndexingMode.ONLY_KEYS) &&
													(columnInfo.isPrimary || columnInfo.isForeign))) {
										String table = tableInfo.name;
										String column = columnInfo.name;
										ColumnRef colRef = new ColumnRef(table, column);
										System.out.println("Indexing " + colRef + " ...");
										index(colRef);
									}
								} catch (Exception e) {
									System.err.println("Error indexing " + columnInfo);
									e.printStackTrace();
								}
							}
					);
				}
		);
		long totalMillis = System.currentTimeMillis() - startMillis;
		System.out.println("Indexing took " + totalMillis + " ms.");
	}

	public static void buildSortIndices() {
		System.out.println("Build sorted indices ...");
		long startMillis = System.currentTimeMillis();
		Map<String, List<String>> tableToAlias = new HashMap<>();
		List<String> tableAlias = new ArrayList<>();
		for (int i = 0; i < 100; i++)
			tableAlias.add("e" + i);

		tableToAlias.put("edge", tableAlias);
		int l_card = CatalogManager.getCardinality("edge");

		ColumnRef k_column1 = new ColumnRef("edge", "sid");
		ColumnRef k_column2 = new ColumnRef("edge", "tid");

		buildSortIndices(Arrays.asList(k_column1, k_column2), l_card, tableToAlias, LFTJiter.baseOrderCache);
		buildSortIndices(Arrays.asList(k_column2, k_column1), l_card, tableToAlias, LFTJiter.baseOrderCache);

		try {

			IntData columnData1 = (IntData) BufferManager.getData(k_column1);
			IntData columnData2 = (IntData) BufferManager.getData(k_column2);
			List<Integer> satisfyingRows = new ArrayList<>();
			String tableName = "edge";
			String filteredName = "filtered.edge";
			List<String> columnNames = Arrays.asList("sid", "tid");
			for (int i = 0; i < l_card; i++) {
				if (columnData1.data[i] < columnData2.data[i]) {
					satisfyingRows.add(i);
				}
			}

			System.out.println("satisfyingRows:" + satisfyingRows.size());
			Materialize.execute(tableName, columnNames,
					satisfyingRows, null, filteredName, true);

			l_card = CatalogManager.getCardinality(filteredName);

			System.out.println("l_card:" + l_card);
			k_column1 = new ColumnRef(filteredName, "sid");
			k_column2 = new ColumnRef(filteredName, "tid");

			tableToAlias = new HashMap<>();
			tableToAlias.put(filteredName, tableAlias);

			buildSortIndices(Arrays.asList(k_column1, k_column2), l_card, tableToAlias, LFTJiter.queryOrderCache);
			buildSortIndices(Arrays.asList(k_column2, k_column1), l_card, tableToAlias, LFTJiter.queryOrderCache);

		} catch (Exception e) {
			e.printStackTrace();
		}

		long totalMillis = System.currentTimeMillis() - startMillis;
		System.out.println("Sort Indexing took " + totalMillis + " ms.");
	}

	public static void buildSortIndices(List<ColumnRef> columnRefs, int card, Map<String, List<String>> tableToAlias, Map<List<ColumnRef>, int[]> cache) {
		List<ColumnData> trieCols = new ArrayList<>();
		//List<ColumnRef> trieRefs = new ArrayList<>();
		for (ColumnRef columnRef : columnRefs) {
			try {
				trieCols.add(BufferManager.getData(columnRef));
				//trieRefs.add(new ColumnRef(tableToAlias.get(columnRef.aliasName), columnRef.columnName));
			} catch (Exception e) {
				System.err.println("Error sort indexing " + columnRef);
				e.printStackTrace();
			}
		}
		int[] tupleOrder = IntStream.range(0, card).boxed().parallel().sorted(new Comparator<Integer>() {
			@Override
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
		}).mapToInt(i -> i).toArray();
		int n = tableToAlias.get(columnRefs.get(0).aliasName).size();
		for (int i = 0; i < n; i++) {
			List<ColumnRef> trieRefs = new ArrayList<>();
			for (ColumnRef columnRef : columnRefs) {
				trieRefs.add(new ColumnRef(tableToAlias.get(columnRef.aliasName).get(i), columnRef.columnName));
			}
			cache.put(trieRefs, tupleOrder);
		}
	}

}
