package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.IndexingMode;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import joining.join.wcoj.LFTJiter;
import query.ColumnRef;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Features utility functions for creating indexes.
 * 
 * @author immanueltrummer
 *
 */
public class Indexer {
	/**
	 * Create an index on the specified column.
	 * 
	 * @param colRef	create index on this column
	 */
	public static void index(ColumnRef colRef) throws Exception {
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				IntIndex index = new IntIndex(intData);
				BufferManager.colToIndex.put(colRef, index);
			} else if (data instanceof DoubleData) {
				DoubleData doubleData = (DoubleData)data;
				DoubleIndex index = new DoubleIndex(doubleData);
				BufferManager.colToIndex.put(colRef, index);
			}
		}
	}
	/**
	 * Creates an index for each key/foreign key column.
	 * 
	 * @param mode	determines on which columns to create indices
	 * @throws Exception
	 */
	public static void indexAll(IndexingMode mode) throws Exception {
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
		Map<String, String> tableToAlias = new HashMap<>();
		tableToAlias.put("cast_info", "ci");
		int card = CatalogManager.getCardinality("cast_info");
		ColumnRef columnRef1 = new ColumnRef("cast_info", "person_id");
		ColumnRef columnRef2 = new ColumnRef("cast_info", "movie_id");
		ColumnRef columnRef3 = new ColumnRef("cast_info", "person_role_id");
		ColumnRef columnRef4 = new ColumnRef("cast_info", "role_id");
		// build following sorted indices
		buildSortIndices(Arrays.asList(columnRef1, columnRef2), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef2, columnRef1), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef1, columnRef2, columnRef4), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef1, columnRef4, columnRef2), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef2, columnRef1, columnRef4), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef2, columnRef4, columnRef1), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef4, columnRef1, columnRef2), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef4, columnRef2, columnRef1), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef1, columnRef2, columnRef3), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef1, columnRef3, columnRef2), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef2, columnRef1, columnRef3), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef2, columnRef3, columnRef1), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef3, columnRef1, columnRef2), card, tableToAlias);
		buildSortIndices(Arrays.asList(columnRef3, columnRef2, columnRef1), card, tableToAlias);
		long totalMillis = System.currentTimeMillis() - startMillis;
		System.out.println("Indexing took " + totalMillis + " ms.");
	}

	public static void buildSortIndices(List<ColumnRef> columnRefs, int card, Map<String, String> tableToAlias) {
		List<ColumnData> trieCols = new ArrayList<>();
		List<ColumnRef> trieRefs = new ArrayList<>();
		for (ColumnRef columnRef : columnRefs) {
			try {
				trieCols.add(BufferManager.getData(columnRef));
				trieRefs.add(new ColumnRef(tableToAlias.get(columnRef.aliasName), columnRef.columnName));
			} catch (Exception e) {
				System.err.println("Error sort indexing "+ columnRef);
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
		LFTJiter.baseOrderCache.put(trieRefs, tupleOrder);
	}
}
