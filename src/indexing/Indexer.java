package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import config.GeneralConfig;
import config.IndexingMode;
import config.ParallelConfig;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import joining.StaticLFTJCollections;
import joining.join.wcoj.LFTJiter;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IndexPolicy;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import query.ColumnRef;
import types.SQLtype;
import util.ArrayUtil;

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
	public static void index(ColumnRef colRef, boolean sorted) throws Exception {
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				IntIndex index = new IntIndex(intData);
				BufferManager.colToIndex.put(colRef, index);
				if (sorted) {
					index.sortRows();
				}
			} else if (data instanceof DoubleData) {
				DoubleData doubleData = (DoubleData)data;
				DoubleIndex index = new DoubleIndex(doubleData);
				BufferManager.colToIndex.put(colRef, index);
			}
		}
	}

	/**
	 * Create an index on the specified column.
	 *
	 * @param colRef	create index on this column
	 */
	public static void partitionIndex(ColumnRef colRef, ColumnRef queryRef, PartitionIndex oldIndex,
									  boolean isPrimary, boolean isSeq, boolean sorted) throws Exception {
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				IntPartitionIndex intIndex = oldIndex == null ? null : (IntPartitionIndex) oldIndex;
				int keySize = intIndex == null ? 0 : intIndex.keyToPositions.size();
				IndexPolicy policy = Indexer.indexPolicy(isPrimary, isSeq, keySize, intData.cardinality);
				IntPartitionIndex index = new IntPartitionIndex(intData, ParallelConfig.EXE_THREADS, colRef, queryRef,
						intIndex, policy);
				if (sorted) {
					index.sortRows();
				}
				BufferManager.colToIndex.put(colRef, index);
			} else if (data instanceof DoubleData) {
				DoubleData doubleData = (DoubleData)data;
				DoublePartitionIndex doubleIndex = oldIndex == null ? null : (DoublePartitionIndex) oldIndex;
				int keySize = doubleIndex == null ? 0 : doubleIndex.keyToPositions.size();
				IndexPolicy policy = Indexer.indexPolicy(isPrimary, isSeq, keySize, doubleData.cardinality);
				DoublePartitionIndex index = new DoublePartitionIndex(doubleData, ParallelConfig.EXE_THREADS,
						colRef, queryRef, doubleIndex, policy);
				if (sorted) {
					index.sortRows();
				}
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
								boolean sorted = columnInfo.type == SQLtype.DATE;
//								if (GeneralConfig.isParallel) {
//									partitionIndex(colRef, colRef, null,
//											columnInfo.isPrimary, true, sorted);
//								}
//								else {
//									index(colRef, sorted);
//								}
								partitionIndex(colRef, colRef, null,
										columnInfo.isPrimary, true, sorted);
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

	public static IndexPolicy indexPolicy(boolean isPrimary, boolean isSeq, int keySize, int cardinality) {
		IndexPolicy policy;
		if (cardinality <= ParallelConfig.PARALLEL_SIZE || isSeq) {
			policy = IndexPolicy.Sequential;
		}
		else if (isPrimary) {
			policy = IndexPolicy.Key;
		}
		else if (keySize >= ParallelConfig.SPARSE_KEY_SIZE) {
			policy = IndexPolicy.Sparse;
		}
		else {
			policy = IndexPolicy.Dense;
		}
		return policy;
	}

	public static void buildSortIndices() {
		System.out.println("Build sorted indices ...");
		long startMillis = System.currentTimeMillis();
		Map<String, String> tableToAlias = new HashMap<>();
		tableToAlias.put("orders", "orders");
		tableToAlias.put("lineitem", "lineitem");
		tableToAlias.put("supplier", "supplier");
		tableToAlias.put("nation", "nation");
		tableToAlias.put("partsupp", "partsupp");
		tableToAlias.put("customer", "customer");

		int o_card = CatalogManager.getCardinality("orders");
		ColumnRef o_column = new ColumnRef("orders", "o_orderkey");
		buildSortIndices(Collections.singletonList(o_column), o_card, tableToAlias);

		int l_card = CatalogManager.getCardinality("lineitem");
		ColumnRef l_column1 = new ColumnRef("lineitem", "l_suppkey");
		ColumnRef l_column2 = new ColumnRef("lineitem", "l_partkey");
		ColumnRef l_column3 = new ColumnRef("lineitem", "l_orderkey");

		buildSortIndices(Collections.singletonList(l_column2), l_card, tableToAlias);
		buildSortIndices(Arrays.asList(l_column1, l_column2, l_column3), l_card, tableToAlias);
		buildSortIndices(Arrays.asList(l_column1, l_column3, l_column2), l_card, tableToAlias);
		buildSortIndices(Arrays.asList(l_column2, l_column1, l_column3), l_card, tableToAlias);
		buildSortIndices(Arrays.asList(l_column2, l_column3, l_column1), l_card, tableToAlias);
		buildSortIndices(Arrays.asList(l_column3, l_column1, l_column2), l_card, tableToAlias);
		buildSortIndices(Arrays.asList(l_column3, l_column2, l_column1), l_card, tableToAlias);

		buildSortIndices(Arrays.asList(l_column1, l_column3), l_card, tableToAlias);
		buildSortIndices(Arrays.asList(l_column3, l_column1), l_card, tableToAlias);

		int ps_card = CatalogManager.getCardinality("partsupp");
		ColumnRef ps_column1 = new ColumnRef("partsupp", "ps_suppkey");
		ColumnRef ps_column2 = new ColumnRef("partsupp", "ps_partkey");
		buildSortIndices(Collections.singletonList(ps_column1), ps_card, tableToAlias);
		buildSortIndices(Arrays.asList(ps_column1, ps_column2), ps_card, tableToAlias);
		buildSortIndices(Arrays.asList(ps_column2, ps_column1), ps_card, tableToAlias);

		int s_card = CatalogManager.getCardinality("supplier");
		ColumnRef s_column1 = new ColumnRef("supplier", "s_nationkey");
		ColumnRef s_column2 = new ColumnRef("supplier", "s_suppkey");
		buildSortIndices(Collections.singletonList(s_column1), s_card, tableToAlias);
		buildSortIndices(Arrays.asList(s_column1, s_column2), s_card, tableToAlias);
		buildSortIndices(Arrays.asList(s_column2, s_column1), s_card, tableToAlias);

		int n_card = CatalogManager.getCardinality("nation");
		ColumnRef n_column = new ColumnRef("nation", "n_nationkey");
		buildSortIndices(Collections.singletonList(n_column), n_card, tableToAlias);

		int c_card = CatalogManager.getCardinality("customer");
		ColumnRef c_column1 = new ColumnRef("customer", "c_nationkey");
		ColumnRef c_column2 = new ColumnRef("customer", "c_custkey");
		buildSortIndices(Arrays.asList(c_column1, c_column2), c_card, tableToAlias);
		buildSortIndices(Arrays.asList(c_column2, c_column1), c_card, tableToAlias);

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
		LFTJiter.baseOrderCache.put(trieRefs, tupleOrder);
	}

	public static void getMinMax(ColumnRef columnRef) {
		try {
			ColumnData columnData = BufferManager.getData(columnRef);
			if (columnData instanceof IntData) {
				int ilb = ArrayUtil.getLowerBound(((IntData) columnData).data);
				int iub = ArrayUtil.getUpperBound(((IntData) columnData).data);
				StaticLFTJCollections.joinValueBoundCache.put(columnRef, new int[]{ilb, iub});
			}
		} catch (Exception e) {
			System.err.println("Error sort indexing " + columnRef);
			e.printStackTrace();
		}
	}

}
