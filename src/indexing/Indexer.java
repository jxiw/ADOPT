package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.IndexingMode;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import joining.StaticLFTJCollections;
import joining.join.wcoj.LFTJiter;
import query.ColumnRef;
import util.ArrayUtil;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Features utility functions for creating indexes.
 *
 * @author immanueltrummer
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
        tableToAlias.put("movie_info", "mi");
        tableToAlias.put("title", "t");
        tableToAlias.put("name", "n");
        tableToAlias.put("movie_info_idx", "mi_idx");
        int ci_card = CatalogManager.getCardinality("cast_info");
        ColumnRef columnRef1 = new ColumnRef("cast_info", "person_id");
        ColumnRef columnRef2 = new ColumnRef("cast_info", "movie_id");
        ColumnRef columnRef3 = new ColumnRef("cast_info", "person_role_id");
        ColumnRef columnRef4 = new ColumnRef("cast_info", "role_id");
        // build following sorted indices on cast_info
        buildSortIndices(Arrays.asList(columnRef1, columnRef2), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef2, columnRef1), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef1, columnRef2, columnRef4), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef1, columnRef4, columnRef2), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef2, columnRef1, columnRef4), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef2, columnRef4, columnRef1), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef4, columnRef1, columnRef2), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef4, columnRef2, columnRef1), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef1, columnRef2, columnRef3), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef1, columnRef3, columnRef2), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef2, columnRef1, columnRef3), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef2, columnRef3, columnRef1), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef3, columnRef1, columnRef2), ci_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef3, columnRef2, columnRef1), ci_card, tableToAlias);
        // build following sorted indices on movie_info
        ColumnRef columnRef5 = new ColumnRef("movie_info", "movie_id");
        ColumnRef columnRef6 = new ColumnRef("movie_info", "info_type_id");
        int mi_card = CatalogManager.getCardinality("movie_info");
        buildSortIndices(Arrays.asList(columnRef5, columnRef6), mi_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef6, columnRef5), mi_card, tableToAlias);
        // build following sorted indices on title
        int t_card = CatalogManager.getCardinality("title");
        ColumnRef columnRef7 = new ColumnRef("title", "id");
        buildSortIndices(Collections.singletonList(columnRef7), t_card, tableToAlias);
        // build following sorted indices on name
        int n_card = CatalogManager.getCardinality("name");
        ColumnRef columnRef8 = new ColumnRef("name", "id");
        buildSortIndices(Collections.singletonList(columnRef8), n_card, tableToAlias);
        int mii_card = CatalogManager.getCardinality("movie_info_idx");
        ColumnRef columnRef9 = new ColumnRef("movie_info_idx", "movie_id");
        ColumnRef columnRef10 = new ColumnRef("movie_info_idx", "info_type_id");
        buildSortIndices(Arrays.asList(columnRef9, columnRef10), mii_card, tableToAlias);
        buildSortIndices(Arrays.asList(columnRef10, columnRef9), mii_card, tableToAlias);
        getMinMax(columnRef1);
        getMinMax(columnRef2);
        getMinMax(columnRef3);
        getMinMax(columnRef4);
        getMinMax(columnRef5);
        getMinMax(columnRef6);
        getMinMax(new ColumnRef("movie_companies", "company_id"));
        getMinMax(new ColumnRef("movie_companies", "movie_id"));
        getMinMax(new ColumnRef("movie_companies", "company_type_id"));
        getMinMax(new ColumnRef("movie_keyword", "keyword_id"));
        getMinMax(new ColumnRef("movie_keyword", "movie_id"));
        getMinMax(new ColumnRef("name", "id"));
        getMinMax(new ColumnRef("char_name", "id"));
        getMinMax(new ColumnRef("title", "id"));
        getMinMax(new ColumnRef("title", "kind_id"));
        getMinMax(new ColumnRef("movie_info_idx", "info_type_id"));
        getMinMax(new ColumnRef("movie_info_idx", "movie_id"));
        getMinMax(new ColumnRef("aka_name", "person_id"));
        getMinMax(new ColumnRef("keyword", "id"));
        getMinMax(new ColumnRef("aka_title", "movie_id"));
        getMinMax(new ColumnRef("complete_cast", "movie_id"));
        getMinMax(new ColumnRef("complete_cast", "status_id"));
        getMinMax(new ColumnRef("complete_cast", "subject_id"));
        getMinMax(new ColumnRef("person_info", "info_type_id"));
        getMinMax(new ColumnRef("person_info", "person_id"));
        getMinMax(new ColumnRef("company_name", "id"));
        getMinMax(new ColumnRef("company_type", "id"));
        getMinMax(new ColumnRef("company_type", "id"));
        getMinMax(new ColumnRef("movie_link", "movie_id"));
        getMinMax(new ColumnRef("movie_link", "linked_movie_id"));

        // t1, t2, n1
//		LFTJiter.baseOrderCache.put(Collections.singletonList(new ColumnRef("t1", "id")),
//				LFTJiter.baseOrderCache.get(new ColumnRef("t", "id")));
//		LFTJiter.baseOrderCache.put(Collections.singletonList(new ColumnRef("t2", "id")),
//				LFTJiter.baseOrderCache.get(new ColumnRef("t", "id")));
//		LFTJiter.baseOrderCache.put(Collections.singletonList(new ColumnRef("n1", "id")),
//				LFTJiter.baseOrderCache.get(new ColumnRef("n", "id")));
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
