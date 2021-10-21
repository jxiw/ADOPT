package operators;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.GeneralConfig;
import data.ColumnData;
import query.ColumnRef;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

class ListKey {

    ArrayList<Integer> listAsKey;

    public ListKey(ArrayList<Integer> listAsKey) {
        this.listAsKey = listAsKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListKey listKey = (ListKey) o;
        return listAsKey.equals(listKey.listAsKey);
    }

    @Override
    public int hashCode() {
        return listAsKey.hashCode();
    }
}

public class Distinct {

    public static Map<String, Map<Integer, List<Integer>>> tableNamesToUniqueIndexes = new HashMap<>();

    public static void execute(String sourceRelName, List<String> columnNames, String targetRelName) throws Exception {
        List<ColumnRef> sourceColRefs = new ArrayList<ColumnRef>();
        for (String columnName : columnNames) {
            sourceColRefs.add(new ColumnRef(sourceRelName, columnName));
        }
        // Update catalog, inserting materialized table
        TableInfo resultTable = new TableInfo(targetRelName, true);
        CatalogManager.currentDB.addTable(resultTable);
        for (ColumnRef sourceColRef : sourceColRefs) {
            // Add result column to result table, using type of source column
            ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
            ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
                    sourceCol.type, sourceCol.isPrimary,
                    sourceCol.isUnique, sourceCol.isNotNull,
                    sourceCol.isForeign);
            resultTable.addColumn(resultCol);
        }
        // Load source data if necessary
        if (!GeneralConfig.inMemory) {
            for (ColumnRef sourceColRef : sourceColRefs) {
                BufferManager.loadColumn(sourceColRef);
            }
        }
        // Generate column data
        List<ColumnData> columnsData = new ArrayList<>();
        for (ColumnRef sourceColRef : sourceColRefs) {
            columnsData.add(BufferManager.colToData.get(sourceColRef));
        }
        int card = columnsData.get(0).cardinality;
//        Map<Integer, List<Integer>> idxToUniqueIdx = new HashMap<Integer, List<Integer>>();
//        Map<Integer, Integer> hashCodeToIdx = new HashMap<Integer, Integer>();
        // distinct table idx to real idx
        Map<Integer, List<Integer>> idxToUniqueIdx = new HashMap<Integer, List<Integer>>();
        Map<List<Integer>, Integer> valueToIdx = new HashMap<List<Integer>, Integer>();
        int uniqueNum = 0;
        List<Integer> rowList = new ArrayList<>();
        for (int i = 0; i < card; i++) {
            int finalI = i;
//            List<Integer> hashCodeForRowIter = columnsData.stream().map(columnData -> columnData.hashForRow(finalI)).collect(Collectors.toList());
//            int hashCodeForRowI = hashCodeForRowIter.hashCode();
//            if (hashCodeToIdx.containsKey(hashCodeForRowI)) {
//                int uniqueIndex = hashCodeToIdx.get(hashCodeForRowI);
//                idxToUniqueIdx.get(uniqueIndex).add(i);
//            } else {
//                hashCodeToIdx.put(hashCodeForRowI, uniqueNum);
//                List<Integer> realIndices = new ArrayList<>();
//                realIndices.add(i);
//                idxToUniqueIdx.put(uniqueNum, realIndices);
//                rowList.add(i);
//                uniqueNum++;
//            }
            List<Integer> hashCodeForRowIter = columnsData.stream().map(columnData -> columnData.hashForRow(finalI)).collect(Collectors.toList());
            if (valueToIdx.containsKey(hashCodeForRowIter)) {
                int uniqueIndex = valueToIdx.get(hashCodeForRowIter);
                idxToUniqueIdx.get(uniqueIndex).add(i);
            } else {
                valueToIdx.put(hashCodeForRowIter, uniqueNum);
                List<Integer> realIndices = new ArrayList<>();
                realIndices.add(i);
                idxToUniqueIdx.put(uniqueNum, realIndices);
                rowList.add(i);
                uniqueNum++;
            }
        }
        System.out.println("old card:" + card);
        System.out.println("new card:" + uniqueNum);
        // copy columns
        // Generate column data
        sourceColRefs.parallelStream().forEach(sourceColRef -> {
            // Copy relevant rows into result column
            ColumnData srcData = BufferManager.colToData.get(sourceColRef);
            ColumnData resultData = srcData.copyRows(rowList);
            String columnName = sourceColRef.columnName;
            ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
            BufferManager.colToData.put(resultColRef, resultData);
        });
        // Update statistics in catalog
        CatalogManager.updateStats(targetRelName);
        tableNamesToUniqueIndexes.put(targetRelName, idxToUniqueIdx);
    }
}
