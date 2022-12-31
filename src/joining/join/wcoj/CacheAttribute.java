package joining.join.wcoj;

import java.util.Arrays;

public class CacheAttribute {

    int[] columnKeyIdx;

    int[] columnKeyValue;

    int[] columnValueIdx;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheAttribute that = (CacheAttribute) o;
        return Arrays.equals(columnKeyIdx, that.columnKeyIdx) &&
                Arrays.equals(columnKeyValue, that.columnKeyValue) &&
                Arrays.equals(columnValueIdx, that.columnValueIdx);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(columnKeyIdx);
        result = 31 * result + Arrays.hashCode(columnKeyValue);
        result = 31 * result + Arrays.hashCode(columnValueIdx);
        return result;
    }
}
