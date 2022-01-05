package joining.plan;

import java.util.Arrays;
import java.util.Objects;

public class AttributeValue {
    /**
     * Number of tables that are joined.
     */
    public final int nrAttributes;
    /**
     * Order in which attributes are joined.
     */
    public final int[] value;

    /**
     * Initializes join order.
     *
     * @param order order in which to join tables
     */
    public AttributeValue(int[] order) {
        this.nrAttributes = order.length;
        this.value = Arrays.copyOf(order, nrAttributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeValue that = (AttributeValue) o;
        return nrAttributes == that.nrAttributes &&
                Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(nrAttributes);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
