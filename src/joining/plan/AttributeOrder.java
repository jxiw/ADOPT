package joining.plan;

import java.util.Arrays;
import java.util.Objects;

public class AttributeOrder {
    /**
     * Number of tables that are joined.
     */
    public final int nrAttributes;
    /**
     * Order in which attributes are joined.
     */
    public final int[] order;

    /**
     * Initializes join order.
     *
     * @param order order in which to join tables
     */
    public AttributeOrder(int[] order) {
        this.nrAttributes = order.length;
        this.order = Arrays.copyOf(order, nrAttributes);
    }

    @Override
    public String toString() {
        return "AttributeOrder{" +
                "nrAttributes=" + nrAttributes +
                ", order=" + Arrays.toString(order) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeOrder that = (AttributeOrder) o;
        return nrAttributes == that.nrAttributes &&
                Arrays.equals(order, that.order);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(nrAttributes);
        result = 31 * result + Arrays.hashCode(order);
        return result;
    }
}


