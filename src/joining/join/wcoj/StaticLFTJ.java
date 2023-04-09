package joining.join.wcoj;

import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import util.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class StaticLFTJ {
    /**
     * Contains at i-th position iterator over
     * i-th element in query from clause.
     */
    public final LFTJiter[] idToIter;
    /**
     * Order of variables (i.e., equivalence classes
     * of join attributes connected via equality
     * predicates).
     */
    final List<Set<ColumnRef>> varOrder;

    public final List<List<Integer>> itersNumberByVar;
    /**
     * Number of variables in input query (i.e.,
     * number of equivalence classes of join columns
     * connected via equality predicates).
     */
    final int nrVars;
    /**
     * Whether entire result was generated.
     */
    boolean finished = false;

    final int nrJoined;

    final int[] attributeOrder;

    List<Pair<Integer, Integer>> attributeValueBound;

//    public static long part1 = 0;
//
//    public static long part2 = 0;

    /**
     * Initialize join for given query.
     *
     * @param query            join query to process via LFTJ
     * @param executionContext summarizes procesing context
     * @throws Exception
     */
    public StaticLFTJ(QueryInfo query, Context executionContext, int[] order, List<Pair<Integer, Integer>> attributeValueBound) throws Exception {
        // Initialize query and context variables
//        super(query, executionContext);
        attributeOrder = order;
        varOrder = Arrays.stream(order).boxed().map(i -> query.equiJoinAttribute.get(i)).collect(Collectors.toList());
        nrVars = query.equiJoinClasses.size();
        nrJoined = query.nrJoined;
//        long startMillis1 = System.currentTimeMillis();
        // Initialize iterators
//        Map<String, LFTJiter> aliasToIter = new HashMap<>();
        HashMap<String, Integer> aliasToNumber = new HashMap<>();
        idToIter = new LFTJiter[nrJoined];
        for (int aliasCtr = 0; aliasCtr < nrJoined; ++aliasCtr) {
            String alias = query.aliases[aliasCtr];
            LFTJiter iter = new LFTJiter(query,
                    executionContext, aliasCtr, varOrder);
//            aliasToIter.put(alias, iter);
            aliasToNumber.put(alias, aliasCtr);
            idToIter[aliasCtr] = iter;
        }

//        long startMillis2 = System.currentTimeMillis();
        // Group iterators by variable
        itersNumberByVar = new ArrayList<>();
        for (Set<ColumnRef> var : varOrder) {
            List<Integer> curNumberIters = new ArrayList<>();
            for (ColumnRef colRef : var) {
                String alias = colRef.aliasName;
                curNumberIters.add(aliasToNumber.get(alias));
            }
            itersNumberByVar.add(curNumberIters);
        }

        this.attributeValueBound = Arrays.stream(order).mapToObj(attributeValueBound::get).collect(Collectors.toList());
//        long startMillis3 = System.currentTimeMillis();

//        part1 += (startMillis2 - startMillis1);
//        part2 += (startMillis3 - startMillis2);
    }

    public boolean isFinished() {
        return finished;
    }
}