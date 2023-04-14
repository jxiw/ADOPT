package joining;

import catalog.CatalogManager;
import config.JoinConfig;
import expressions.ExpressionInfo;
import joining.join.wcoj.HyperCubeEvaluationTask;
import joining.join.wcoj.Hypercube;
import joining.join.wcoj.HypercubeManager;
import joining.join.wcoj.StaticLFTJ;
import joining.plan.AttributeOrder;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import util.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class ParallelLFTJ {

    public final HashMap<AttributeOrder, HyperCubeEvaluationTask> orderToLFTJ;

//    public long executionTime = 0;

//    public long waitTime = 0;

//    public long initLFTJTime = 0;

//    public long taskInitTime = 0;

    public boolean isFinish = false;

    public final QueryInfo query;

    public final Context context;

    public List<int[]> joinResult;

    public ParallelLFTJ(List<int[]> result, QueryInfo query, Context context) {
        this.orderToLFTJ = new HashMap<>();
        this.isFinish = false;
        this.joinResult = result;
        this.query = query;
        this.context = context;
    }

    public double execute(int[] order) {
        AttributeOrder attributeOrder = new AttributeOrder(order);
        try {
            if (orderToLFTJ.containsKey(attributeOrder)) {
                long initStartMillis = System.currentTimeMillis();
                HyperCubeEvaluationTask hyperCubeTask = orderToLFTJ.get(attributeOrder);
                long initEndMillis = System.currentTimeMillis();
                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if (selectCube.intervals.size() == 0) {
                    // receive special hypercube (terminate hypercube)
                    this.isFinish = true;
                    return 0;
                }
//                long startExecMillis = System.currentTimeMillis();
                Pair<Double, List<int[]>> output = hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                double reward = output.getFirst();
                List<int[]> currentResult = output.getSecond();
                handleNonEquiPredicates(currentResult);
//                long endMillis = System.currentTimeMillis();
//                waitTime += startExecMillis - startWaitMillis;
//                executionTime += endMillis - startExecMillis;
//                initLFTJTime += initEndMillis - initStartMillis;
                return reward;
            } else {
                long initStartMillis = System.currentTimeMillis();
                StaticLFTJ staticLFTJ = StaticLFTJCollections.generateLFTJ(attributeOrder);
                long initEndMillis = System.currentTimeMillis();
                List<Pair<Integer, Integer>> attributeValueBound = Arrays.stream(order).mapToObj(StaticLFTJCollections.joinValueBound::get).collect(Collectors.toList());
                HyperCubeEvaluationTask hyperCubeTask = new HyperCubeEvaluationTask(staticLFTJ.idToIter, staticLFTJ.itersNumberByVar, attributeValueBound);
                orderToLFTJ.put(attributeOrder, hyperCubeTask);
                long startWaitMillis = System.currentTimeMillis();
                Hypercube selectCube = HypercubeManager.allocateHypercube();
                if (selectCube.intervals.size() == 0) {
                    // finish all hypercubes
                    this.isFinish = true;
                    return 0;
                }
//                long startExecMillis = System.currentTimeMillis();
                Pair<Double, List<int[]>> output = hyperCubeTask.execute(JoinConfig.BUDGET_PER_EPISODE, order, selectCube);
                double reward = output.getFirst();
                List<int[]> currentResult = output.getSecond();
                handleNonEquiPredicates(currentResult);
//                long endMillis = System.currentTimeMillis();
//                waitTime += startExecMillis - startWaitMillis;
//                executionTime += endMillis - startExecMillis;
//                initLFTJTime += initEndMillis - initStartMillis;
//                taskInitTime += startWaitMillis - initEndMillis;
                return reward;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public void handleNonEquiPredicates(List<int[]> currentResult) {
        if (currentResult.size() > 0) {
            // we have non equality predicates here.
            if (query.nonEquiJoinPreds.size() > 0) {
                // number of non equality join predicates
                int nrNonEquivalentPredict = query.nonEquiJoinNodes.size();
                List<ExpressionInfo> nonEquiExpressions = query.nonEquiJoinPreds;
                // map non equality predicate id to table id
                HashMap<Integer, Integer> nonEquiTablesToAliasIndex = new HashMap<>();
                HashMap<Integer, Integer> nonEquiCards = new HashMap<>();
                Set<Integer> availableTables = new HashSet<>();

                for (int tid = 0; tid < query.nrJoined; tid++) {
                    availableTables.add(tid);
                    for (int eid = 0; eid < nrNonEquivalentPredict; eid++) {
                        if (!nonEquiTablesToAliasIndex.containsKey(eid) && availableTables.containsAll(
                                nonEquiExpressions.get(eid).aliasIdxMentioned)) {
                            nonEquiTablesToAliasIndex.put(eid, tid);
                            String alias = query.aliases[tid];
                            String table = context.aliasToFiltered.get(alias);
                            nonEquiCards.put(eid, CatalogManager.getCardinality(table));
                        }
                    }
                }

                Set<Integer> validateRowIds = new HashSet<>(currentResult.size());
                for (int rid = 0; rid < currentResult.size(); rid++) {
                    validateRowIds.add(rid);
                }

                for (int nonEquiPredictId = 0; nonEquiPredictId < nrNonEquivalentPredict; nonEquiPredictId++) {
                    int processTableId = nonEquiTablesToAliasIndex.get(nonEquiPredictId);
                    int cardinality = nonEquiCards.get(nonEquiPredictId);
                    NonEquiNode nonEquiNode = query.nonEquiJoinNodes.get(nonEquiPredictId);
                    Set<Integer> currentValidateRowIds = new HashSet<>();
                    for (int rid : validateRowIds) {
                        int[] result = currentResult.get(rid);
                        if (result[processTableId] == -1) {
                            // non equality condition is not mentioned before, should be exist or non-exist in subqueries
                            result[processTableId] = 0;
                            while (result[processTableId] < cardinality) {
                                if (nonEquiNode.evaluate(result, processTableId, cardinality)) {
                                    // satisfy condition
                                    currentValidateRowIds.add(rid);
                                    break;
                                } else {
                                    result[processTableId]++;
                                }
                            }
                        } else {
                            // test directly
                            if (nonEquiNode.evaluate(result, processTableId, cardinality)) {
                                // satisfy condition
                                currentValidateRowIds.add(rid);
                            }
                        }
                    }
                    validateRowIds = currentValidateRowIds;
                }

                for (int validateRowId : validateRowIds) {
                    this.joinResult.add(currentResult.get(validateRowId));
                }
            } else {
                // apply cartesian
                boolean applyCartesianProduct = false;
                int cartesianProductTableIndex = -1;
                for (int tid = 0; tid < currentResult.get(0).length; tid++) {
                    int rid = currentResult.get(0)[tid];
                    if (rid == -1) {
                        applyCartesianProduct = true;
                        cartesianProductTableIndex = tid;
                        break;
                    }
                }
                if (applyCartesianProduct) {
                    // add more results here
                    String alias = query.aliases[cartesianProductTableIndex];
                    String table = context.aliasToFiltered.get(alias);
                    int card = CatalogManager.getCardinality(table);
                    for (int[] result : currentResult) {
                        for (int i = 0; i < card; i++) {
                            int[] newResult = Arrays.copyOf(result, result.length);
                            newResult[cartesianProductTableIndex] = i;
                            this.joinResult.add(newResult);
                        }
                    }
                } else {
                    this.joinResult.addAll(currentResult);
                }
            }
        }

    }
}