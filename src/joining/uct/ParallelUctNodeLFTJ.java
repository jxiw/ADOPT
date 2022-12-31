package joining.uct;

import joining.ParallelLFTJ;
import joining.join.DynamicMWJoin;
import joining.result.JoinResult;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import config.JoinConfig;

/**
 * Represents node in UCT search tree.
 *
 * @author immanueltrummer
 */
public class ParallelUctNodeLFTJ {
    /**
     * Used for randomized selection policy.
     */
    final Random random = new Random();
    /**
     * The query for which we are optimizing.
     */
    final QueryInfo query;
    /**
     * Iteration in which node was created.
     */
    final long createdIn;
    /**
     * Level of node in tree (root node has level 0).
     * At the same time the join order index into
     * which attribute selected in this node is inserted.
     */
    public final int treeLevel;
    /**
     * Number of possible actions from this state.
     */
    public final int nrActions;
    /**
     * Actions that have not been tried yet - if the
     * heuristic is used, this only contains actions
     * that have not been tried and are recommended.
     */
    final ConcurrentLinkedDeque<Integer> priorityActions;
    /**
     * Assigns each action index to child node.
     */
    public final ParallelUctNodeLFTJ[] childNodes;
    /**
     * Number of times this node was visited.
     */
    private int[] nrVisits;
    /**
     * Number of times each action was tried out.
     */
    private final int[][] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    private final double[][] accumulatedReward;
    /**
     * Total number of attribute to join.
     */
    final int nrAttributes;
    /**
     * Set of already joined attributes (each UCT node represents
     * a state in which a subset of attributes are joined).
     */
    final Set<Integer> joinedAttributes;
    /**
     * List of unjoined attributes (we use a list instead of a set
     * to enable shuffling during playouts).
     */
    final List<Integer> unjoinedAttributes;
    /**
     * Associates each action index with a next attribute to join.
     */
    public final int[] nextAttributes;
    /**
     * Indicates whether the search space is restricted to
     * join orders that avoid Cartesian products. This
     * flag should only be activated if it is ensured
     * that a given query can be evaluated under that
     * constraint.
     */
    final boolean useHeuristic;
    /**
     * Contains actions that are consistent with the "avoid
     * Cartesian products" heuristic. UCT algorithm will
     * restrict focus on such actions if heuristic flag
     * is activated.
     */
    final Set<Integer> recommendedActions;

    final int nrThreads;

    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     current round number
     * @param query        the query which is optimized
     * @param useHeuristic whether to avoid Cartesian products
     */
    public ParallelUctNodeLFTJ(long roundCtr, QueryInfo query,
                               boolean useHeuristic, int nrThreads) {
        // Count node generation
        this.query = query;
        this.nrAttributes = query.equiJoinAttribute.size();
        this.nrThreads = nrThreads;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrAttributes;
        List<Integer> priorityList = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            priorityList.add(actionCtr);
        }
        Collections.shuffle(priorityList);
        priorityActions = new ConcurrentLinkedDeque<>(priorityList);
        childNodes = new ParallelUctNodeLFTJ[nrActions];
        nrVisits = new int[nrThreads];
        nrTries = new int[nrThreads][nrActions];
        accumulatedReward = new double[nrThreads][nrActions];
        joinedAttributes = new HashSet<Integer>();
        unjoinedAttributes = new ArrayList<>();
        nextAttributes = new int[nrAttributes];
        for (int attributeCtr = 0; attributeCtr < nrAttributes; ++attributeCtr) {
            unjoinedAttributes.add(attributeCtr);
            nextAttributes[attributeCtr] = attributeCtr;
        }
        this.useHeuristic = useHeuristic;
        recommendedActions = new HashSet<Integer>();
        for (int action = 0; action < nrActions; ++action) {
            recommendedActions.add(action);
        }
    }

    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr        current round number
     * @param parent          parent node in UCT tree
     * @param joinedAttribute new joined attributed
     */
    public ParallelUctNodeLFTJ(long roundCtr, ParallelUctNodeLFTJ parent, int joinedAttribute) {
        // Count node generation
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        nrThreads = parent.nrThreads;
        childNodes = new ParallelUctNodeLFTJ[nrActions];
        nrVisits = new int[nrThreads];
        nrTries = new int[nrThreads][nrActions];
        accumulatedReward = new double[nrThreads][nrActions];
        query = parent.query;
        nrAttributes = parent.nrAttributes;
        joinedAttributes = new HashSet<Integer>();
        joinedAttributes.addAll(parent.joinedAttributes);
        joinedAttributes.add(joinedAttribute);
        unjoinedAttributes = new ArrayList<Integer>();
        unjoinedAttributes.addAll(parent.unjoinedAttributes);
        int indexToRemove = unjoinedAttributes.indexOf(joinedAttribute);
        unjoinedAttributes.remove(indexToRemove);
        nextAttributes = new int[nrActions];
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            nextAttributes[actionCtr] = unjoinedAttributes.get(actionCtr);
        }
        // Calculate recommended actions if heuristic is activated
        this.useHeuristic = parent.useHeuristic;
        if (useHeuristic) {
            recommendedActions = new HashSet<Integer>();
            // Iterate over all actions
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Get attribute associated with (join) action
                int attribute = nextAttributes[actionCtr];
                // Check if at least one predicate connects current
                // attributes to new attribute.
                if (query.connectedAttribute(joinedAttributes, attribute)) {
                    recommendedActions.add(actionCtr);
                } // over predicates
            } // over actions
            if (recommendedActions.isEmpty()) {
                // add all actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    recommendedActions.add(actionCtr);
                }
            }
        } // if heuristic is used
        else {
            recommendedActions = null;
        }
        // Collect untried actions, restrict to recommended actions
        // if the heuristic is activated.
        List<Integer> priorityList = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
                priorityList.add(actionCtr);
            }
        }
        Collections.shuffle(priorityList);
        priorityActions = new ConcurrentLinkedDeque<>(priorityList);
    }

    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy policy used to select action
     * @return index of action to try next
     */
    int selectAction(SelectionPolicy policy) {
        // Are there untried actions?
        Integer priorAction = null;
        if (!priorityActions.isEmpty()) {
            priorAction = priorityActions.pollFirst();
        }
        if (priorAction != null) {
            return priorAction;
        } else {
            /* When using the default selection policy (UCB1):
             * We apply the UCT formula as no actions are untried.
             * We iterate over all actions and calculate their
             * UCT value, updating best action and best UCT value
             * on the way. We start iterations with a randomly
             * selected action to ensure that we pick a random
             * action among the ones with maximal UCT value.
             */
            int offset = random.nextInt(nrActions);
            int bestAction = -1;
            double bestQuality = -1;
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Calculate index of current action
                int action = (offset + actionCtr) % nrActions;
                // if heuristic is used, choose only from recommended actions
                if (useHeuristic && recommendedActions.size() == 0) {
                    throw new RuntimeException("there are no recommended exception and we are trying to use heuristic");
                }
                if (useHeuristic && !recommendedActions.contains(action))
                    continue;
                // collection information from all threads
                double accumulatedActionReward = 0;
                int nrActionTries = 0;
                int totalVisit = 0;
                for (int i = 0; i < nrThreads; i++) {
                    accumulatedActionReward += accumulatedReward[i][action];
                    nrActionTries += nrTries[i][action];
                    totalVisit += nrVisits[i];
                }
                if (nrActionTries < 1) {
                    continue;
                }
                double meanReward = accumulatedActionReward / nrActionTries;
                double exploration = Math.sqrt(Math.log(totalVisit) / nrActionTries);
                // Assess the quality of the action according to policy
                double quality = -1;
                switch (policy) {
                    case UCB1:
                        quality = meanReward +
                                JoinConfig.EXPLORATION_WEIGHT * exploration;
                        break;
                    case MAX_REWARD:
                    case EPSILON_GREEDY:
                        quality = meanReward;
                        break;
                    case RANDOM:
                        quality = random.nextDouble();
                        break;
                    case RANDOM_UCB1:
                        if (treeLevel == 0) {
                            quality = random.nextDouble();
                        } else {
                            quality = meanReward +
                                    JoinConfig.EXPLORATION_WEIGHT * exploration;
                        }
                        break;
                }
                if (quality > bestQuality) {
                    bestAction = action;
                    bestQuality = quality;
                }
            }
            if (bestAction == -1) {
                List<Integer> recommendedActionsList = new ArrayList<>(recommendedActions);
                bestAction = recommendedActionsList.get(random.nextInt(recommendedActions.size()));
            }
            // For epsilon greedy, return random action with
            // probability epsilon.
            if (policy.equals(SelectionPolicy.EPSILON_GREEDY)) {
                if (random.nextDouble() <= JoinConfig.EPSILON) {
                    return random.nextInt(nrActions);
                }
            }
            // Otherwise: return best action.
            return bestAction;
        } // if there are unvisited actions
    }

    /**
     * Updates UCT statistics after sampling.
     *
     * @param selectedAction action taken
     * @param reward         reward achieved
     */
    void updateStatistics(int selectedAction, double reward, int tid) {
        ++nrVisits[tid];
        ++nrTries[tid][selectedAction];
        accumulatedReward[tid][selectedAction] += reward;
    }

    /**
     * Randomly complete join order with remaining attributes,
     * invoke evaluation, and return obtained reward.
     *
     * @param joinOrder partially completed join order
     * @return obtained reward
     */
    double playout(ParallelLFTJ joinOp, int[] joinOrder) throws Exception {
        // Last selected attribute
        int lastAttribute = joinOrder[treeLevel];
        // Should we avoid Cartesian product joins?
        if (useHeuristic) {
            Set<Integer> newlyJoined = new HashSet<Integer>();
            newlyJoined.addAll(joinedAttributes);
            newlyJoined.add(lastAttribute);
            // Iterate over join order positions to fill
            List<Integer> unjoinedAttributesShuffled = new ArrayList<Integer>();
            unjoinedAttributesShuffled.addAll(unjoinedAttributes);
            Collections.shuffle(unjoinedAttributesShuffled);
            for (int posCtr = treeLevel + 1; posCtr < nrAttributes; ++posCtr) {
                boolean foundAttribute = false;
                for (int attribute : unjoinedAttributesShuffled) {
                    if (!newlyJoined.contains(attribute) &&
                            query.connectedAttribute(newlyJoined, attribute)) {
                        joinOrder[posCtr] = attribute;
                        newlyJoined.add(attribute);
                        foundAttribute = true;
                        break;
                    }
                }
                if (!foundAttribute) {
                    for (int attribute : unjoinedAttributesShuffled) {
                        if (!newlyJoined.contains(attribute)) {
                            joinOrder[posCtr] = attribute;
                            newlyJoined.add(attribute);
                            break;
                        }
                    }
                }
            }
        } else {
            // Shuffle remaining attributes
            Collections.shuffle(unjoinedAttributes);
            Iterator<Integer> unjoinedAttributesIter = unjoinedAttributes.iterator();
            // Fill in remaining join order positions
            for (int posCtr = treeLevel + 1; posCtr < nrAttributes; ++posCtr) {
                int nextAttribute = unjoinedAttributesIter.next();
                while (nextAttribute == lastAttribute) {
                    nextAttribute = unjoinedAttributesIter.next();
                }
                joinOrder[posCtr] = nextAttribute;
            }
        }
        // Evaluate completed join order and return reward
//        System.out.println("joinOrder:" + Arrays.toString(joinOrder));
        return joinOp.execute(joinOrder);
    }

    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @param policy    policy used to select actions
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder,
                         SelectionPolicy policy, ParallelLFTJ joinOp, int tid) throws Exception {
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // leaf node - evaluate join order and return reward
//            System.out.println("joinOrder:" + Arrays.toString(joinOrder));
            return joinOp.execute(joinOrder);
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy);
            int attribute = nextAttributes[action];
            joinOrder[treeLevel] = attribute;
            // grow tree if possible
            boolean canExpand = (createdIn != roundCtr);
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new ParallelUctNodeLFTJ(roundCtr, this, attribute);
            }
            // evaluate via recursive invocation or via playout
            ParallelUctNodeLFTJ child = childNodes[action];
            double reward = (child != null) ? child.sample(roundCtr, joinOrder, policy, joinOp, tid) : playout(joinOp, joinOrder);
            // update UCT statistics and return reward
            updateStatistics(action, reward, tid);
            return reward;
        }
    }

    public void getOptimalOrder(int[] order) {
        if (treeLevel < nrAttributes) {
            int bestAction = -1;
            double bestQuality = -1;
            int selectNrActionTries = 0;
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                double accumulatedActionReward = 0;
                int nrActionTries = 0;
                for (int i = 0; i < nrThreads; i++) {
                    accumulatedActionReward += accumulatedReward[i][actionCtr];
                    nrActionTries += nrTries[i][actionCtr];
                }
                // Calculate index of current action
                double meanReward = (nrActionTries > 0) ? accumulatedActionReward / nrActionTries : 0;
                if (meanReward > bestQuality) {
                    bestAction = actionCtr;
                    bestQuality = meanReward;
                    selectNrActionTries = nrActionTries;
                }
            }
            // if number of visits is 0
            if (bestAction >= 0 && selectNrActionTries > 0) {
                order[treeLevel] = nextAttributes[bestAction];
                if (childNodes[bestAction] != null) {
                    childNodes[bestAction].getOptimalOrder(order);
                }
            }
        }
    }

    public void getMostFreqOrder(int[] order) {
        if (treeLevel < nrAttributes) {
            int bestAction = -1;
            int maxTries = -1;
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Calculate index of current action
                int nrActionTries = 0;
                for (int i = 0; i < nrThreads; i++) {
                    nrActionTries += nrTries[i][actionCtr];
                }
                if (nrActionTries > maxTries) {
                    maxTries = nrActionTries;
                    bestAction = actionCtr;
                }
            }
            // if number of visits is 0
            if (bestAction >= 0 && maxTries > 0) {
                order[treeLevel] = nextAttributes[bestAction];
                if (childNodes[bestAction] != null) {
                    childNodes[bestAction].getOptimalOrder(order);
                }
            }
        }
    }
}