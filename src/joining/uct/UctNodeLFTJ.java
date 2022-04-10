package joining.uct;

import joining.ParallelLFTJ;
import joining.join.DynamicMWJoin;
import joining.result.JoinResult;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;

import config.JoinConfig;

/**
 * Represents node in UCT search tree.
 *
 * @author immanueltrummer
 */
public class UctNodeLFTJ {
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
    final List<Integer> priorityActions;
    /**
     * Assigns each action index to child node.
     */
    public final UctNodeLFTJ[] childNodes;
    /**
     * Number of times this node was visited.
     */
    int nrVisits = 0;
    /**
     * Number of times each action was tried out.
     */
    public final int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    public final double[] accumulatedReward;
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
     * Evaluates a given join order and accumulates results.
     */
    final ParallelLFTJ joinOp;
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


    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     current round number
     * @param query        the query which is optimized
     * @param useHeuristic whether to avoid Cartesian products
     */
    public UctNodeLFTJ(long roundCtr, QueryInfo query,
                       boolean useHeuristic, ParallelLFTJ joinOp) {
        // Count node generation
//        ++JoinStats.nrUctNodes;
        this.query = query;
        this.nrAttributes = query.equiJoinAttribute.size();
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrAttributes;
        priorityActions = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            priorityActions.add(actionCtr);
        }
        childNodes = new UctNodeLFTJ[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        joinedAttributes = new HashSet<Integer>();
        unjoinedAttributes = new ArrayList<>();
        nextAttributes = new int[nrAttributes];
        for (int attributeCtr = 0; attributeCtr < nrAttributes; ++attributeCtr) {
            unjoinedAttributes.add(attributeCtr);
            nextAttributes[attributeCtr] = attributeCtr;
        }
        this.joinOp = joinOp;
        this.useHeuristic = useHeuristic;
        recommendedActions = new HashSet<Integer>();
        for (int action = 0; action < nrActions; ++action) {
            accumulatedReward[action] = 0;
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
    public UctNodeLFTJ(long roundCtr, UctNodeLFTJ parent, int joinedAttribute) {
        // Count node generation
//        ++JoinStats.nrUctNodes;
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new UctNodeLFTJ[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
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
            accumulatedReward[actionCtr] = 0;
            nextAttributes[actionCtr] = unjoinedAttributes.get(actionCtr);
        }
        this.joinOp = parent.joinOp;
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
//            System.out.println("recommendedActions:" + recommendedActions);
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
        priorityActions = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
                priorityActions.add(actionCtr);
            }
        }
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
        if (!priorityActions.isEmpty()) {
            int nrUntried = priorityActions.size();
            int actionIndex = random.nextInt(nrUntried);
            int action = priorityActions.get(actionIndex);
            // Remove from untried actions and return
            priorityActions.remove(actionIndex);
            return action;
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
                double meanReward = accumulatedReward[action] / nrTries[action];
                double exploration = Math.sqrt(Math.log(nrVisits) / nrTries[action]);
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
    void updateStatistics(int selectedAction, double reward) {
        ++nrVisits;
        ++nrTries[selectedAction];
        accumulatedReward[selectedAction] += reward;
    }

    /**
     * Randomly complete join order with remaining attributes,
     * invoke evaluation, and return obtained reward.
     *
     * @param joinOrder partially completed join order
     * @return obtained reward
     */
    double playout(int[] joinOrder) throws Exception {
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
//                    System.out.println("attribute:" + attribute + ", newlyJoined:" + newlyJoined + ", connect:" + query.connectedAttribute(newlyJoined, attribute));
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
                       SelectionPolicy policy) throws Exception {
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // leaf node - evaluate join order and return reward
            return joinOp.execute(joinOrder);
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy);
            int attribute = nextAttributes[action];
            joinOrder[treeLevel] = attribute;
            // grow tree if possible
            boolean canExpand = (createdIn != roundCtr);
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new UctNodeLFTJ(roundCtr, this, attribute);
            }
            // evaluate via recursive invocation or via playout
            UctNodeLFTJ child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, policy):
                    playout(joinOrder);
            // update UCT statistics and return reward
            updateStatistics(action, reward);
            return reward;
        }
    }

    public void getOptimalOrder(int[] order) {
        if (treeLevel < nrAttributes) {
            int bestAction = -1;
            double bestQuality = -1;
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Calculate index of current action
                double meanReward = (nrTries[actionCtr] > 0) ? accumulatedReward[actionCtr] / nrTries[actionCtr] : 0;
                if (meanReward > bestQuality) {
                    bestAction = actionCtr;
                    bestQuality = meanReward;
                }
            }
            // if number of visits is 0
            if (bestAction >= 0 && nrTries[bestAction] > 0) {
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
                if (nrTries[actionCtr] > maxTries) {
                    maxTries = nrTries[actionCtr];
                    bestAction = actionCtr;
                }
            }
            order[treeLevel] = nextAttributes[bestAction];
            if (childNodes[bestAction] != null) {
                childNodes[bestAction].getOptimalOrder(order);
            }
        }
    }
}