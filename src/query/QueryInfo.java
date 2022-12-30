package query;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.JoinConfig;
import config.LoggingConfig;
import config.NamingConfig;
import expressions.ExpressionInfo;
import expressions.aggregates.AggInfo;
import expressions.typing.ExpressionScope;
import joining.join.wcoj.ArrayUtilities;
import joining.plan.AttributeOrder;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import query.from.FromUtil;
import query.select.SelectUtil;

import org.apache.commons.lang3.StringUtils;
import util.Pair;

/**
 * Contains information on the query to execute.
 *
 * 
 */
public class QueryInfo {
    /**
     * Plain select statement query to execute.
     */
    public final PlainSelect plainSelect;
    /**
     * Whether the query execution should be visualized.
     */
    public final boolean explain;
    /**
     * Plot at most that many plots.
     */
    public final int plotAtMost;
    /**
     * Generate a plot every so many samples.
     */
    public final int plotEvery;
    /**
     * Generate plots in this directory if activated.
     */
    public final String plotDir;
    /**
     * Number of table instances in FROM clause.
     */
    public int nrJoined = 0;
    /**
     * Number of join attribute
     */
    public int nrAttribute = 0;
    /**
     * All aliases in the query's FROM clause.
     */
    public String[] aliases;
    /**
     * Maps each alias to its alias index.
     */
    public Map<String, Integer> aliasToIndex =
            new HashMap<String, Integer>();
    /**
     * Maps aliases to associated table name.
     */
    public Map<String, String> aliasToTable =
            new HashMap<String, String>();
    /**
     * Maps unique column names to associated table aliases.
     */
    public Map<String, String> columnToAlias =
            new HashMap<String, String>();
    /**
     * Maps from aliases to SQL expressions.
     */
    public Map<String, Expression> aliasToExpression =
            new HashMap<String, Expression>();
    /**
     * Maps select clause items to corresponding alias.
     */
    public Map<ExpressionInfo, String> selectToAlias =
            new HashMap<>();
    /**
     * Maps column reference to column info.
     */
    public Map<ColumnRef, ColumnInfo> colRefToInfo =
            new HashMap<ColumnRef, ColumnInfo>();
    /**
     * Expressions that appear in the SELECT clause
     * with associated meta-data.
     */
    public List<ExpressionInfo> selectExpressions =
            new ArrayList<ExpressionInfo>();
    /**
     * Stores information on predicates in WHERE clause.
     * Each expression integrates all predicates that
     * refer to the same table instances.
     */
    public List<ExpressionInfo> wherePredicates =
            new ArrayList<ExpressionInfo>();
    /**
     * List of expressions that correspond to unary
     * predicates.
     */
    public List<ExpressionInfo> unaryPredicates =
            new ArrayList<ExpressionInfo>();
    /**
     * Sets of alias indices that are connected via
     * join predicates - used to quickly determined
     * eligible tables for continuing join orders
     * while avoiding Cartesian product joins.
     */
    public Set<Set<Integer>> joinedIndices = new HashSet<>();
    /**
     * Columns that are involved in binary equi-join
     * predicates (i.e., we may want to create hash
     * indices for them during pre-processing).
     */
    public Set<ColumnRef> equiJoinCols = new HashSet<>();
    /**
     * Contains pairs of columns that are connected
     * via binary equality join conditions.
     */
    public Set<Set<ColumnRef>> equiJoinPairs = new HashSet<>();
    /**
     * Equivalence classes of columns involved in equi-joins.
     */
    public Set<Set<ColumnRef>> equiJoinClasses = new HashSet<>();
    /**
     * Equivalence classes of attribute involved in equi-joins.
     */
    public List<Set<ColumnRef>> equiJoinAttribute = new ArrayList<>();
    /**
     * List of expressions that correspond to
     * equality join predicates.
     */
    public List<ExpressionInfo> equiJoinPreds =
            new ArrayList<>();
    /**
     * List of expressions that correspond to
     * non-equi join predicates.
     */
    public List<ExpressionInfo> nonEquiJoinPreds =
            new ArrayList<ExpressionInfo>();
    /**
     * Expressions that appear in GROUP-BY clause with
     * associated meta-data.
     */
    public List<ExpressionInfo> groupByExpressions =
            new ArrayList<ExpressionInfo>();
    /**
     * Expressions that appear in ORDER-BY clause with
     * associated meta-data.
     */
    public List<ExpressionInfo> orderByExpressions =
            new ArrayList<ExpressionInfo>();
    /**
     * Whether we sort i-th order-by element in
     * ascending (as opposed to descending) order.
     */
    public boolean[] orderByAsc;
    /**
     * HAVING clause expression with meta-data.
     */
    public ExpressionInfo havingExpression;
    /**
     * Set of columns required for join processing.
     */
    public Set<ColumnRef> colsForJoins =
            new HashSet<ColumnRef>();
    /**
     * Set of columns required for post-processing.
     */
    public Set<ColumnRef> colsForPostProcessing =
            new HashSet<ColumnRef>();
    /**
     * Aggregate expressions in SELECT clause.
     */
    public Set<AggInfo> aggregates = new HashSet<>();
    /**
     * Class of aggregation query.
     */
    public final AggregationType aggregationType;
    /**
     * Number of tuples specified in LIMIT clause, if any,
     * or -1 if no LIMIT specified.
     */
    public final int limit;

    /**
     * Extract information from the FROM clause (e.g.,
     * all tables referenced with their aliases, the
     * number of items in the from clause etc.).
     */
    void extractFromInfo() throws Exception {
        // Check if FROM clause exists
        if (plainSelect.getFromItem() == null) {
            throw new SQLexception("Error - no FROM clause");
        }
        // Extract all from items
        List<FromItem> fromItems = FromUtil.allFromItems(plainSelect);
        nrJoined = fromItems.size();
        // Extract tables from items
        aliases = new String[nrJoined];
        for (int i = 0; i < nrJoined; ++i) {
            FromItem fromItem = fromItems.get(i);
            // Retrieve information on associated table
            Table table = (Table) fromItem;
            String alias = table.getAlias() != null ?
                    table.getAlias().getName().toLowerCase() :
                    table.getName().toLowerCase();
            String tableName = table.getName().toLowerCase();
            // Verify that table is known
            if (!CatalogManager.currentDB.
                    nameToTable.containsKey(tableName)) {
                throw new SQLexception("Error - table " +
                        tableName + " is unknown");
            }
            // Verify that alias is unique
            for (int j = 0; j < i; ++j) {
                if (aliases[j].equals(alias)) {
                    throw new SQLexception("Error - "
                            + "alias " + alias + " is "
                            + "not unique");
                }
            }
            // Register mapping from alias to table
            aliasToTable.put(alias, tableName);
            // Register mapping from index to alias
            aliases[i] = alias;
            // Register mapping from alias to index
            aliasToIndex.put(alias, i);
            // Extract columns with types
            for (ColumnInfo colInfo : CatalogManager.currentDB.
                    nameToTable.get(tableName).nameToCol.values()) {
                String colName = colInfo.name;
                colRefToInfo.put(new ColumnRef(alias, colName), colInfo);
            }
        }
    }

    /**
     * Adds implicit table references via unique column names.
     */
    void addImplicitRefs() throws Exception {
        for (Entry<String, String> entry : aliasToTable.entrySet()) {
            String alias = entry.getKey();
            String table = entry.getValue();
            // Disallow implicit references for tables added during
            // unnesting to represent the results of sub-queries.
            if (!table.startsWith(NamingConfig.SUBQUERY_PRE)) {
                for (ColumnInfo columnInfo : CatalogManager.currentDB.
                        nameToTable.get(table).nameToCol.values()) {
                    String columnName = columnInfo.name;
                    if (columnToAlias.containsKey(columnName)) {
                        columnToAlias.put(columnName, null);
                    } else {
                        columnToAlias.put(columnName, alias);
                    }
                }
            }
        }
    }

    /**
     * Adds all columns of a given table to a list of select clause items.
     *
     * @param tblAlias    add columns for this table alias
     * @param selectItems add columns to this select item list
     */
    void addAllColumns(String tblAlias, List<SelectItem> selectItems) {
        String tableName = aliasToTable.get(tblAlias);
        TableInfo tblInfo = CatalogManager.currentDB.nameToTable.get(tableName);
        Table table = new Table(tblAlias);
        for (String colName : tblInfo.columnNames) {
            Column column = new Column(table, colName);
            selectItems.add(new SelectExpressionItem(column));
        }
    }

    /**
     * Resolve wildcards in SELECT clause and assign aliases.
     */
    void treatSelectClause() throws Exception {
        // Expand SELECT clause into list of simple select items
        // (i.e., resolve all wildcards).
        List<SelectItem> selectItems = new ArrayList<>();
        // Expand SELECT clause into list of expressions
        for (SelectItem selectItem : plainSelect.getSelectItems()) {
            if (selectItem instanceof SelectExpressionItem) {
                selectItems.add(selectItem);
            } else if (selectItem instanceof AllTableColumns) {
                AllTableColumns allTblCols = (AllTableColumns) selectItem;
                String aliasName = allTblCols.getTable().getName();
                addAllColumns(aliasName, selectItems);
            } else if (selectItem instanceof AllColumns) {
                for (String tblAlias : aliases) {
                    addAllColumns(tblAlias, selectItems);
                }
            } else {
                System.out.println("Unknown type of select "
                        + "clause item - ignoring");
            }
        }
        // Name items in select clause
        Map<Expression, String> selectExprToAlias =
                SelectUtil.assignAliases(selectItems);
        // Update fields associated with select clause
        for (SelectItem selectItem : selectItems) {
            SelectExpressionItem exprItem = (SelectExpressionItem) selectItem;
            Expression expr = exprItem.getExpression();
            String alias = selectExprToAlias.get(expr);
            ExpressionInfo exprInfo = new ExpressionInfo(this, expr);
            selectExpressions.add(exprInfo);
            aliasToExpression.put(alias, exprInfo.finalExpression);
            selectToAlias.put(exprInfo, alias);
        }
    }

    /**
     * Assuming the given expression is a binary equality join,
     * returns the two joined columns. Returns null otherwise.
     *
     * @param exprInfo potential equi-join predicate
     */
    Set<ColumnRef> extractEquiJoinCols(ExpressionInfo exprInfo) {
        Expression expr = exprInfo.finalExpression;
        // Check for equality predicate
        if (expr instanceof EqualsTo) {
            EqualsTo equalsExpr = (EqualsTo) expr;
            Expression left = equalsExpr.getLeftExpression();
            Expression right = equalsExpr.getRightExpression();
            // Is it an equality between two columns?
            if (left instanceof Column && right instanceof Column) {
                Column leftCol = (Column) left;
                Column rightCol = (Column) right;
                ColumnRef leftRef = new ColumnRef(
                        leftCol.getTable().getName(),
                        leftCol.getColumnName());
                ColumnRef rightRef = new ColumnRef(
                        rightCol.getTable().getName(),
                        rightCol.getColumnName());
                // Do those columns belong to different tables?
                if (!leftRef.aliasName.equals(rightRef.aliasName)) {
                    Set<ColumnRef> colPair = new HashSet<>();
                    colPair.add(leftRef);
                    colPair.add(rightRef);
                    return colPair;
                }
            }
        }
        return null;
    }

    /**
     * Extracts predicates from normalized WHERE clause, separating
     * predicates by the tables they refer to.
     */
    void extractPredicates() throws Exception {
        Expression where = plainSelect.getWhere();
        if (where != null) {
            // Normalize WHERE clause and transform into CNF
            ExpressionInfo whereInfo = new ExpressionInfo(this, where);
            // Decompose into conjuncts
            List<Expression> conjuncts = whereInfo.conjuncts;
            // Merge conditions that refer to the same tables
            Map<Set<String>, Expression> tablesToCondition =
                    new HashMap<Set<String>, Expression>();
            for (Expression conjunct : conjuncts) {
                ExpressionInfo conjunctInfo =
                        new ExpressionInfo(this, conjunct);
                Set<String> tables = conjunctInfo.aliasesMentioned;
                if (tablesToCondition.containsKey(tables)) {
                    Expression prior = tablesToCondition.get(tables);
                    Expression curAndPrior =
                            new AndExpression(prior, conjunct);
                    tablesToCondition.put(tables, curAndPrior);
                } else {
                    tablesToCondition.put(tables, conjunct);
                }
            }
            // Create predicates from merged expressions
            for (Expression condition : tablesToCondition.values()) {
                ExpressionInfo pred = new ExpressionInfo(this, condition);
                wherePredicates.add(pred);
            }
            // Separate into unary and join predicates
            for (ExpressionInfo exprInfo : wherePredicates) {
                if (exprInfo.aliasesMentioned.size() == 1) {
                    unaryPredicates.add(exprInfo);
                } else {
                    // Join predicate - calculate mentioned alias indexes
                    Set<Integer> aliasIdxs = new HashSet<Integer>();
                    for (String alias : exprInfo.aliasesMentioned) {
                        aliasIdxs.add(aliasToIndex.get(alias));
                    }
                    joinedIndices.add(aliasIdxs);
                    // Separate into equi- and non-equi join predicates
                    Expression nonEquiPred = null;
                    for (Expression conjunct : exprInfo.conjuncts) {
                        ExpressionInfo curInfo = new ExpressionInfo(this, conjunct);
                        Set<ColumnRef> curEquiJoinCols = extractEquiJoinCols(curInfo);
                        if (curEquiJoinCols != null) {
                            equiJoinCols.addAll(curEquiJoinCols);
                            equiJoinPairs.add(curEquiJoinCols);
                            equiJoinPreds.add(curInfo);
                        } else {
                            nonEquiPred = nonEquiPred == null ? conjunct :
                                    new AndExpression(nonEquiPred, conjunct);
                        }
                    } // over conjuncts of join predicates
                    // Add non-equi join predicates if any
                    if (nonEquiPred != null) {
                        nonEquiJoinPreds.add(new ExpressionInfo(this, nonEquiPred));
                    }
                } // if join predicate
            } // over where conjuncts
        } // if where clause
    }

    /**
     * Partition columns involved in equality joins
     * into equivalence classes.
     */
    void partitionEquiJoinCols() {
        // Initialize equivalence classes: each column
        // forms an equivalence class on its own.
        for (ColumnRef colRef : equiJoinCols) {
            Set<ColumnRef> colClass = Collections.singleton(colRef);
            equiJoinClasses.add(colClass);
        }
        // Continue until no more merge operations
        boolean mergedClasses = false;
        do {
            // No classes merged in this iteration
            mergedClasses = false;
            // Equivalence classes after this iteration
            Set<Set<ColumnRef>> nextClasses = new HashSet<>();
            // Iterate over current equivalence classes
            for (Set<ColumnRef> equiJoinClass : equiJoinClasses) {
                // Copy before changing
                Set<ColumnRef> newEquiJoinClass = new HashSet<>();
                newEquiJoinClass.addAll(equiJoinClass);
                // Iterate over column pairs in equi joins
                for (Set<ColumnRef> equiJoinPair : equiJoinPairs) {
                    // Iterate over columns in predicate
                    for (ColumnRef colRef : equiJoinPair) {
                        if (equiJoinClass.contains(colRef)) {
                            mergedClasses = mergedClasses ||
                                    newEquiJoinClass.addAll(
                                            equiJoinPair);
                        }
                    }
                }
                // Add potentially changed class to new set
                nextClasses.add(newEquiJoinClass);
            }
            // Next classes become current classes
            equiJoinClasses = nextClasses;
        } while (mergedClasses);
    }

    /**
     * Add unary predicates that connect two columns in
     * the same table via an equality predicate, based
     * on predicate equivalence classes.
     */
    void addUnaryEquiPreds() throws Exception {
        // Iterate over predicate equivalence classes
        for (Set<ColumnRef> equiClass : equiJoinClasses) {
            // Map table alias to first column in equivalence class
            Map<String, ColumnRef> aliasToCol = new HashMap<>();
            for (ColumnRef colRef : equiClass) {
                aliasToCol.put(colRef.aliasName, colRef);
            }
            // Form unary predicates if possible
            Iterator<ColumnRef> colIter = equiClass.iterator();
            while (colIter.hasNext()) {
                ColumnRef colRef = colIter.next();
                String alias = colRef.aliasName;
                // Have other column on same table?
                if (aliasToCol.containsKey(alias) &&
                        !aliasToCol.get(alias).equals(colRef)) {
                    ColumnRef otherColRef = aliasToCol.get(alias);
                    // Create equality predicate expression
                    Table aliasTable = new Table(alias);
                    Column col = new Column(aliasTable,
                            colRef.columnName);
                    Column otherCol = new Column(aliasTable,
                            otherColRef.columnName);
                    EqualsTo equalsTo = new EqualsTo();
                    equalsTo.setLeftExpression(col);
                    equalsTo.setRightExpression(otherCol);
                    ExpressionInfo equalsToExpr =
                            new ExpressionInfo(this, equalsTo);
                    unaryPredicates.add(equalsToExpr);
                    // Remove prior join predicates - TODO
                    colIter.remove();
                }
            }
        }
    }

    /**
     * Adds expressions in the GROUP-By clause (if any).
     */
    void treatGroupBy() throws Exception {
        if (plainSelect.getGroupByColumnReferences() != null) {
            for (Expression groupExpr :
                    plainSelect.getGroupByColumnReferences()) {
                groupByExpressions.add(new ExpressionInfo(this, groupExpr));
            }
            // Verify that select clause and group-by clause
            // are consistent (each entry in the select clause
            // must be either an aggregate or an expression that
            // appears in the group-by clause).
            Set<String> groupByStrings = new HashSet<>();
            for (ExpressionInfo groupExpr : groupByExpressions) {
                groupByStrings.add(groupExpr.finalExpression.toString());
            }
            for (ExpressionInfo selectExpr : selectExpressions) {
                if (!selectExpr.resultScope.equals(ExpressionScope.PER_GROUP) &&
                        !groupByStrings.contains(selectExpr.finalExpression.toString())) {
                    throw new SQLexception("Error - select item " + selectExpr +
                            " is neither aggregate nor does it appear in group by " +
                            "clause");
                }
            }
        } // if group-by clause present
    }

    /**
     * Adds expression in HAVING clause.
     */
    void treatHaving() throws Exception {
        Expression having = plainSelect.getHaving();
        if (having != null) {
            havingExpression = new ExpressionInfo(this, having);
        } else {
            havingExpression = null;
        }
    }

    /**
     * Adds expression in ORDER BY clause.
     */
    void treatOrderBy() throws Exception {
        List<OrderByElement> orderElements = plainSelect.getOrderByElements();
        if (orderElements != null) {
            int nrOrderElements = orderElements.size();
            orderByAsc = new boolean[nrOrderElements];
            for (int orderCtr = 0; orderCtr < nrOrderElements; ++orderCtr) {
                OrderByElement orderElement = orderElements.get(orderCtr);
                Expression expr = orderElement.getExpression();
                ExpressionInfo exprInfo = new ExpressionInfo(this, expr);
                orderByExpressions.add(exprInfo);
                boolean isAscending = orderElement.isAsc();
                orderByAsc[orderCtr] = isAscending;
            }
        }
    }

    /**
     * Collect aggregates from SELECT and HAVING clause.
     */
    void collectAggregates() throws Exception {
        // Collect expressions that may contain aggregates
        List<ExpressionInfo> exprsWithAggs = new ArrayList<>();
        exprsWithAggs.addAll(selectExpressions);
        if (havingExpression != null) {
            exprsWithAggs.add(havingExpression);
        }
        // Collect aggregates with additional information
        for (ExpressionInfo exprWithAgg : exprsWithAggs) {
            for (Function agg : exprWithAgg.aggregates) {
                aggregates.add(new AggInfo(this, agg));
            }
        }
    }

    /**
     * Collects columns required for steps after pre-processing.
     */
    void collectRequiredCols() {
        colsForJoins.addAll(equiJoinCols);
        colsForJoins.addAll(extractCols(nonEquiJoinPreds));
        colsForPostProcessing.addAll(extractCols(selectExpressions));
        colsForPostProcessing.addAll(extractCols(groupByExpressions));
        if (havingExpression != null) {
            colsForPostProcessing.addAll(havingExpression.columnsMentioned);
        }
        colsForPostProcessing.addAll(extractCols(orderByExpressions));
        // Add dummy column if no columns are selected for post-processing
        // (otherwise certain queries would not be treated correctly,
        // e.g. if select clause contains constant expressions).
        if (colsForPostProcessing.isEmpty()) {
            // (assumes from clause)
            String alias = aliases[0];
            String table = aliasToTable.get(alias);
            TableInfo tableInfo = CatalogManager.
                    currentDB.nameToTable.get(table);
            // If at least one table in the from clause
            // has no columns then the join result is
            // empty so no dummy columns are required.
            if (!tableInfo.columnNames.isEmpty()) {
                String column = tableInfo.columnNames.get(0);
                ColumnRef colRef = new ColumnRef(alias, column);
                colsForPostProcessing.add(colRef);
            }
        }
    }

    /**
     * Extracts a list of all columns mentioned in a list of
     * expressions.
     *
     * @param expressions list of expressions to extract columns from
     * @return set of references to mentioned columns
     */
    static Set<ColumnRef> extractCols(List<ExpressionInfo> expressions) {
        Set<ColumnRef> colRefs = new HashSet<ColumnRef>();
        for (ExpressionInfo expr : expressions) {
            colRefs.addAll(expr.columnsMentioned);
        }
        return colRefs;
    }

    /**
     * Determines query aggregation type.
     *
     * @return aggregation type of this query
     */
    AggregationType getAggregationType() {
        if (aggregates.isEmpty()) {
            // No aggregates means no aggregation
            return AggregationType.NONE;
        } else if (groupByExpressions.isEmpty()) {
            // Aggregates but no group by
            return AggregationType.ALL_ROWS;
        } else {
            // Aggregates and group by
            return AggregationType.GROUPS;
        }
    }

    /**
     * Returns true if there it at least one join predicate
     * connecting the set of items in the FROM clause to the
     * single item. We assume that the new table is not in
     * the set of already joined tables.
     *
     * @param aliasIndices indexes of aliases already joined
     * @param newIndex     index of new alias to check
     * @return true iff join predicates connect
     */
    public boolean connected(Set<Integer> aliasIndices, int newIndex) {
        // Resulting join indices if selecting new table for join
        Set<Integer> indicesAfterJoin = new HashSet<Integer>();
        indicesAfterJoin.addAll(aliasIndices);
        indicesAfterJoin.add(newIndex);
        // Is there at least one connecting join predicate?
        for (Set<Integer> joined : joinedIndices) {
            if (indicesAfterJoin.containsAll(joined) &&
                    joined.contains(newIndex)) {
                return true;
            }
        }
        return false;
    }

	/**
	 * Returns true if there it at least one join predicate
	 * connecting the set of items in the FROM clause to the
	 * single item. We assume that the new table is not in
	 * the set of already joined tables.
	 *
	 */
    public boolean connectedAttribute(Set<Integer> aliasIndices, int newAttribute) {
        // Resulting join indices if selecting new table for join
        Set<ColumnRef> toJoinColumns = equiJoinAttribute.get(newAttribute);
        Set<String> toJoinTable = new HashSet<>();
        for (ColumnRef column : toJoinColumns) {
            toJoinTable.add(column.aliasName);
        }
        for (Integer index : aliasIndices) {
            Set<ColumnRef> joinColumns = equiJoinAttribute.get(index);
            for (ColumnRef joinColumn : joinColumns) {
                if (toJoinTable.contains(joinColumn.aliasName)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isValidAttributeOrder(int[] attributeOrder) {
        Set<Integer> currentAttributes = new HashSet<>();
        currentAttributes.add(attributeOrder[0]);
        for (int i = 1; i < attributeOrder.length ;i++) {
            int attribute = attributeOrder[i];
            if (connectedAttribute(currentAttributes, attribute)) {
                currentAttributes.add(attribute);
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Concatenates string representations of given expression
     * list, using the given separator.
     *
     * @param expressions list of expressions to concatenate
     * @param separator   separator to insert between elements
     * @return concatenation string
     */
    String concatenateExprs(List<ExpressionInfo> expressions, String separator) {
        List<String> toConcat = new ArrayList<String>();
        for (ExpressionInfo expr : expressions) {
            toConcat.add(expr.toString());
        }
        return StringUtils.join(toConcat, separator);
    }

    /**
     * Prints out log entry about query analysis
     * if corresponding logging flag is set.
     *
     * @param logEntry text to display if logging is activated
     */
    static void log(String logEntry) {
        if (LoggingConfig.QUERY_ANALYSIS_VERBOSE) {
            System.out.println(logEntry);
        }
    }

    /**
     * Extracts the limit on the number of result tuples
     * and returns -1 if none specified.
     *
     * @param plainSelect input query
     * @return result tuple limit or -1 if none specified
     * @throws Exception
     */
    static int getLimit(PlainSelect plainSelect) throws Exception {
        Limit limitObj = plainSelect.getLimit();
        // No limit specified?
        if (limitObj == null) {
            return -1;
        }
        Expression limitExpr = limitObj.getRowCount();
        // No row limit specified?
        if (limitExpr == null) {
            return -1;
        }
        // Only constant row limits are supported
        if (!(limitExpr instanceof LongValue)) {
            throw new SQLexception("Error - only constant limits supported");
        } else {
            return (int) (((LongValue) limitExpr).getValue());
        }
    }

    /**
     * Analyzes a select query to prepare processing.
     *
     * @param plainSelect a plain select query
     * @param explain     whether this is an explain query
     * @param plotAtMost  plot at most that many plots (in explain mode)
     * @param plotEvery   generate one plot after that many samples (in explain mode)
     * @param plotDir     add plots to this directory (in explain mode)
     */
    public QueryInfo(PlainSelect plainSelect, boolean explain,
                     int plotAtMost, int plotEvery, String plotDir) throws Exception {
        log("Input query: " + plainSelect);
        this.plainSelect = plainSelect;
        this.explain = explain;
        this.plotAtMost = plotAtMost;
        this.plotEvery = plotEvery;
        this.plotDir = plotDir;
        // Extract information in FROM clause
        extractFromInfo();
        log("Alias -> table: " + aliasToTable);
        log("Column info: " + colRefToInfo);
        // Add implicit references to aliases
        addImplicitRefs();
        log("Unique column name -> alias: " + columnToAlias);
        // Resolve wildcards and add aliases for SELECT clause
        treatSelectClause();
        log("Select expressions: " + selectExpressions);
        log("Select aliases: " + selectToAlias);
        log("Alias to expression: " + aliasToExpression);
        // Extract predicates in WHERE clause
        extractPredicates();
        partitionEquiJoinCols();
        addUnaryEquiPreds();
        log("Unary predicates: " + unaryPredicates);
        log("Equi join cols: " + equiJoinCols);
        log("Equi join pairs: " + equiJoinPairs);
        log("Equi join preds: " + equiJoinPreds);
        log("Equivalent cols: " + equiJoinClasses);
        log("Other join preds: " + nonEquiJoinPreds);
        // Add expressions in GROUP BY clause
        treatGroupBy();
        log("GROUP BY expressions: " + groupByExpressions);
        // Add expression in HAVING clause
        treatHaving();
        log("HAVING clause: " + (havingExpression != null ? havingExpression : "none"));
        // Adds expressions in ORDER BY clause
        treatOrderBy();
        log("ORDER BY expressions: " + orderByExpressions);
        // Collect required columns
        collectRequiredCols();
        log("Required cols for joins: " + colsForJoins);
        log("Required for post-processing: " + colsForPostProcessing);
        // Collect aggregates
        collectAggregates();
        log("Extracted aggregates: " + aggregates);
        // Set aggregation type
        aggregationType = getAggregationType();
        log("Aggregation type:\t" + aggregationType);
        // Set result tuple limit
        limit = getLimit(plainSelect);
        log("Limit:\t" + limit);
        equiJoinAttribute.addAll(equiJoinClasses);
        nrAttribute = equiJoinAttribute.size();
//
//        if (JoinConfig.CACHE_ENABLE) {
//            JoinCache.hashMetaData = new HashMap<>();
//            // cache information of all orders
//            for (int[] order : ArrayUtilities.permutations(nrAttribute)) {
//                if (this.isValidAttributeOrder(order)) {
////                    System.out.println("order:" + Arrays.toString(order));
//                    HashMap<Integer, Pair<Set<Integer>, Set<Integer>>> cacheInfoCurrentOrder = new HashMap<>();
//                    // for each validate order
//                    List<Set<ColumnRef>> varOrder = Arrays.stream(order).boxed().map(i -> this.equiJoinAttribute.get(i)).collect(Collectors.toList());
//                    // for cache
//                    // gather table in each attribute
//                    Map<String, Set<Integer>> joinTableToAttributeIdx = new HashMap<>();
//                    for (int aliasCtr = 0; aliasCtr < this.nrAttribute; ++aliasCtr) {
//                        for (ColumnRef columnRef : varOrder.get(aliasCtr)) {
//                            joinTableToAttributeIdx.putIfAbsent(columnRef.aliasName, new HashSet<>());
//                            joinTableToAttributeIdx.get(columnRef.aliasName).add(aliasCtr);
//                        }
//                    }
//
//                    // construct dependency set
//                    Map<Integer, Set<Integer>> dependencySet = new HashMap<>();
//                    for (int aliasCtr = this.nrAttribute - 1; aliasCtr > 0; --aliasCtr) {
//                        Set<Integer> dependency = new HashSet<>();
//                        for (ColumnRef columnRef : varOrder.get(aliasCtr)) {
//                            Set<Integer> attributeIdx = joinTableToAttributeIdx.get(columnRef.aliasName);
//                            dependency.addAll(attributeIdx);
//                        }
//                        if (aliasCtr < this.nrAttribute - 1) {
//                            dependency.addAll(dependencySet.get(aliasCtr + 1));
//                        }
//                        // remove key in front of this key
//                        for (int i = this.nrAttribute - 1; i >= aliasCtr; --i) {
//                            dependency.remove(i);
//                        }
//                        dependencySet.put(aliasCtr, dependency);
//                    }
//
//                    // collect which attribute to cache
//                    for (int aliasCtr = 1; aliasCtr < this.nrAttribute; aliasCtr++) {
//                        // dependency
//                        Set<Integer> dependency = dependencySet.get(aliasCtr);
//                        if (dependency.size() < aliasCtr) {
//                            // validate cache key and value
//                            Set<Integer> attributeLater = new HashSet<>();
//                            for (int i = aliasCtr; i < nrAttribute; i++) {
//                                attributeLater.add(i);
//                            }
//
//                            // map the local order to the global order
//                            // dependency: example, key: 1,3, value: 2, 4
//                            Set<Integer> globalKey = dependency.stream().map(key -> order[key]).collect(Collectors.toSet());
//                            Set<Integer> globalValue = attributeLater.stream().map(key -> order[key]).collect(Collectors.toSet());
//
//                            cacheInfoCurrentOrder.put(aliasCtr - 1, new Pair(globalKey, globalValue));
//
////                            System.out.println("ctr:" + aliasCtr + ", key:" + globalKey + ", value:" + globalValue);
////                            System.out.println("key+++++");
////                            for (int k : globalKey) {
////                                System.out.println(this.equiJoinAttribute.get(k));
////                            }
////
////                            System.out.println("value+++++");
////                            for (int v : globalValue) {
////                                System.out.println(this.equiJoinAttribute.get(v));
////                            }
////                            System.out.println("");
//
//                        }
//                    }
//
//                    JoinCache.hashMetaData.put(new AttributeOrder(order), cacheInfoCurrentOrder);
//                }
//            }
//            JoinCache.cacheResult = new ConcurrentHashMap<>();
//            // finish cache information of all orders
//            System.out.println("JoinCache.hashMetaData:" + JoinCache.hashMetaData);
//        }
    }
}