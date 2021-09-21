package joining.join.wcoj;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Test;

import query.QueryInfo;

class LFTJiterTest {

	@Test
	void test() throws Exception {
		String sql = "select * from a";
		Statement statement = CCJSqlParserUtil.parse(sql);
		Select select = (Select)statement;
		PlainSelect pSelect = (PlainSelect)select.getSelectBody();
		QueryInfo query = new QueryInfo(pSelect, false, 0, 0, null);
		//LFTJiter iter = new LFTJiter(query, context, aliasID, globalVarOrder)
	}

}
