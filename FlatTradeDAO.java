package schemaDesign_trades;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FlatTradeDAO implements TradeDAO {

	private final HTable table;
	private final static byte [] ENTRY_FAMILY = Bytes.toBytes("entry");
	
	public FlatTradeDAO(Configuration conf) throws IOException{
		//TODO &&&MJM NOT DE-PLAGGED YET.
		// TODO replace user01 with the user name you are using.
		// Don't forget to create the table in HBase shell first:
		// 		create '/user/user01/Trade_FlatAndWide', 'entry'		
		table = new HTable(conf, "/user/user01/Trade_FlatAndWide");
	}
	
	@Override
	public void close() throws IOException{
		//TODO &&&MJM NOT DE-PLAGGED YET.
		table.close();
	}
	
	@Override
	public void save(Trade trade) throws IOException {
		//TODO &&&MJM NOT DE-PLAGGED YET.
		Put put = new Put(Bytes.toBytes(trade.getAuthorId()));
		put.add(ENTRY_FAMILY, Bytes.toBytes(dateToColumn(trade.getTradeDate())), 
				Bytes.toBytes(trade.getTitle()));
		table.put(put);
	}
	
	private String dateToColumn(Date date ){
		//TODO &&&MJM NOT DE-PLAGGED YET.
		String reversedDateAsStr=
				Long.toString(Long.MAX_VALUE-date.getTime());
		StringBuilder builder = new StringBuilder();
		
		for ( int i = reversedDateAsStr.length(); i < 19; i++){
			builder.append('0');
		}
		
		builder.append(reversedDateAsStr);
		return builder.toString();
	}
	
	public Date columnToDate(String column){
		//TODO &&&MJM NOT DE-PLAGGED YET.
		long reverseStamp = Long.parseLong(column);
		return new Date(Long.MAX_VALUE-reverseStamp);
	}
	
	@Override
	public List<Trade> getTradesByDate(String authorId, Date from,
			//TODO &&&MJM NOT DE-PLAGGED YET.
			Date to) throws IOException {
		List<Trade> trades = new ArrayList<Trade>();
		Get get = new Get(Bytes.toBytes(authorId));
		
		FilterList filters = new FilterList();
		filters.addFilter(new QualifierFilter(CompareOp.LESS_OR_EQUAL, 
				new BinaryComparator(Bytes.toBytes(dateToColumn(from)))));
		filters.addFilter(new QualifierFilter(CompareOp.GREATER_OR_EQUAL, 
				new BinaryComparator(Bytes.toBytes(dateToColumn(to)))));
		
		get.setFilter(filters);
		
		Result result = table.get(get);
		Map<byte[], byte[]> columnValueMap = result.getFamilyMap(ENTRY_FAMILY);
		
		for (Map.Entry<byte[], byte[]> entry : columnValueMap.entrySet()){
			Date publishDate = columnToDate(Bytes.toString(entry.getKey()));
			String title = Bytes.toString(entry.getValue());
			trades.add(new Trade(authorId, title, publishDate));
		}
		
		return trades;
	}

}
