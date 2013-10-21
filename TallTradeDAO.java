package schemaDesign_trades;

import static org.apache.hadoop.hbase.util.Bytes.toLong;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TallTradeDAO implements TradeDAO {

	private final HTable table;
	private final static byte [] baseCF = Bytes.toBytes("CF1");
	private final static byte [] priceCol = Bytes.toBytes("price");
	private final static byte [] volumeCol = Bytes.toBytes("vol");
	
	private final static char delimChar = '_';

	// TODO replace "/user/mapr" with your user directory.
	// Don't forget to create this table in HBase shell first:
	// hbase> create '/user/mapr/trades_tall',{NAME=>'CF1'}
	private final static String tablePath = "/user/mapr/trades_flat";
	
	/**
	 * constructs a TradeDAO based on a tall-narrow implementation
	 * @param conf the HBase configuration
	 * @throws IOException
	 */
	public TallTradeDAO(Configuration conf) throws IOException{
		// TODO &&&MJM: Update instructions to match the lab environment.
		table = new HTable(conf, tablePath);
	}
	
	@Override
	public void close() throws IOException {
		table.close();
	}
	
	@Override
	public void log(Trade trade) throws IOException {
		//TODO &&&MJM NOT DE-PLAGGED YET.
		String rowkey = formRowkey(trade.getSymbol(), trade.getTime());
		
//OLD		Put put = new Put(Bytes.toBytes(trade.getSymbol() + delimChar +  convertForId(trade.getTradeDate())));
		Put put = new Put(Bytes.toBytes(rowkey));

		/* TODO: Implement code to put trade data into the table. 
		put.add(ENTRY_FAMILY, AUTHOR, Bytes.toBytes(trade.getAuthorId()));
		put.add(ENTRY_FAMILY, BLOG_TITLE, Bytes.toBytes(trade.getTitle()));
		put.add(ENTRY_FAMILY, PUBLISH_DATE, Bytes.toBytes(trade.getTradeDate().getTime()));
		 */		
		table.put(put);
	}
	
	/*
	 * creates the rowkey format given a Trade object.
	 * For the tall-narrow implementation, rowkey format = SYMBOL_TIMESTAMP
	 * Example: AMZN_1381396363 
	 */
	private String formRowkey(String symbol, Long time){
		//TODO &&&MJM NOT DE-PLAGGED YET.
		String timeString = String.format("%0d", time);
		String rowkey = symbol + delimChar + timeString; 
				// date.getTime(); 
				// trade.getDate().getTime().toString(); 
		System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey); // TODO &&&MJM Remove this.
		System.out.println("DEBUG formRowkey(): time.getTime class & printable values: " + timeString); // TODO &&&MJM Remove this.
//		System.out.println("DEBUG formRowkey(): date.getTime class & printable values: " + date.getTime() + ", class is LONG."); // TODO &&&MJM Remove this.

		/* TODO Code to reverse the timestamp
		//TODO &&&MJM NOT DE-PLAGGED YET.
		String reversedDateAsStr=
				Long.toString(Long.MAX_VALUE-timestamp);
		StringBuilder builder = new StringBuilder();
		
		for ( int i = reversedDateAsStr.length(); i < 19; i++){
			builder.append('0');
		}
		
		builder.append(reversedDateAsStr);
		return builder.toString();
		*/ 

		
		return rowkey;

// &&&MJM Junk for my Java learning
//		System.out.println("Using DAO: " + testDao.getClass());
// 		String.format("%d", value);		
//		"between [" + DATE_FORMAT.format(start) + "] " +
//		private final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd hh:mm:ss");

		
//		return convertForId(date.getTime());
	}

/* &&&MJM JUNK	
	private String formRowkey(long timestamp){
		//TODO &&&MJM NOT DE-PLAGGED YET.
		String reversedDateAsStr=
				Long.toString(Long.MAX_VALUE-timestamp);
		StringBuilder builder = new StringBuilder();
		
		for ( int i = reversedDateAsStr.length(); i < 19; i++){
			builder.append('0');
		}
		
		builder.append(reversedDateAsStr);
		return builder.toString();
	}
*/
	
	/** 
	 * retrieves all results for a given symbol, between two timestamps  
	 */
	@Override
	public List<Trade> getTradesByDate(String symbol, Long from, Long to) throws IOException{
		System.out.println("DEBUG TallTradeDAO.getTradesByDate(): entered the function."); // TODO &&&MJM Remove this.
	//TODO &&&MJM NOT DE-PLAGGED YET.
		
		// Create a list to store resulting trades
		List<Trade> trades = new ArrayList<Trade>();  
		
//OLD		Scan scan = new Scan(Bytes.toBytes(authorId + KEY_SPLIT_CHAR + convertForId(to)), 
//OLD		Bytes.toBytes(authorId + KEY_SPLIT_CHAR + convertForId(from.getTime()-1)));
		// Scan all applicable rows for the symbol, between given timestamps
		Scan scan = new Scan(Bytes.toBytes(formRowkey(symbol, to)), 
				Bytes.toBytes(formRowkey(symbol, from)) );
		scan.addFamily(baseCF);
		
		ResultScanner scanner = table.getScanner(scan);

		// Iterate through the scanner, and transfer scan results to our list of Trades. 
		// Populate these from scan to trade: Date tradeDate, String tradeSymbol, Float tradePrice, Long tradeVolume
		for (Result result : scanner){

			// Extract the symbol & timestamp from the result's rowkey
//OLD			String title = Bytes.toString(result.getValue(ENTRY_FAMILY, BLOG_TITLE));
			String rowkey = Bytes.toString(result.getRow()); // rowkey format is SYMBOL_TIMESTAMP
			String[] rowkeyTokens = rowkey.split("_"); // tokenize rowkey
//			Date date = new Date(Long.parseLong(rowkeyTokens[1])); // convert the timestamp part into a Date object
			Long time = Long.parseLong(rowkeyTokens[1]); // convert the timestamp part into a Date object
			System.out.println("DEBUG TallTradeDAO.getTradesByDate(): Tokens from result are " + 
					rowkeyTokens[0] + " and " + rowkeyTokens[1]); // TODO &&&MJM Remove this.

			
			// Populate the price & volume into 
//OLD			String title = Bytes.toString(result.getValue(ENTRY_FAMILY, BLOG_TITLE));
//OLD			long created = toLong(result.getValue(ENTRY_FAMILY, PUBLISH_DATE));
			Float price = Bytes.toFloat(result.getValue(priceCol, baseCF));
			Long volume = Bytes.toLong(result.getValue(volumeCol, baseCF));

			// Add a new trade to the list of trades
//OLD			trades.add(new Trade(authorId, title, new Date(created)));
			trades.add(new Trade(time, rowkeyTokens[0], price, volume));
		}
		return trades;
	}

	// TODO &&&MJM: Remove this??
	@Override
	public List<Trade> getTradesBySymbol(String symbol) throws IOException{
		List<Trade> trades = new ArrayList<Trade>();

		// TODO: Add code to scan and extract trades here.

		return trades;
	}

	
}
