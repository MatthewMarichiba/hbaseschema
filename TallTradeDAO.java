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
	private static String tablePath = "/user/mapr/trades_tall";
	
	/**
	 * constructs a TradeDAO using a tall-narrow table schema.
	 * This implementation assigns a default pathToTable: /user/mapr/trades_tall 
	 * @param conf the HBase configuration
	 * @throws IOException
	 */
	public TallTradeDAO(Configuration conf) throws IOException{
		table = new HTable(conf, tablePath); 
	}
	
	/**
	 * constructs a TradeDAO using a tall-narrow table schema.
	 * This implementation takes a pathToTable for the data table. 
	 * @param conf the HBase configuration
	 * @param pathToTable the HBase configuration
	 * @throws IOException
	 */
	public TallTradeDAO(Configuration conf, String pathToTable) throws IOException{
		if (pathToTable != null) {
			tablePath = pathToTable;
		}
		table = new HTable(conf, tablePath);
	}

	@Override
	public void close() throws IOException {
		table.close();
	}
	
	@Override
	public void store(Trade trade) throws IOException {
		//TODO &&&MJM NOT DE-PLAGGED YET.
		String rowkey = formRowkey(trade.getSymbol(), trade.getTime());
		
//OLD		Put put = new Put(Bytes.toBytes(trade.getSymbol() + delimChar +  convertForId(trade.getTradeDate())));
		Put put = new Put(Bytes.toBytes(rowkey));
		Float priceNoDecimals = trade.getPrice() * 100f;
		put.add(baseCF, priceCol, Bytes.toBytes(priceNoDecimals.longValue() )); // Convert price to a long before writing.
		put.add(baseCF, volumeCol, Bytes.toBytes(trade.getVolume()));

		table.put(put);
	}
	
	/*
	 * creates the rowkey format given a Trade object.
	 * For the tall-narrow implementation, rowkey format = SYMBOL_TIMESTAMP
	 * Example: AMZN_1381396363 
	 */
	/** 
	 * generates a rowkey for tall table implementation. 
	 * Example: GOOG_1381396363000 
	 * @param symbol
	 * @param time
	 * @return
	 */
//	private String formRowkey(String symbol, Long time){
	public static String formRowkey(String symbol, Long time){
		//TODO &&&MJM NOT DE-PLAGGED YET.
		String timeString = String.format("%d", (Long.MAX_VALUE-time) );
		String rowkey = symbol + delimChar + timeString; 
				// date.getTime(); 
				// trade.getDate().getTime().toString(); 
		System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey); // TODO &&&MJM Remove this.

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
		System.out.println("DEBUG: Entered TallTradeDAO.getTradesByDate()"); // TODO &&&MJM Remove this.
	//TODO &&&MJM NOT DE-PLAGGED YET.
		
		// Create a list to store resulting trades
		List<Trade> trades = new ArrayList<Trade>();  
		
//OLD		Scan scan = new Scan(Bytes.toBytes(authorId + KEY_SPLIT_CHAR + convertForId(to)), 
//OLD		Bytes.toBytes(authorId + KEY_SPLIT_CHAR + convertForId(from.getTime()-1)));
		// Scan all applicable rows for the symbol, between given timestamps
		Scan scan = new Scan(Bytes.toBytes(formRowkey(symbol, from)), 
				Bytes.toBytes(formRowkey(symbol, to)) );
		// scan.addFamily(baseCF);
		
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("DEBUG getTradesByDate() result scanner size= " + scanner.size() ); 


		// Iterate through the scanner, and transfer scan results to our list of Trades. 
		// Populate these from scan to trade: Date tradeDate, String tradeSymbol, Float tradePrice, Long tradeVolume
		System.out.println("DEBUG getTradesByDate(): Entering FOR loop next.");
		for (Result result : scanner){

			// Extract the symbol & timestamp from the result's rowkey
			String rowkey = Bytes.toString(result.getRow()); // rowkey format is SYMBOL_TIMESTAMP
			String[] rowkeyTokens = rowkey.split(String.valueOf(delimChar)); // tokenize rowkey
			Long time = Long.MAX_VALUE - Long.parseLong(rowkeyTokens[1]); // reconstitute a valid timestamp from the rowkey digits 
			System.out.println("DEBUG getTradesByDate() FOR loop: reconstituted timestamp= " + 
					time); // TODO &&&MJM Remove this.
			
			// Populate the price & volume into 
			Float price = Bytes.toLong(result.getValue(priceCol, baseCF)) / 100f; // Price data is stored as long byte-array * 100. Extract to Float.
			Long volume = Bytes.toLong(result.getValue(volumeCol, baseCF));

			// Add the new trade to the list of trades
			trades.add(new Trade(rowkeyTokens[0], price, volume, time));
		}
		return trades;
	}

	// TODO &&&MJM: Remove this??
/*
	@Override
	public List<Trade> getTradesBySymbol(String symbol) throws IOException{
		List<Trade> trades = new ArrayList<Trade>();

		// TODO: Add code to scan and extract trades here.

		return trades;
	}
*/

	
}
