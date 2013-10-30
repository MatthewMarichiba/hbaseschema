package tradeStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TradeDAOTall implements TradeDAO {

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
	 * This implementation takes a pathToTable for the data table. 
	 * @param conf the HBase configuration
	 * @param pathToTable the path to the table, stated from the root of the Hadoop filesystem. pass null to use a default table location.
	 * @throws IOException
	 */
	public TradeDAOTall(Configuration conf, String pathToTable) throws IOException{
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
		System.out.println("Putting trade: " + trade);
		String rowkey = formRowkey(trade.getSymbol(), trade.getTime());
		
		Put put = new Put(Bytes.toBytes(rowkey));
		Float priceNoDecimals = trade.getPrice() * 100f;
		put.add(baseCF, priceCol, Bytes.toBytes(priceNoDecimals.longValue() )); // Convert price to a long before writing.
		put.add(baseCF, volumeCol, Bytes.toBytes(trade.getVolume()));

		table.put(put);
	}
	
	/** 
	 * generates a rowkey for tall table implementation.
	 * rowkeys descend as the timestamp parameter ascends. 
	 * rowkey format = SYMBOL_TIMESTAMP
	 * Example: GOOG_9223370654476953800 
	 * @param symbol
	 * @param time
	 * @return
	 */
	private String formRowkey(String symbol, Long time){
		String timeString = String.format("%d", (Long.MAX_VALUE-time) );
		String rowkey = symbol + delimChar + timeString; 
		// System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey); // DEBUG
		
		return rowkey;
	}

	
	/** 
	 * retrieves all results for a given symbol, between two timestamps  
	 */
	@Override
	public List<Trade> getTradesByDate(String symbol, Long from, Long to) throws IOException{
		
		// Create a list to store resulting trades
		List<Trade> trades = new ArrayList<Trade>();  
		
		// Scan all applicable rows for the symbol, between given timestamps
		Scan scan = new Scan(Bytes.toBytes(formRowkey(symbol, to)), 
				Bytes.toBytes(formRowkey(symbol, from)) );
		// scan.addFamily(baseCF); // Adding the CF isn't necessary if there's only one.
		
		ResultScanner scanner = table.getScanner(scan);

		// Iterate through the scanner, and transfer scan results to our list of Trades. 
		// Populate these: Date tradeDate, String tradeSymbol, Float tradePrice, Long tradeVolume
		for (Result result : scanner){
			// Extract the symbol & timestamp from the result's rowkey
			String rowkey = Bytes.toString(result.getRow()); // rowkey format is SYMBOL_TIMESTAMP
			String[] rowkeyTokens = rowkey.split(String.valueOf(delimChar)); // tokenize rowkey
			Long time = Long.MAX_VALUE - Long.parseLong(rowkeyTokens[1]); // reconstitute a valid timestamp from the rowkey digits 
			
			// Extract price & volume from the result 
			Float price = (float) (Bytes.toLong(result.getValue(baseCF, priceCol))) / 100f; // (Price*100) is stored as long byte-array. Divide by 100 to extract to a Float.
			Long volume = Bytes.toLong(result.getValue(baseCF, volumeCol));

			// Add the new trade to the list of trades
			trades.add(new Trade(rowkeyTokens[0], price, volume, time));
		}
		return trades;
	}
	
}
