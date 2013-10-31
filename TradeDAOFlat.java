package tradeStorage;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class TradeDAOFlat implements TradeDAO {

	private final HTable table;
	private final static byte [] priceCF = Bytes.toBytes("price");
	private final static byte [] volumeCF = Bytes.toBytes("vol");
	private final static byte [] statsCF = Bytes.toBytes("stats");

	
	private final static DateFormat rowkeyDateFormat = new SimpleDateFormat("yyyyMMdd");
	private final static DateFormat columnHourFormat = new SimpleDateFormat("HH");
	private final static char delimChar = '_';
	private final static byte[][] hours = {Bytes.toBytes("00"),
			Bytes.toBytes("01"),
			Bytes.toBytes("02"),
			Bytes.toBytes("03"),
			Bytes.toBytes("04"),
			Bytes.toBytes("05"),
			Bytes.toBytes("06"),
			Bytes.toBytes("07"),
			Bytes.toBytes("08"),
			Bytes.toBytes("09"),
			Bytes.toBytes("10"),
			Bytes.toBytes("11"),
			Bytes.toBytes("12"),
			Bytes.toBytes("13"),
			Bytes.toBytes("14"),
			Bytes.toBytes("15"),
			Bytes.toBytes("16"),
			Bytes.toBytes("17"),
			Bytes.toBytes("18"),
			Bytes.toBytes("19"),
			Bytes.toBytes("20"),
			Bytes.toBytes("21"),
			Bytes.toBytes("22"),
			Bytes.toBytes("23"),
			};   
		


	// TODO replace "/user/mapr" with your user directory.
	private static String tablePath = "/user/mapr/trades_flat";

	/**
	 * constructs a TradeDAO using a flat-wide table schema.
	 * This implementation takes a pathToTable for the data table. 
	 * @param conf the HBase configuration
	 * @param pathToTable the path to the table, stated from the root of the Hadoop filesystem. pass null to use a default table location.
	 * @throws IOException
	 */
	public TradeDAOFlat(Configuration conf, String pathToTable) throws IOException{
		if (pathToTable != null) {
			tablePath = pathToTable;
		}
		table = new HTable(conf, tablePath);
	}

	
	@Override
	public void close() throws IOException{
		table.close();
	}
	
	@Override
	public void store(Trade trade) throws IOException {
		System.out.println("Putting trade: " + trade);
		String rowkey = formRowkey(trade.getSymbol(), trade.getTime());
		byte [] hourCol = Bytes.toBytes(columnHourFormat.format(trade.getTime()));

		// Put the price to the price column family
		Put put = new Put(Bytes.toBytes(rowkey));
		Float priceNoDecimals = trade.getPrice() * 100f; // The value to store is (long) price*100
		byte[] priceNoDecimalsAsLongBytes = Bytes.toBytes(priceNoDecimals.longValue() ); // Store as byte array of long, not float
		put.add(priceCF, hourCol, trade.getTime(), priceNoDecimalsAsLongBytes);
		put.add(volumeCF, hourCol, trade.getTime(), Bytes.toBytes(trade.getVolume()));

		// Put the volume to the volume column family
		table.put(put);
	}
	
	/** 
	 * generates a rowkey for flat table implementation. 
	 * rowkey format = SYMBOL_DATE  (Date is formatted YYYYMMDD.)
	 * Example: GOOG_20131020 
	 * @param symbol
	 * @param time
	 * @return
	 */
	private String formRowkey(String symbol, Long time){
		String timeString = rowkeyDateFormat.format(time);
		String rowkey = symbol + delimChar + timeString; 
		// System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey); // DEBUG
		
		return rowkey;
	}

	
	@Override
	public List<Trade> getTradesByDate(String symbol, Long from, Long to) throws IOException {
		
		// Create a list to store resulting trades
		List<Trade> trades = new ArrayList<Trade>();  
		
		// Scan all applicable rows for the symbol, between given timestamps
		// System.out.println("DEBUG getTradesByDate(): from= " + from + ", to= "+ to); // DEBUG
		Scan scan = new Scan(Bytes.toBytes(formRowkey(symbol, from)), 
				Bytes.toBytes(formRowkey(symbol, to)) );
		scan.addFamily(priceCF); 
		scan.addFamily(volumeCF); 
		scan.setMaxVersions(); // set scan to get all cell versions
		
		ResultScanner scanner = table.getScanner(scan);

		// Iterate through the scanner, and transfer scan results to our list of Trades. 
		// Populate these: Date tradeDate, String tradeSymbol, Float tradePrice, Long tradeVolume
		for (Result result : scanner) { // scanner has one row result per Symbol per day
			// 1. Loop through columns (hours) 00 to 23 on PRICE CF
			// 2. Get timestamp & price
			// 3. Use the timestamp to lookup volume CF 

			// Loop through every hour in the day and extract data within the hour bucket to a List of KeyValues.
			for (byte[] hour : hours) {
				List<KeyValue> priceKVs = result.getColumn(priceCF, hour);
				List<KeyValue> volumeKVs = result.getColumn(volumeCF, hour);
				if (priceKVs.size() != volumeKVs.size()) {
					System.out.println("WARNING: There is a trade missing price or volume data.");
				}
				
				// Extract price, volume & time from each KV
				for (int i = 0; i < priceKVs.size(); i++) {
					KeyValue priceKV = priceKVs.get(i);
					KeyValue volumeKV = volumeKVs.get(i);
					Float price = Bytes.toLong(priceKV.getValue()) / 100f;
					long time = priceKV.getTimestamp();
					Long volume = Bytes.toLong(volumeKV.getValue());

					// Add the new trade to the list of trades
					trades.add(new Trade(symbol, price, volume, time));
				}
			}
		}

		return trades;
	}

}
