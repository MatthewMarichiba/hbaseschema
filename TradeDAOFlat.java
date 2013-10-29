package tradeStorage;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
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
			};  // &&&MJM Finish this off, if this works 
		


	// TODO replace "/user/mapr" with your user directory.
	// Don't forget to create this table in HBase shell first:
	// hbase> create '/user/mapr/trades_flat',{NAME=>'price',NAME=>'vol',NAME=>'stats'}
	private static String tablePath = "/user/mapr/trades_flat";

	/**
	 * constructs a TradeDAO using a flat-wide table schema.
	 * This implementation assigns a default pathToTable: /user/mapr/trades_flat 
	 * @param conf the HBase configuration
	 * @throws IOException
	 */
	public TradeDAOFlat(Configuration conf) throws IOException{
		table = new HTable(conf, tablePath); 
	}
	
	/**
	 * constructs a TradeDAO using a flat-wide table schema.
	 * This implementation takes a pathToTable for the data table. 
	 * @param conf the HBase configuration
	 * @param pathToTable the path to the table, stated from the root of the Hadoop filesystem.
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
		//TODO &&&MJM NOT DE-PLAGGED YET.
		System.out.println("Putting trade: " + trade);
		String rowkey = formRowkey(trade.getSymbol(), trade.getTime());
		byte [] hourCol = Bytes.toBytes(columnHourFormat.format(trade.getTime()));

		// Put the price to the price column family
		Put put = new Put(Bytes.toBytes(rowkey));
		Float priceNoDecimals = trade.getPrice() * 100f;
		byte[] priceNoDecimalsAsLongBytes = Bytes.toBytes(priceNoDecimals.longValue() ); // Convert price to a long before writing.
		put.add(priceCF, hourCol, trade.getTime(), priceNoDecimalsAsLongBytes);
		put.add(volumeCF, hourCol, trade.getTime(), Bytes.toBytes(trade.getVolume()));

		// Put the volume to the volume column family
		table.put(put);

	}
	
	
	// TODO MJM: Revert this to private nonstatic.
	public static String formRowkey(String symbol, Long time){
		String timeString = rowkeyDateFormat.format(time);
		String rowkey = symbol + delimChar + timeString; 
		System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey); // TODO &&&MJM Remove this.
		
		return rowkey;
	}

	
	@Override
	public List<Trade> getTradesByDate(String symbol, Long from, Long to) throws IOException {
		System.out.println("DEBUG: Entered TradeDAOWide.getTradesByDate()"); // TODO &&&MJM Remove this.
	//TODO &&&MJM NOT DE-PLAGGED YET.
		
		// Create a list to store resulting trades
		List<Trade> trades = new ArrayList<Trade>();  
		
		// Scan all applicable rows for the symbol, between given timestamps
		System.out.println("DEBUG getTradesByDate(): from= " + from + ", to= "+ to); // TODO &&&MJM Remove this.
		Scan scan = new Scan(Bytes.toBytes(formRowkey(symbol, from)), 
				Bytes.toBytes(formRowkey(symbol, to)) );
		scan.addFamily(priceCF); 
		scan.addFamily(volumeCF); 
		scan.setMaxVersions(); // set scan to get all cell versions
		
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("DEBUG getTradesByDate() result scanner:");
		System.out.println(scanner);

		// Iterate through the scanner, and transfer scan results to our list of Trades. 
		// Populate these from scan to trade: Date tradeDate, String tradeSymbol, Float tradePrice, Long tradeVolume
		System.out.println("DEBUG getTradesByDate(): Entering FOR loop next.");
//		Float price;
//		Long volume;
//		long time;
//		List<KeyValue> priceKVs, volumeKVs;
//		List<KeyValue> volumeKVs;
//		KeyValue priceKV, volumeKV;
//		int i;
		
		for (Result result : scanner) { // scanner has one row result per Symbol per day
			// 1. Loop through columns (hours) 00 to 23 on PRICE CF
			// 2. Get timestamp & price
			// 3. Use the timestamp to lookup volume CF 

			// Loop through every hour in the day and extract all trades in that hour bucket.
			for (byte[] hour : hours) {
				List<KeyValue> priceKVs = result.getColumn(priceCF, hour);
				List<KeyValue> volumeKVs = result.getColumn(volumeCF, hour);
				System.out.println("Number of trades (versions) in priceCF list = " + priceKVs.size());
				System.out.println("Number of trades (versions) in volumeCF list = " + volumeKVs.size());
				
				// Extract price, volume & time from each KV
				for (int i = 0; i < priceKVs.size(); i++) {
//				for (KeyValue priceKV : priceKVs ) {
					KeyValue priceKV = priceKVs.get(i);
					KeyValue volumeKV = volumeKVs.get(i);
					Float price = Bytes.toLong(priceKV.getValue()) / 100f;
					long time = priceKV.getTimestamp();
					Long volume = Bytes.toLong(volumeKV.getValue());
					// volumeKV = volumeKVs.get(listIndex);
					// volume = Bytes.toLong(volumeKVs.get(i).getValue());
					// listIndex++;

					// Add the new trade to the list of trades
					trades.add(new Trade(symbol, price, volume, time));
				}
			}
		}

		return trades;
	}

	public Trade getRow(String rowkey) throws IOException{return new Trade("", 0f, 0l, 0l);}
	/*
	// TODO &&&MJM REMOVE THIS METHOD.
		Get get = new Get(Bytes.toBytes(rowkey));
		Result result = table.get(get);
		System.out.println(result);
		
		// Populate the price & volume into 
		Float price = Bytes.toLong(result.getValue(baseCF, priceCol)) / 100f; // Price data is stored as long byte-array * 100. Extract to Float.
		Long volume = Bytes.toLong(result.getValue(baseCF, volumeCol));
		String[] rowkeyTokens = rowkey.split(String.valueOf(delimChar)); // tokenize rowkey
		Long time = Long.MAX_VALUE - Long.parseLong(rowkeyTokens[1]); // reconstitute a valid timestamp from the rowkey digits 
		
		// Add the new trade to the list of trades
		Trade trade = new Trade(rowkeyTokens[0], price, volume, time);
		// System.out.println("DEBUG getRow(): Trade object is: ");
		// System.out.println(trade);

		return trade;
	}
*/

}
