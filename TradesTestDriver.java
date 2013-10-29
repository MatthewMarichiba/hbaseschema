package tradeStorage;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;

public class TradesTestDriver {
	
	private final static DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
	private final static DateFormat dateFormatYyyyMmDd = new SimpleDateFormat("yyyyMMdd");

	private final static String tablePathTall = "/user/user20/trades_tall";
	private final static String tablePathFlat = "/user/user20/trades_flat";
	
	private final static String symbolToGet = "GOOG";
	private final static String fromDateStr = "20131001";
	private final static String toDateStr = "20131031";
	
	public static void main(String[] args) throws IOException {
		
		// List<Trade> testTradeSet = generateDataSet();
		// printTrades(testTradeSet);
		
		Configuration conf = HBaseConfiguration.create();
		
		// Exercise the tall-narrow implementation.
		// TradeDAO tradeDao = new TradeDAOTall(conf, tablePathTall);

		// Exercise the flat-wide implementation.
		TradeDAO tradeDao = new TradeDAOFlat(conf, tablePathFlat);


		System.out.println("Using DAO: " + tradeDao.getClass());

		// Store the trades to DAO
		// for (Trade trade : testTradeSet){
		// 	tradeDao.store(trade);
		// }

		// DEBUG &&&MJM: Test getting one value back.
		// Trade testTrade = tradeDao.getRow("GOOG_9223370655438999807");
		// System.out.println("Trade from get:");
		// System.out.println(testTrade);
		
		// Retrieve values from the DAO
//		Date fromDate = dateFormatYyyyMmDd.parse(fromDateStr, new ParsePosition(0));
		Long fromDate = dateFormatYyyyMmDd.parse(fromDateStr, new ParsePosition(0)).getTime();
//		Date toDate = dateFormatYyyyMmDd.parse(toDateStr, new ParsePosition(0));
		Long toDate = dateFormatYyyyMmDd.parse(toDateStr, new ParsePosition(0)).getTime();

		List<Trade> retrievedTradeSet = tradeDao.getTradesByDate(symbolToGet, fromDate, toDate);
		System.out.println("Printing Trades retreived from table.");
		printTrades(retrievedTradeSet);

		tradeDao.close();
	}
	
	
	private static void printTrades(List<Trade> trades) {
		System.out.println("Printing " + trades.size() + " trades.");
		for (Trade trade : trades) {
			System.out.println(trade);
		}
	}

	/**
	 * generates a test set of Trade objects to be used for testing 
	 * @return a List of Trade objects
	 */
	private static List<Trade> generateDataSet() {
		//TODO &&&MJM NOT DE-PLAGGED YET.
		List<Trade> trades = new ArrayList<Trade>();
		// Params: String tradeSymbol, Float tradePrice, Long tradeVolume, Long tradeTime
		trades.add(new Trade("AMZN", 304.66f, 1333l, 1381396363l*1000));
		trades.add(new Trade("AMZN", 303.91f, 1666l, 1381397364l*1000));
		trades.add(new Trade("AMZN", 304.82f, 1999l, 1381398365l*1000));
		trades.add(new Trade("CSCO", 22.76f, 2332l, 1381399349l*1000));
		trades.add(new Trade("CSCO", 22.78f, 2665l, 1381399650l*1000));
		trades.add(new Trade("CSCO", 22.80f, 2998l, 1381399951l*1000));
		trades.add(new Trade("CSCO", 22.82f, 3331l, 1381400252l*1000));
		trades.add(new Trade("CSCO", 22.84f, 3664l, 1381400553l*1000));
		trades.add(new Trade("CSCO", 22.86f, 3997l, 1381400854l*1000));
		trades.add(new Trade("CSCO", 22.88f, 4330l, 1381401155l*1000));
		trades.add(new Trade("CSCO", 22.90f, 4663l, 1381401456l*1000));
		trades.add(new Trade("CSCO", 22.92f, 4996l, 1381401757l*1000));
		trades.add(new Trade("CSCO", 22.94f, 5329l, 1381402058l));
		trades.add(new Trade("CSCO", 22.96f, 5662l, 1381402359l*1000));
		trades.add(new Trade("CSCO", 22.98f, 5995l, 1381402660l*1000));
		trades.add(new Trade("CSCO", 22.99f, 6328l, 1381402801l*1000));
		trades.add(new Trade("GOOG", 867.24f, 7327l, 1381415776l*1000));
		trades.add(new Trade("GOOG", 866.73f, 7660l, 1381416277l*1000));
		trades.add(new Trade("GOOG", 866.22f, 7993l, 1381416778l*1000));
		trades.add(new Trade("GOOG", 865.71f, 8326l, 1381417279l*1000));
		trades.add(new Trade("GOOG", 865.20f, 8659l, 1381417780l*1000));
		trades.add(new Trade("GOOG", 864.69f, 8992l, 1381418281l*1000));
		trades.add(new Trade("GOOG", 864.18f, 9325l, 1381418782l*1000));
		return trades;
	}
	

	/**
	 * scans for trades and print the result set. 
	 * This method simulates operations on a TradeDAO.
	 * @param dao An implementation of TradeDAO 
	 * @param symbol The stock symbol to look up
	 * @param from Starting timestamp
	 * @param to Ending timestamp
	 * @throws IOException
	 */
	private static void getTradesByDate(TradeDAO dao, String symbol, Long from, Long to)
	//TODO &&&MJM NOT DE-PLAGGED YET.
			throws IOException {
		
		System.out.println("Getting trades of " + symbol + 
				" from " + dateFormat.format(from) + 
				" to " + dateFormat.format(to));
		
		List<Trade> trades = dao.getTradesByDate(symbol, from, to);
		
		if (trades.isEmpty()) {
			System.out.println("getTradesByDate for " + dao.getClass() + " is not available. Please implement it!");
			return;
		}
				
		for (Trade trade : dao.getTradesByDate(symbol, from, to)) {
			System.out.println(trade);
		}
	}
}
