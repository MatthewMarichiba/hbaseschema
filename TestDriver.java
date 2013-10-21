package schemaDesign_trades;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class TestDriver {
	
	private final static DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd hh:mm:ss");
	
	public static void main(String[] args) throws IOException {
		
		// TODO: After implementing generateDataSet() uncomment this for loop.  
		List<Trade> testTradeSet = generateDataSet();
		printTrades(testTradeSet);
		
		Date myDate = new Date();
		System.out.println(myDate.getTime());
		
	}
	
	private static void printTrades(List<Trade> trades) {
		System.out.println("Printing " + trades.size() + " trades.");
		for (Trade trade : trades) {
			System.out.println(trade);
			System.out.println(trade.getTime());
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
			throws IOException {
		//TODO &&&MJM NOT DE-PLAGGED YET.
		/*
		
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
	*/
	}
}
