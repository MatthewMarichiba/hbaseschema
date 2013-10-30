package tradeStorage;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class LookupTrades {

	private final static DateFormat dateArgFormat = new SimpleDateFormat("yyyyMMdd");

	public static void main(String[] args) throws IOException {
		// args[0]: tall or flat
		// args[1]: path to table
		// args[2]: symbol
		// args[3]: startdate
		// args[4]: stopdate
		
		// 1. Check for correct args. Parse args. 
		if (args.length <= 3) { // quit if not enough args provided
			System.out.println("LookupTrades {tall | flat} <path to table> symbol [startdate[, stopdate]]");
			System.out.println("Format dates as YYYYMMDD.");
			System.out.println("Example: LookupTrades flat /table/path CSCO 20131021 20131025");
			return;
		}
		
		if (!(args[0].equals("tall") || args[0].equals("flat"))) { // quit if schema type not specified
			System.out.println("Specify a schema type of 'tall' or 'flat'.");
			return;
		} 

		String tablePath = args[1];
		String symbol = args[2];

		Long startDate = Long.MIN_VALUE;
		Long stopDate = Long.MAX_VALUE;
		if (args.length == 3) {	// Start & stop dates not provided
			System.out.println("No start and stop dates specified. Retrieving all trades for " + symbol);
		} else if (args.length == 4) { // Only a start date is provided
			startDate = dateArgFormat.parse(args[3], new ParsePosition(0)).getTime(); 
			System.out.println("Retrieving trades starting from " + args[3] + " for " + symbol);
		} else { // Both start & stop dates are provided
			startDate = dateArgFormat.parse(args[3], new ParsePosition(0)).getTime(); 
			stopDate = dateArgFormat.parse(args[4], new ParsePosition(0)).getTime(); 
			System.out.println("Retrieving trades for " + symbol + "from " + args[3] + " to " + args[4]);
		}
		
		// 2. Open an HBaseConfiguration connection and instantiate a DAO to access the table.
		Configuration conf = HBaseConfiguration.create();
		TradeDAO tradeDao = null;
		if (args[0].equals("tall")) {
			tradeDao = new TradeDAOTall(conf, args[1]);
		} else if (args[0].equals("flat")) {
			tradeDao = new TradeDAOFlat(conf, args[1]);
		} 
		System.out.println("Using DAO: " + tradeDao.getClass());

		// 3. Read a set of trades via the DAO.
		List<Trade> retrievedTradeSet = tradeDao.getTradesByDate(symbol, startDate, stopDate);
		System.out.println("Printing Trades retreived from DAO.");

		// 4. Print the results and exit.
		printTrades(retrievedTradeSet);
		tradeDao.close();
		return;
	}

	private static void printTrades(List<Trade> trades) {
		System.out.println("Printing " + trades.size() + " trades.");
		for (Trade trade : trades) {
			System.out.println(trade);
		}
	
	}

}