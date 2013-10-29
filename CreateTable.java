package tradeStorage;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {
	private static String tablePath;
	private static String inputFilePath;
	private final static byte [] baseCF = Bytes.toBytes("CF1");
	private final static byte [] priceCF = Bytes.toBytes("price");
	private final static byte [] volumeCF = Bytes.toBytes("vol");
	private final static byte [] statsCF = Bytes.toBytes("stats");
	
	
	public static void main(String[] args) throws IOException {
		// 1. Check for correct args 
		// args[0]: tall or flat
		// args[1]: path to table
		// args[2]: path to input data file 
		if (args.length <= 1) { // quit if not enough args provided
			System.out.println("Usage: CreateTable {tall | flat} <path to table> [<input file>]");
			System.out.println("Example: CreateTable tall /user/mapr/mytable input.txt");
			System.out.println("Each line in input CSV file must have the format: TICKER, PRICE, VOLUME, TIMESTAMP");
			System.out.println("Example: AMZN, 304.82, 3000, 1381398365000");
			return;
		}
		
		if (!(args[0].equals("tall") || args[0].equals("flat"))) { // quit if schema type not specified
			System.out.println("Specify a schema type of 'tall' or 'flat'.");
			return;
		} 

		// 2. Generate a test data set 
		List<Trade> testTradeSet;
		if (args.length == 2) {	// If no input file specified, use a predefined data set specified in generateDataSet()
			System.out.println("No input data provided. Creating a small, pre-defined data set.");
			testTradeSet = CreateTableUtils.generateDataSet();
		} else { // Read the specified input file.
			inputFilePath = args[2];
			testTradeSet = CreateTableUtils.getDataFromFile(inputFilePath); 
		}
		
		// 3. Create a table and store the data set to the table via the DAO
		Configuration conf = HBaseConfiguration.create();
		TradeDAO tradeDao;
		tablePath = args[1]; 

		if (args[0].equals("tall")) {
			CreateTableUtils.createTable(conf, tablePath, new byte[][] {baseCF});
			tradeDao = new TradeDAOTall(conf, tablePath);
		} else if (args[0].equals("flat")) {
			CreateTableUtils.createTable(conf, tablePath, new byte[][] {priceCF, volumeCF, statsCF});
			tradeDao = new TradeDAOFlat(conf, tablePath);
		} else {
			tradeDao = null;
		}

		System.out.println("Using DAO: " + tradeDao.getClass());
		System.out.println("Storing the test data set...");
		for (Trade trade : testTradeSet){
			tradeDao.store(trade);
		}

		return;
	}
	
}