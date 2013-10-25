package schemaDesign_trades;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTableTall {
	private static String tablePath;
	private static String inputFilePath;
	private final static byte [] baseCF = Bytes.toBytes("CF1");
	
	
	public static void main(String[] args) throws IOException {
		// 1. Check for correct args 
		if (args.length == 0) {
			System.out.println("Usage: CreateTableTall <path to table to create> [<input CSV file>]");
			System.out.println("Example: CreateTableTall /user/mapr/mytable input.txt");
			System.out.println("Each line in <input CSV file> must have the format: TICKER, PRICE, VOLUME, TIMESTAMP");
			System.out.println("Example: AMZN, 304.82, 3000, 1381398365000");
			return;
		} else {
			tablePath = args[0]; 
		}

		// 2. Create the table
		System.out.println("Creating table " + tablePath + "...");
		Configuration conf = HBaseConfiguration.create();
		CreateTableUtils.createTable(conf, tablePath, new byte[][] {baseCF});
		HBaseAdmin admin = new HBaseAdmin(conf);
		byte[] tablePathBytes = Bytes.toBytes(tablePath); 
		HTableDescriptor desc = new HTableDescriptor(tablePathBytes);
		HColumnDescriptor coldef = new HColumnDescriptor(baseCF);
		desc.addFamily(coldef);
		admin.createTable(desc);

		boolean avail = admin.isTableAvailable(tablePathBytes);
		System.out.println("Table " + tablePath + " available: " + avail);		

		// 3. Generate a test data set 
		List<Trade> testTradeSet;
		if (args.length <= 1) {	// If no input file specified, use a predefined data set specified in generateDataSet()
			System.out.println("No input data provided. Creating a small, pre-defined data set.");
			testTradeSet = generateDataSet();
		} else { // Read the specified input file.
			inputFilePath = args[1];
			testTradeSet = getDataFromFile(inputFilePath); 
		}
		
		// 4. Store the data set to the table via the DAO
		TradeDAO tradeDao = new TallTradeDAO(conf, tablePath);
		System.out.println("Using DAO: " + tradeDao.getClass());
		System.out.println("Storing the test data set...");
		for (Trade trade : testTradeSet){
			tradeDao.store(trade);
		}

		// 5. Close resources and exit.
		if (admin != null) admin.close();
		return;
	}
	
	
	private static List<Trade> getDataFromFile(String filePath) throws IOException {
 		List<Trade> trades = new ArrayList<Trade>();  // This is the list of Trades to populate from the input file and return.
 		
 		// Open the input file and read it line by line
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String currentLine;
		String[] tokens; // an array to hold values from one line of the file
		System.out.println("Importing data from input file " + filePath + "...");
		while ((currentLine = br.readLine()) != null) {
			System.out.println(currentLine);
			tokens = currentLine.split("\\s*,\\s*");
			if ( (tokens != null) && (tokens.length == 4) )  {
				trades.add(new Trade(tokens[0], Float.parseFloat(tokens[1]), Long.parseLong(tokens[2]), Long.parseLong(tokens[3])));
				// System.out.println("Added trade to dataset: " + trades.toArray()[trades.size()-1]);
			} else {
				System.out.println("Ignoring malformed line: " + currentLine);
			}
		}
		
		if (br != null) br.close();
		return trades;
	}
	
	
	/**
	 * generates a small, fixed list of Trade objects to be used for testing 
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

}