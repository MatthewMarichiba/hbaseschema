package schemaDesign_trades;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public interface TradeDAO {
	public void store(Trade trade) throws IOException;
	public void close() throws IOException;
	public List<Trade> getTradesByDate(String symbol, Long from, Long to) throws IOException;
	public Trade getRow(String rowkey) throws IOException;
	// TODO &&&MJM	public List<Trade> getTradesBySymbol(String symbol) throws IOException;
}
