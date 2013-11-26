package mpi.tools.database;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBaseManager {
  private static final Logger logger = 
      LoggerFactory.getLogger(DataBaseManager.class);

	private DBConnectionPool pool = null;
	private boolean connected = false;

	protected DataBaseManager(DBSettings settings) throws SQLException {
		if (settings.getType().equals(DBSettings.DB_TYPE_NONE)) {
			connected = false;
		} else {
			pool = new DBConnectionPool(settings);
			DBConnection con = getConnection("initial connect");
			if (con != null) {
				connected = true;
	      releaseConnection(con);
			}
		}
	}
	
	protected boolean getConnected(){
		return connected;
	}

	protected DBConnection getConnection(String requestor) throws SQLException {
		return pool.getConnection(requestor);
	}

	protected void releaseConnection(DBConnection con) {
		try {
			pool.release(con);
		} catch (SQLException sqle) {
			logger.error(sqle.getLocalizedMessage());
		}
	}

	protected void disconnect() {
		try {
			if (pool != null) {
				pool.closeAll();
			}
		} catch (SQLException sqle) {
			logger.error(sqle.getLocalizedMessage());
		}
		connected = false;
	}

	protected DBSettings getSettings() {
		if (pool != null) {
			return pool.getSettings();
		} else {
			return null;
		}
	}
}
