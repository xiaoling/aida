package mpi.tools.database;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MDBManager {
  private static final Logger logger = 
      LoggerFactory.getLogger(MDBManager.class);

	private DataBaseManager databaseManager = null;
	private boolean connected = false;
	private  DBSettings settings = null;
	public static String DB_TYPE = null;

	public MDBManager(DBSettings settings){
		this.settings= settings; 
	}
	
	
	public void connect() {
		if (settings == null){
			logger.warn("settings may not be null");
		}
		if (databaseManager == null) {
			try {
				if (settings.getType().equals(DBSettings.DB_TYPE_NONE)) {
					DB_TYPE = DBSettings.DB_TYPE_NONE;
					connected = true;
					logger.debug("No Database being used");
				} else {
				  logger.debug("Trying to connect " + settings.getType() + " Database with " + settings.getUsername());
					databaseManager = new DataBaseManager(settings);
					connected = databaseManager.getConnected();
					DB_TYPE = settings.getType();
					logger.debug("Connected to " + settings.getType() + " Database with " + settings.getUsername());
				}
			} catch (SQLException e) {
				logger.error(e.getLocalizedMessage());
				databaseManager = null;
				DB_TYPE = null;
				connected = false;
				logger.debug("Failed to connected to Database");
			}
		} else {
		  logger.debug("DataManager has already been initiated");
		}
	}

	protected void setConnected(boolean value) {
		connected = value;
	}

	public DBConnection getConnection(String requestor) throws SQLException {
		if (DB_TYPE == DBSettings.DB_TYPE_NONE){
			return null;
		}
		return databaseManager.getConnection(requestor);
	}

	public void releaseConnection(DBConnection con) {
		databaseManager.releaseConnection(con);
	}

	public boolean isConnected() {
		return connected;
	}

	public String getDBTYPE(){
		return DB_TYPE;
	}
	
	public void disconnect() {
		if (databaseManager != null && isConnected()) {
			connected = false;
			databaseManager.disconnect();
			databaseManager = null;
			System.gc();
			DB_TYPE = null;
			logger.debug("Disconnected DataBase " + settings.getUsername());
		}
	}

	public DBSettings getSettings() {
		if (databaseManager != null && isConnected()) {
			return databaseManager.getSettings();
		} else {
			return null;
		}
	}

}
