package mpi.tools.database;

import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipleDBManager {
  private static final Logger logger = 
      LoggerFactory.getLogger(MultipleDBManager.class);

	private static Hashtable<String, MDBManager> databases = null;

	public static void addDatabase(String id, DBSettings settings) {
		if (databases == null) {
			databases = new Hashtable<String, MDBManager>();
		}
		if (databases.containsKey(id)){
			logger.warn("Database Id already exists in table");
		}
		MDBManager database = new MDBManager(settings);
		database.connect();
		if (database.isConnected()) {
			databases.put(id, database);
		} else {
			logger.warn("Failed to connect not added to table");
		}
	}

	public static DBConnection getConnection(String id, String requestor) throws SQLException {
		if (databases == null) {
			return null;
		}
		MDBManager database = databases.get(id);
		if (database == null) {
			logger.warn("Id was not in hashtable");
			return null;
		}
		if (database.getDBTYPE() == DBSettings.DB_TYPE_NONE) {
			return null;
		}
		return database.getConnection(requestor);
	}

	public static void releaseConnection(String id, DBConnection con) {
		if (databases != null) {
			MDBManager database = databases.get(id);
			if (database != null) {
				database.releaseConnection(con);
			} else {
				logger.warn("Id was not in hashtable");
			}
		}
	}

	public static boolean isConnected(String id) {
		if (databases == null) {
			return false;
		}
		MDBManager database = databases.get(id);
		if (database == null) {
			logger.debug("Id was not in hashtable");
			return false;
		}
		return database.isConnected();
	}

	public static String getDBTYPE(String id) {
		if (databases == null) {
			return null;
		}
		MDBManager database = databases.get(id);
		if (database == null) {
			logger.warn("Id was not in hashtable");
			return null;
		}
		return database.getDBTYPE();
	}

	public static void disconnect(String id) {
		if (databases != null) {
			MDBManager database = databases.remove(id);
			if (database != null) {
				database.disconnect();
			} else {
				logger.warn("Id was not in hashtable");
			}
		}
	}

	public static void disconnectAll() {
	  logger.debug("Closing all database connections");
		if (databases != null) {
			Enumeration<String> keys = databases.keys();
			Vector<String> todisconnect = new Vector<String>();
			while (keys.hasMoreElements()) {
				todisconnect.add(keys.nextElement());
			}
			Iterator<String> iter = todisconnect.iterator();
			while (iter.hasNext()) {
				MultipleDBManager.disconnect(iter.next());
			}
		}
	}

	public static DBSettings getSettings(String id) {
		if (databases == null) {
			return null;
		}
		MDBManager database = databases.get(id);
		if (database == null) {
			logger.warn("Id was not in hashtable");
			return null;
		}
		return database.getSettings();
	}

}
