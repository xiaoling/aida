package mpi.tools.database;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author edwin
 *
 */
public class DataManager {
  private static final Logger logger = 
      LoggerFactory.getLogger(DataManager.class);
  
  private static DataBaseManager databaseManager = null;

  private static boolean connected = false;

  private static String DB_TYPE = null;

  /**
   * connects to the database using the BDsettings
   * @param settings for connecting to the database
   */
  public static void connect(DBSettings settings) {
    if (databaseManager == null) {
      try {
        if (settings.getType().equals(DBSettings.DB_TYPE_NONE)) {
          DB_TYPE = DBSettings.DB_TYPE_NONE;
          DataManager.connected = true;
          logger.debug("No Database being used");
        } else {
          logger.debug("Trying to connect " + settings.getType() + " Database with " + settings.getUsername());
          databaseManager = new DataBaseManager(settings);
          DataManager.connected = databaseManager.getConnected();
          DB_TYPE = settings.getType();
          if (DataManager.connected) {
            logger.debug("Connected to " + settings.getType() + " Database");
          } else {
            logger.warn("Failed to connect to the database");
          }
        }
      } catch (SQLException e) {
        logger.error(e.getLocalizedMessage());
        databaseManager = null;
        DB_TYPE = null;
        DataManager.connected = false;
        logger.debug("Failed to connected to Database");
      }
    } else {
      logger.debug("DataManager has already been initiated");
    }
  }

  protected static void setConnected(boolean value) {
    DataManager.connected = value;
  }

  /**
   * Gets a connection for executing sql statements
   * @param requestor String a name of the requesting (usually the methodname) 
   * @return a DBConnection
   * @throws SQLException
   */
  public static DBConnection getConnection(String requestor) throws SQLException {
    if (DB_TYPE == DBSettings.DB_TYPE_NONE) {
      return null;
    }
    return databaseManager.getConnection(requestor);
  }

  /**
   * returns the connection to the thread. DO NOT forget to do this. Otherwise
   * you will run out of connections.
   * @param con
   */
  public static void releaseConnection(DBConnection con) {
    databaseManager.releaseConnection(con);
  }

  /**
   *  
   * @return true if you are connected to the database
   */
  public static boolean isConnected() {
    return DataManager.connected;
  }

  /**
   * 
   * @return the type of database currently connected to.
   */
  public static String getDBTYPE() {
    return DB_TYPE;
  }

  /**
   * disconnects from the database.
   */
  public static void disconnect() {
    if (DataManager.databaseManager != null && DataManager.isConnected()) {
      DataManager.connected = false;
      databaseManager.disconnect();
      databaseManager = null;
      DB_TYPE = null;
      logger.debug("Disconnected DataBase");
    }
  }

  /**
   * Gets the current database settings
   * @return DBSettings
   */
  public static DBSettings getSettings() {
    if (DataManager.databaseManager != null && DataManager.isConnected()) {
      return databaseManager.getSettings();
    } else {
      return null;
    }
  }

}
