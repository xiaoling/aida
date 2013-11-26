package mpi.tools.database;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.derby.drda.NetworkServerControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Id: DBConnectionBuilder.java,v 1.1 2007/01/02 16:06:53 tcrecel Exp $
 * @author czimmer
 */

public class DBConnectionBuilder {
  private static final Logger logger = 
      LoggerFactory.getLogger(DBConnectionBuilder.class);

  /**
   * Method to return a ADBConnection associated with the given pool
   * 
   * @return The connection to an Oracle<sup>&reg;</sup> database.
   */
  protected static DBConnection createConnection(DBSettings settings) throws SQLException {
    if (DBSettings.DB_TYPE_CLOUDSCAPE.equalsIgnoreCase(settings.getType())) {
      return createCloudscapeConnection(settings);
    } else if (DBSettings.DB_TYPE_ORACLE.equalsIgnoreCase(settings.getType())) {
      return createOracleConnection(settings);
    } else if (DBSettings.DB_TYPE_POSTGRES.equalsIgnoreCase(settings.getType())) {
      return createPostGresConnection(settings);
    } else {
      logger.warn("Connection unkown " + settings.getType());
      return null;
    }
  }

  private static DBConnection createOracleConnection(DBSettings settings) throws SQLException {
    StringBuffer thin = new StringBuffer(200);
    thin.append("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=");
    thin.append(settings.getHostname());
    thin.append(")(PORT=");
    thin.append(settings.getPort());
    if (settings.getServiceName() != null) {
      thin.append(")))(CONNECT_DATA=(SERVICE_NAME=");
      thin.append(settings.getServiceName());
    } else if (settings.getSID() != null) {
      thin.append(")))(CONNECT_DATA=(SID=");
      thin.append(settings.getSID());
    } else {
      throw new SQLException("both SID and Service name was null");
    }
    thin.append(")(server = dedicated)))");
    java.util.Properties info = new java.util.Properties();
    info.put("user", settings.getUsername());
    info.put("password", settings.getPassword());
    if (settings.getUsername().equalsIgnoreCase("sys")) {
      info.put("internal_logon", "sysdba");
    }
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      Connection conn = DriverManager.getConnection(thin.toString(), info);
      return new DBConnection(conn, settings.getType(), settings.getBatchSize());
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
      throw new SQLException(e.toString());
    }
  }

  private static DBConnection createCloudscapeConnection(DBSettings settings) throws SQLException {
    StringBuffer url = new StringBuffer(200);
    url.append("jdbc:derby:");
    try {
      org.apache.derby.drda.NetworkServerControl nsc = new NetworkServerControl();
      nsc.ping();
      DriverManager.registerDriver(new org.apache.derby.jdbc.ClientDriver());
      url.append("//");
      url.append(settings.getHostname());
      url.append(':');
      url.append(settings.getPort());
      url.append('/');
      url.append(settings.getServiceName());
    } catch (Exception de) {
      logger.error(de.getLocalizedMessage());
    }
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      Connection conn = DriverManager.getConnection(url.toString(), settings.getUsername(), settings.getPassword());
      return new DBConnection(conn, settings.getType(), settings.getBatchSize());
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
      throw new SQLException(e.toString());
    }
  }

  private static DBConnection createPostGresConnection(DBSettings settings) throws SQLException {
    StringBuffer url = new StringBuffer(200);
    url.append("jdbc:postgresql:");
    url.append("//");
    url.append(settings.getHostname());
    url.append(':');
    url.append(settings.getPort());
    url.append('/');
    url.append(settings.getServiceName());
    try {
      Driver driver = (Driver) Class.forName("org.postgresql.Driver").newInstance();
      DriverManager.registerDriver(driver);
      Connection conn = DriverManager.getConnection(url.toString(), settings.getUsername(), settings.getPassword());
      return new DBConnection(conn, settings.getType(), settings.getBatchSize());
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
      throw new SQLException(e.toString());
    }

  }
}