package mpi.tools.database;

import java.sql.Connection;
import java.sql.SQLException;

import mpi.tools.database.interfaces.DBPreparedStatementInterface;
import mpi.tools.database.interfaces.DBStatementInterface;
import mpi.tools.database.postgres.DBPreparedStatementPostGres;
import mpi.tools.database.postgres.DBStatementPostGres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Id: DBOracleConnection.java,v 1.1 2007/01/02 16:06:53 tcrecel Exp $
 * @author czimmer
 */
public class DBConnection {
  private static final Logger logger = 
      LoggerFactory.getLogger(DBConnection.class);

  /** HashMap containing all PreparedStatements */
  //  private HashMap<String, DBPreparedStatementInterface> statementCache;

  /** Statement for executing SQL-commands that are not cacheable */
  private DBStatementInterface statement;

  /** BatchSize of the ConnectionPool and all the Connections. */
  private int batchSize;

  /** flag indicating if this connection is free */
  private boolean released;

  protected Connection underlyingConnection;

  /** holds the current requestor information for this connection */
  private String requestor;

  private String type = null;

  private Integer id = -1;

  /**
   * creates a new wrapped oracle connection
   * 
   * @param pool
   *          the connection pool this connection is to be assigned to
   * @param releasedState
   *          set to <code>true</code> if this connection is free to use in its
   *          connection pool (i.e. an arbitrary thread my allocate it)
   * @throws ClassNotFoundException
   *           if the oracle jdbc driver could not be loaded
   * @throws SQLException
   *           if no physical connection could be established
   */
  protected DBConnection(Connection underlyingConnection, String type, int BatchSize) {
    this.type = type;
    this.underlyingConnection = underlyingConnection;
    //    this.statementCache = new HashMap<String, DBPreparedStatementInterface>();
    this.batchSize = BatchSize;
    try {
      if (DBSettings.DB_TYPE_POSTGRES.equalsIgnoreCase(type)) {
        this.statement = new DBStatementPostGres(this);
      } else {
        logger.error("Database unknown for constructing a statment");
      }
    } catch (SQLException sqle) {
      logger.error(sqle.getLocalizedMessage());
    }
  }

  /**
   * creates a Prepared Statement
   * 
   * @param sql
   *          the parametrized SQL command
   * @param cacheable
   *          set to <code>true</code> if the returned statement is to be cached
   * @return a wrapped <code>OraclePreparedStatement</code>
   * @throws SQLException
   *           if an error occurs
   */
  public DBPreparedStatementInterface prepareStatement(String sql, int batchSize) throws SQLException {
    DBPreparedStatementInterface stmnt = null;
    if (DBSettings.DB_TYPE_POSTGRES.equalsIgnoreCase(type)) {
      stmnt = new DBPreparedStatementPostGres(this, sql, batchSize);
    } else {
      logger.error("Database unknown for constructing a statment");
    }
    return stmnt;
  }

  public DBPreparedStatementInterface prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int batchSize) throws SQLException {
    DBPreparedStatementInterface stmnt = null;
    if (DBSettings.DB_TYPE_ORACLE.equalsIgnoreCase(type)) {
      logger.error("Not supported");
    } else if (DBSettings.DB_TYPE_CLOUDSCAPE.equalsIgnoreCase(type)) {
      logger.error("Not supported");
    } else if (DBSettings.DB_TYPE_POSTGRES.equalsIgnoreCase(type)) {
      stmnt = new DBPreparedStatementPostGres(this, sql, resultSetType, resultSetConcurrency, batchSize);
    } else {
      logger.error("Database unknown for constructing a statment");
    }
    return stmnt;
  }

  public DBPreparedStatementInterface prepareStatement(String sql) throws SQLException {
    return prepareStatement(sql, batchSize);
  }

  /**
   * sets the released state of this connection
   * 
   * @param releasedState
   *          the released state of this connection
   */
  public void setReleasedState(boolean releasedState, Integer id) {
    this.released = releasedState;
    this.id = id;
  }

  public int getId() {
    return id;
  }

  /**
   * returns <code>true</code> if this connection is currently free in its
   * associated pool.
   * 
   * @return <code>true</code> if this connection is currently free in its
   *         associated pool.
   */
  public boolean isReleased() {
    return this.released;
  }

  /**
   * releases this connection in its associated pool.
   */
  protected void release() {
    if (this.released) {
      // this should never happen!
      logger.warn("Connection has already been released!");
    } else {
      this.released = true;
    }
  }

  protected void resetId() {
    this.id = -1;
  }

  /**
   * Method to flush this connection.
   * 
   * @throws SQLException
   *           if an error occurs.
   */
  public boolean flush() throws SQLException {
    if (this.released) {
      this.underlyingConnection.commit();
      return true;
    }
    return false;
  }

  /**
   * Method to close this connection (physically)
   * 
   * @throws SQLException
   *           if an error occurs.
   */
  public void close() throws SQLException {
    this.underlyingConnection.close();
  }

  /**
   * returns the underlying physical connection.
   * 
   * @return the underlying physical connection.
   */
  public Connection getPhysicalConnection() {
    return this.underlyingConnection;
  }

  /**
   * Returns an IDBStatement. Use this method to get a Statement-Object that
   * should be used for executing SQL-Commands that are not cacheable
   * (PreparedStatements are cached by its associated SQL-String, i.e. if you
   * prepare a statement with an SQL-String that you had not before then always
   * a new PreparedStatement is created.
   * 
   * @return an OracleStatement
   */
  public DBStatementInterface getStatement() {
    return this.statement;
  }

  /**
   * returns the current requestor information for this connection (e.g. the
   * name of the method which requested this connection) return the current
   * requestor information for this connection
   */
  public String getRequestor() {
    return this.requestor;
  }

  /**
   * sets the current requestor information for this connection (e.g. the name
   * of the method which requested this connection)
   */
  public void setRequestor(String requestor) {
    this.requestor = requestor;
  }

  public void setAutoCommit(boolean value) throws SQLException {
    underlyingConnection.setAutoCommit(value);
  }

  public boolean isValid() {
    try {
      this.statement.checkRollback();
    } catch (Exception e) {
      logger.warn("Connection invalid. " +
      		"Most likly a firewall problem, trying to get a new connection.\n" + 
          e.toString());
      return false;
    }
    return true;
  }

}