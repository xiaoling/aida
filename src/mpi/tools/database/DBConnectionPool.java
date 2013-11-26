package mpi.tools.database;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Id: DBConnectionPool.java,v 1.1 2007/01/02 16:06:53 tcrecel Exp $
 * @author czimmer
 */
public class DBConnectionPool {
  private static final Logger logger = 
      LoggerFactory.getLogger(DBConnectionPool.class);

  private int id = 0;

  private List<DBConnection> connectionPool;

  private int maxConnections;

  private HashMap<Integer, DBConnection> usedConnections;

  private DBSettings settings;

  private boolean stop = false;

  private int created = 0;

  private int waiters = 0;

  /**
   * Constructor.
   * 
   * @param dbConfig
   *          the DBConnectionConfig.
   */
  protected DBConnectionPool(DBSettings settings) {
    this.settings = settings;
    this.connectionPool = new LinkedList<DBConnection>();
    this.usedConnections = new HashMap<Integer, DBConnection>();
    this.maxConnections = settings.getMaxConnection();
  }

  protected DBSettings getSettings() {
    return settings;
  }

  /**
   * Method to get a Connectionn. The connection returned will be associated
   * with the current thread. In this way it is possible to specify a thread
   * when calling the <code>flush</code> method which results in flushing only
   * those connections associated with the given thread. Obviously, this makes
   * batching more efficient compared to flushing all connections.
   * 
   * @return the returned DBOracleConnection.
   * @throws ClassNotFoundException
   *           f Exception
   * @throws SQLException
   *           Exception
   */
  protected synchronized DBConnection getConnection(String requestor) throws SQLException {
    DBConnection conn = null;
    if (stop) {
      logger.warn("DB is being closed, a request still came in");
      return null;
    }
    if (requestor == null || requestor.trim().equals("")) {
      // if no valid requestor is provided, we will give out no connection!
      logger.error("Pool will not return connection without valid requestor!");
      return null;
    }
    //incase there is a problem building a connection.
    int count = 0;
    boolean waitNoMore = waiters >= this.maxConnections;
    if (waitNoMore) {
      logger.warn("Too many conneciton are in use.");
      throw new SQLException("Too many connetions have been used.");
    }
    while (!stop && conn == null && count < this.maxConnections + 5) {
      count++;
      if (connectionPool.size() > 0) {
        conn = connectionPool.remove(0);
      } else {
        if (usedConnections.size() < this.maxConnections) {
          logger.debug("Creating new connection");
          DBConnection newConn = DBConnectionBuilder.createConnection(settings);
          created++;
          if (newConn != null) {
            connectionPool.add(newConn);
          }
        } else {
          logger.debug("No more connections can be created waiting for free connection.\n Current requestor is " + requestor + "(Thread " + Thread.currentThread().getName() + ")");
          while (!stop && usedConnections.size() >= this.maxConnections) {
            try {
              waiters++;
              wait();
            } catch (InterruptedException ex) {
            } finally {
              waiters--;
            }
          }
        }
      }
      if (conn != null && !conn.isValid()) {
        try {
          conn.flush();
          conn.close();
        } catch (SQLException e) {
        }
        created--;
        conn = null;
      }
    }
    if (conn == null) {
      logger.error("get Connection return null");
    } else {
      Integer id = new Integer(this.id++);
      usedConnections.put(id, conn);
      conn.setRequestor(requestor);
      conn.setReleasedState(false, id);
    }
    return conn;
  }

  /**
   * Method to release a DBoracleConnection.
   * 
   * @param conn
   *          the Connection to release.
   */
  protected synchronized void release(DBConnection conn) throws SQLException {
    if (conn == null) {
      logger.warn("connection was null");
      return;
    }
    try {
      conn.flush();
      conn.release();
      conn.setRequestor(null);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    }
    // make the connection available again
    this.usedConnections.remove(conn.getId());
    conn.resetId();
    this.connectionPool.add(conn);
    notifyAll();
  }

  /**
   * Method to (physically) close and flush all database connections in the
   * connection pool.
   * 
   * @throws SQLException
   *           if an error occurs
   */
  protected synchronized void closeAll() throws SQLException {
    // no connections are given.
    stop = true;
    // wait for all connections to become released try killing them every 1 seconds.
    while (usedConnections.size() > 0) {
      Iterator<Integer> keys = usedConnections.keySet().iterator();
      while (keys.hasNext()) {
        try {
          Integer key = keys.next();
          DBConnection conn = usedConnections.get(key);
          conn.getStatement().cancel();
          conn.flush();
          conn.close();
        } catch (SQLException e) {
          logger.error(e.getLocalizedMessage());
        }
      }
      notifyAll();
      try {
        wait(1000);
      } catch (InterruptedException ex) {
      }
    }
    int count = 0;
    // close them
    while (!connectionPool.isEmpty()) {
      count++;
      try {
        DBConnection conn = (DBConnection) connectionPool.remove(0);
        conn.flush();
        conn.close();
      } catch (SQLException e) {
        logger.error(e.getLocalizedMessage());
      }
    }
    logger.debug("Closed " + count + " connections out of " + created);
  }
}