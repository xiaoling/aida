package mpi.tools.database.postgres;

import java.io.Reader;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import mpi.tools.database.DBConnection;
import mpi.tools.database.interfaces.DBPreparedStatementInterface;

public class DBPreparedStatementPostGres extends DBStatementPostGres implements DBPreparedStatementInterface {

  private int myBatchSize;

  /**
   * Constructor - creates a wrapper for <code>OraclePreparedStatement</code>
   * 
   * @param connection
   *          the associated DBOracleConnection
   * @param sql
   *          the SQL command
   * @param batchSize
   *          the batch size used for update commands
   * @throws SQLException
   *           if an error occurs.
   */
  public DBPreparedStatementPostGres(DBConnection connection, String sql, int batchSize) throws SQLException {
    myPostGresConnection = connection;
    myPhysicalConnection = connection.getPhysicalConnection();
    myStatement = myPhysicalConnection.prepareStatement(sql);
    commitStatement = myPhysicalConnection.createStatement();
    myBatchSize = batchSize;
  }

  /**
   * Constructor - creates a wrapper for <code>OraclePreparedStatement</code>
   * 
   * @param connection
   *          the associated DBOracleConnection
   * @param sql
   *          the SQL command
   * @param batchSize
   *          the batch size used for update commands
   * @throws SQLException
   *           if an error occurs.
   */
  public DBPreparedStatementPostGres(DBConnection connection, String sql, int resultSetType, int resultSetConcurrency, int batchSize) throws SQLException {
    myPostGresConnection = connection;
    myPhysicalConnection = connection.getPhysicalConnection();
    myStatement = myPhysicalConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    commitStatement = myPhysicalConnection.createStatement(resultSetType, resultSetConcurrency);
    myBatchSize = batchSize;
  }

  /**
   * Method to set Integers.
   * 
   * @param parameterIndex
   *          parameter number.
   * @param x
   *          the integer value.
   * @throws SQLException
   *           if an error occurs.
   */
  public void setInt(int parameterIndex, int x) throws SQLException {
    ((PreparedStatement) myStatement).setInt(parameterIndex, x);
  }

  /**
   * Method to set Longs.
   * 
   * @param parameterIndex
   *          parameter number.
   * @param x
   *          the long value.
   * @throws SQLException
   *           if an error occurs.
   */
  public void setLong(int parameterIndex, long x) throws SQLException {
    ((PreparedStatement) myStatement).setLong(parameterIndex, x);
  }

  /**
   * Method to set Floats.
   * 
   * @param parameterIndex
   *          parameter number.
   * @param x
   *          the float value.
   * @throws SQLException
   *           if an error occurs.
   */
  public void setFloat(int parameterIndex, float x) throws SQLException {
    ((PreparedStatement) myStatement).setFloat(parameterIndex, x);
  }

  /**
   * Method to set Double.
   * 
   * @param parameterIndex
   *          parameter number.
   * @param x
   *          the double value.
   * @throws SQLException
   *           if an error occurs.
   */
  public void setDouble(int parameterIndex, double x) throws SQLException {
    ((PreparedStatement) myStatement).setDouble(parameterIndex, x);
  }

  /**
   * Method to set Strings.
   * 
   * @param parameterIndex
   *          parameter number.
   * @param x
   *          the string value.
   * @throws SQLException
   *           if an error occurs.
   */
  public void setString(int parameterIndex, String x) throws SQLException {
    ((PreparedStatement) myStatement).setString(parameterIndex, x);
  }

  /**
   * Method to set Clobs.
   * 
   * @param parameterIndex
   *          parameter number.
   * @param x
   *          the clob value.
   * @throws SQLException
   *           if an error occurs.
   */
  public void setClob(int parameterIndex, java.sql.Clob clob) throws SQLException {
    throw new UnsupportedOperationException("");
  }

  /**
   * Method to set timestamps
   * 
   * @param parameterIndex
   *          parameter number.
   * @param t
   *          the timestamp value
   * @throws SQLException
   *           if an error occurs
   */
  public void setTimestamp(int parameterIndex, java.sql.Timestamp t) throws SQLException {
    ((PreparedStatement) myStatement).setTimestamp(parameterIndex, t);
  }

  /**
   * Method to execute a query.
   * 
   * @return the ResultSet of the executed query.
   * @throws SQLException
   *           if an error occurs.
   */
  public ResultSet executeQuery() throws SQLException {
    return ((PreparedStatement) myStatement).executeQuery();
  }

  /**
   * Method to execute a query.
   * 
   * @return the ResultSet of the executed query.
   * @throws SQLException
   *           if an error occurs.
   */
  public boolean execute() throws SQLException {
    return ((PreparedStatement) myStatement).execute();
  }

  /**
   * Method to execute an update operation. This will automatically use
   * batching. A full batch sent to the backend will automatically be commited.
   * 
   * @return number of updated entries.
   * @throws SQLException
   *           if an error occurs.
   */
  public int executeUpdate() throws SQLException {
    int result = ((PreparedStatement) myStatement).executeUpdate();
    if (result > 0) {
      // issue a commit
      this.commitStatement.execute("COMMIT");
      freeTemporaryClobs();
    }
    return result;
  }

  /**
   * sends all batched entries of this statement to the DB and issues a commit
   * afterwards
   * 
   * @throws SQLException
   *           if an error occurs.
   */
  public void commit() throws SQLException {
    ((PreparedStatement) myStatement).executeBatch();
    // issue the actual commit
    this.commitStatement.execute("COMMIT");
  }

  /**
   * frees all memorized temporary clobs
   * 
   * @throws SQLException
   *           if an error occurs
   */
  private void freeTemporaryClobs() throws SQLException {
    throw new UnsupportedOperationException("");
  }

  /**
   * Method to set a character stream.
   * 
   * @param parameterIndex
   *          parameter number
   * @param reader
   *          the reader which contains the data
   * @param length
   *          the number of characters in the stream
   * @throws SQLException
   *           if an error occurs
   */
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    ((PreparedStatement) myStatement).setCharacterStream(parameterIndex, reader, length);
  }

  /**
   * Method to set an array.
   * 
   * @param parameterIndex
   *          parameter number
   * @param array
   *          the array
   * @throws SQLException
   *           if an error occurs
   */
  public void setArray(int parameterIndex, Array array) throws SQLException {
    ((PreparedStatement) myStatement).setArray(parameterIndex, array);
  }

  private int count = 0;

  public void addBatch() throws SQLException {
    ((PreparedStatement) myStatement).addBatch();
    count++;
    if (count >= myBatchSize) {
      executeBatch();
      count = 0;
    }
  }

  public void executeBatch() throws SQLException {
    ((PreparedStatement) myStatement).executeBatch();
    count = 0;
  }

  public void clearBatch() throws SQLException {
    ((PreparedStatement) myStatement).clearBatch();
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp t, Calendar c) throws SQLException {
    ((PreparedStatement) myStatement).setTimestamp(parameterIndex, t, c);
  }

  @Override
  public void setNull(int parameterIndex, int sqltype) throws SQLException {
    ((PreparedStatement) myStatement).setNull(parameterIndex, sqltype);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] array) throws SQLException {
    ((PreparedStatement) myStatement).setBytes(parameterIndex, array);

  }
}
