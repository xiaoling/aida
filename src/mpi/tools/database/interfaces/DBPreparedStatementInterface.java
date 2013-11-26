package mpi.tools.database.interfaces;

import java.io.Reader;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

/**
 * @version $Id: DBPreparedStatementInterface.java,v 1.1 2007/01/02 16:06:53
 *          tcrecel Exp $
 * @author czimmer
 */
public interface DBPreparedStatementInterface extends DBStatementInterface {

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
  public void setInt(int parameterIndex, int x) throws SQLException;

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
  public void setLong(int parameterIndex, long x) throws SQLException;

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
  public void setFloat(int parameterIndex, float x) throws SQLException;

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
  public void setDouble(int parameterIndex, double x) throws SQLException;

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
  public void setString(int parameterIndex, String x) throws SQLException;

  /**
   * Method to setClobs.
   * 
   * @param parameterIndex
   *          parameter number.
   * @param x
   *          the clob value.
   * @throws SQLException
   *           if an error occurs.
   */
  public void setClob(int parameterIndex, java.sql.Clob x) throws SQLException;

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
  public void setTimestamp(int parameterIndex, java.sql.Timestamp t) throws SQLException;

  /**
   * Method to set timestamps with calander
   * 
   * @param parameterIndex
   *          parameter number.
   * @param t
   *          the timestamp value
   * @throws SQLException
   *           if an error occurs
   */
  public void setTimestamp(int parameterIndex, java.sql.Timestamp t, Calendar c) throws SQLException;

  public void setNull(int parameterInderx, int sqltype) throws SQLException;

  public void setBytes(int parameterIndex, byte[] array) throws SQLException;

  /**
   * Method to execute a query.
   * 
   * @return the ResultSet of the executed query.
   * @throws SQLException
   *           if an error occurs.
   */
  public ResultSet executeQuery() throws SQLException;

  /**
   * Method to execute a query.
   * 
   * @return the ResultSet of the executed query.
   * @throws SQLException
   *           if an error occurs.
   */
  public boolean execute() throws SQLException;

  /**
   * Method to execute an update operation. This will automatically use
   * batching.
   * 
   * @return number of updated entries.
   * @throws SQLException
   *           if an error occurs.
   */
  public int executeUpdate() throws SQLException;

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
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException;

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
  public void setArray(int parameterIndex, Array array) throws SQLException;

  // nexus: support batching ..
  public void addBatch() throws SQLException;

  public void executeBatch() throws SQLException;

  public void clearBatch() throws SQLException;

}