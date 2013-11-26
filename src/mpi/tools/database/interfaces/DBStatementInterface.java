package mpi.tools.database.interfaces;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @version $Id: DBStatementInterface.java,v 1.1 2007/01/02 16:06:54 tcrecel Exp
 *          $
 * @author czimmer
 */
public interface DBStatementInterface {

	/**
	 * executes the given SQL command
	 * 
	 * @param sql
	 *          the SQL command to be executed
	 * @return <code>true</code> if the first result is a ResultSet object;
	 *         <code>false</code> if it is an update count or there are no results
	 * @throws SQLException
	 *           if an error occurs
	 */
	public boolean execute(String sql) throws SQLException;

	public ResultSet executeQuery(String sql) throws SQLException;

	/**
	 * Method to get the result set of the executed query
	 * 
	 * @return the result set of the executed query.
	 * @throws SQLException
	 *           if an error occurs.
	 */
	public ResultSet getResultSet() throws SQLException;

	/**
	 * sends all batched entries of this statement to the DB and issues a commit
	 * afterwards
	 * 
	 * @throws SQLException
	 *           if an error occurs.
	 */
	public void commit() throws SQLException;

	/**
	 * Rollsback any changes
	 * 
	 */
	public void rollback();

	/**
	 * used to check if a connection is still active, and removes and previous
	 * none commit statements.
	 * 
	 * @throws SQLException
	 *           if an error occurs.
	 */
	public void checkRollback() throws SQLException;

	/**
	 * Method to close the IDBPreparedStatement physically.
	 * 
	 * @throws SQLException
	 *           if an error occurs.
	 */
	public void close() throws SQLException;

	public void setFetchSize(int size) throws SQLException;
	
	public void cancel() throws SQLException;
}