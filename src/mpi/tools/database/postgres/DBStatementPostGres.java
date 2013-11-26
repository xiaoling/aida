package mpi.tools.database.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import mpi.tools.database.DBConnection;
import mpi.tools.database.interfaces.DBStatementInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBStatementPostGres implements DBStatementInterface {
  private static final Logger logger = 
      LoggerFactory.getLogger(DBStatementPostGres.class);

	/** associated connection wrapper */
	protected DBConnection myPostGresConnection;

	/** associated physical connection */
	protected Connection myPhysicalConnection;

	/** underlying physical <code>Statement</code> instance */
	protected Statement myStatement;

	/** statement for sending commit-commands to the database */
	protected Statement commitStatement;

	/**
	 * default constructor. does nothing here.
	 */
	protected DBStatementPostGres() {
	}

	public DBStatementPostGres(DBConnection connection) throws SQLException {
		myPostGresConnection = connection;
		myPhysicalConnection = connection.getPhysicalConnection();
		myStatement = myPhysicalConnection.createStatement();
		commitStatement = myPhysicalConnection.createStatement();
	}

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
	public boolean execute(String sql) throws SQLException {
		return myStatement.execute(sql);
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		return myStatement.executeQuery(sql);
	}

	/**
	 * Method to get the result set of the executed query
	 * 
	 * @return the result set of the executed query.
	 * @throws SQLException
	 *           if an error occurs.
	 */
	public ResultSet getResultSet() throws SQLException {
		return myStatement.getResultSet();
	}

	/**
	 * sends all batched entries of this statement to the DB and issues a commit
	 * afterwards
	 * 
	 * @throws SQLException
	 *           if an error occurs.
	 */
	public void commit() throws SQLException {
		this.commitStatement.execute("COMMIT");
	}

	public void rollback() {
		try {
			this.commitStatement.execute("ROLLBACK");
		} catch (SQLException sqle) {
			logger.error(sqle.getLocalizedMessage());
		}
	}

	public void checkRollback() throws SQLException {
		this.commitStatement.execute("ROLLBACK");
	}

	/**
	 * Method to close the IDBPreparedStatement physically.
	 * 
	 * @throws SQLException
	 *           if an error occurs.
	 */
	public void close() throws SQLException {
		myStatement.close();
		commitStatement.close();
	}

	public void setFetchSize(int size) throws SQLException {
		myStatement.setFetchSize(size);
		commitStatement.setFetchSize(size);
	}

	public void cancel() throws SQLException {
		commitStatement.cancel();
	}
}
