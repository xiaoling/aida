package mpi.aida.util;

import java.sql.SQLException;

import mpi.database.interfaces.DBPreparedStatementInterface;


public class DBUtil {
  public static void addBatch(DBPreparedStatementInterface prepStmt) {
    try {
      prepStmt.addBatch();
    } catch (SQLException sqle) {
      sqle.getNextException().printStackTrace();
      throw new RuntimeException(sqle);
    }
  }
  
  public static void executeBatch(DBPreparedStatementInterface prepStmt) {
    try {
      prepStmt.executeBatch();
    } catch (SQLException sqle) {
      sqle.getNextException().printStackTrace();
      throw new RuntimeException(sqle);
    }
  }
}
