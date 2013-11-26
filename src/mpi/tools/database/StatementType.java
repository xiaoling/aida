package mpi.tools.database;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Hashtable;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementType {
  private static final Logger logger = 
      LoggerFactory.getLogger(StatementType.class);

	private static final String MAINPATH = "/statements";
	public static final String STATISTICS = "statistics";
	public static final String REPOSITORY = "repository";
	public static final String POSTER = "poster";

	private static Hashtable<String, String> statementsTable;

	private String path;
	private String statement;

	public StatementType(String folder, String name) {
		path = StatementType.MAINPATH + "/" + folder + "/" + name + ".properties";
		statement = StatementType.getStatement(path);
		if (statement == null) {
			try {
				path = StatementType.MAINPATH + "/" + folder + "/" + name + ".properties";
				Properties properties = new Properties();
				try {
					properties.load(new FileInputStream("."+path));
				} catch (FileNotFoundException nfe) {
					properties = new Properties();
					InputStream is = getClass().getResourceAsStream(path);
					properties.load(is);
				}
				String type = "stmt_" + DataManager.getDBTYPE();
				if (properties.containsKey(type)) {
					statement = properties.getProperty(type);
				} else if (properties.containsKey("stmt_all")) {
					statement = properties.getProperty("stmt_all");
				} else {
					statement = null;
				}
			} catch (Exception e) {
				logger.error(e.getLocalizedMessage());
			}
			if (statement != null) {
				StatementType.addStatement(path, statement);
			} else {
				logger.error("no statment was found in " + path);
			}
		}
	}

	public String getStatement() {
		return statement;
	}

	public String getStatement(Object[] params) {
		String sql = MessageFormat.format(statement, params);
		return sql;
	}

	private static String getStatement(String path) {
		if (statementsTable != null && statementsTable.containsKey(path)) {
			return statementsTable.get(path);
		}
		return null;
	}

	private static void addStatement(String path, String statement) {
		if (statementsTable == null) {
			statementsTable = new Hashtable<String, String>();
		}
		statementsTable.put(path, statement);
	}

}
