package mpi.tools.database;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Id: DBSettings.java,v 1.1 2007/01/02 16:06:54 tcrecel Exp $
 * @author czimmer
 */
public class DBSettings {
  private static final Logger logger = 
      LoggerFactory.getLogger(DBSettings.class);

  public final static String DB_TYPE_NONE = "None";

  public final static String DB_TYPE_CLOUDSCAPE = "Cloudscape";

  public final static String DB_TYPE_ORACLE = "Oracle";

  public final static String DB_TYPE_POSTGRES = "PostGres";

  private String type = DB_TYPE_NONE;

  private String hostname = null;

  private int port = 0;

  private int maxConnection = 0;

  private String username = null;

  private String password = null;

  private String serviceName = null;

  private String sid = null;

  private int batchSize = 50;

  public DBSettings(String hostname, int port, String username, String password, int maxConnection, String type, String serviceName) {
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;
    this.maxConnection = maxConnection;
    this.type = type;
    this.serviceName = serviceName;
  }

  public DBSettings(String propertiesFilePath) {
    Properties prop = new Properties();
    try {
      File file = new File(propertiesFilePath);
      if (file.exists()) {
        prop.load(new FileReader(file));
        this.hostname = prop.getProperty("hostname");
        this.port = Integer.parseInt(prop.getProperty("port"));
        this.username = prop.getProperty("username");
        this.password = prop.getProperty("password");
        this.maxConnection = Integer.parseInt(prop.getProperty("maxConnection"));
        this.type = prop.getProperty("type");
        if (this.type.equalsIgnoreCase(DBSettings.DB_TYPE_ORACLE)) {
          this.serviceName = prop.getProperty("serviceName");
        } else if (this.type.equalsIgnoreCase(DBSettings.DB_TYPE_POSTGRES)) {
          this.serviceName = prop.getProperty("schema");
        }
        try {
          this.batchSize = Integer.parseInt(prop.getProperty("batchSize"));
        } catch (Exception e) {
          //ignore
        }
      } else {
        this.type = DBSettings.DB_TYPE_NONE;
        logger.error("Database properties file is missing from " + propertiesFilePath);
      }
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    }
  }

  public void setSID(String sid) {
    this.sid = sid;
  }

  public String getSID() {
    return sid;
  }

  public String getType() {
    return type;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  protected int getMaxConnection() {
    return maxConnection;
  }

  public String getUsername() {
    return username;
  }

  protected String getPassword() {
    return password;
  }

  public String getServiceName() {
    return serviceName;
  }

  public int getBatchSize() {
    return batchSize;
  }
}