package mpi.tools.database;

import java.util.Properties;

import mpi.aida.util.ClassPathUtils;

import org.junit.Assert;
import org.junit.Test;

public class DBConnectionBuilderTest {

	@Test
	public void test() {
		Properties prop;
		try {
			prop = ClassPathUtils
					.getPropertiesFromClasspath("database_aida.properties");
			String type = prop.getProperty("type");
			String service = null;
			if (type.equalsIgnoreCase("Oracle")) {
				service = prop.getProperty("serviceName");
			} else if (type.equalsIgnoreCase("PostGres")) {
				service = prop.getProperty("schema");
			}
			String hostname = prop.getProperty("hostname");
			Integer port = Integer.parseInt(prop.getProperty("port"));
			String username = prop.getProperty("username");
			String password = prop.getProperty("password");
			Integer maxCon = Integer
					.parseInt(prop.getProperty("maxConnection"));
			System.out.println(hostname);
			DBSettings settings = new DBSettings(hostname, port, username,
					password, maxCon, type, service);
			Assert.assertNotNull(DBConnectionBuilder.createConnection(settings));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
