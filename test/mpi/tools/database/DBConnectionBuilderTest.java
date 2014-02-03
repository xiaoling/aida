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
			DBSettings settings = new DBSettings(hostname, port, username,
					password, maxCon, type, service);
			StringBuffer url = new StringBuffer(200);
			url.append("jdbc:postgresql:");
			url.append("//");
			url.append(settings.getHostname());
			url.append(':');
			url.append(settings.getPort());
			url.append('/');
			url.append(settings.getServiceName());
			System.out.println(url);
			System.out.println("pass:" + password + ";");
			System.out.println(hostname);
			Assert.assertNotNull(DBConnectionBuilder.createConnection(settings));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
