package mpi.aida.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ClassPathUtils {
	public ClassPathUtils(){
		
	}
	
	public static Properties getPropertiesFromClasspath(String propFileName) throws IOException {
	    // loading xmlProfileGen.properties from the classpath
	    Properties props = new Properties();
	    InputStream inputStream = ClassPathUtils.class.getClassLoader()
	        .getResourceAsStream(propFileName);

	    if (inputStream == null) {
	        throw new FileNotFoundException("property file '" + propFileName
	            + "' not found in the classpath");
	    }

	    props.load(inputStream);
	    return props;
	}
	
	public static List<String> getContent(String fileName) throws IOException{
    List<String> content = new ArrayList<String>();
      InputStream inputStream = ClassPathUtils.class.getClassLoader()
          .getResourceAsStream(fileName);

      if (inputStream == null) {
          throw new FileNotFoundException("property file '" + fileName
              + "' not found in the classpath");
      }

      BufferedReader bufReader = new BufferedReader(new InputStreamReader(inputStream));

    String line;
    while (true) {
      line = bufReader.readLine();
      if (line == "" || line == null)
        break;
      content.add(line);
    } 

      return content;
  }
}
