package mpi.experiment;

import java.io.File;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class EvaluateAida {
	public static void main(String[] args) {
		JSONParser parser = new JSONParser();
		try {
			JSONObject obj = (JSONObject) parser.parse(FileUtils
					.readFileToString(new File("test.json")));
			JSONObject metadata = (JSONObject) obj.get("entityMetadata");
			HashMap<Long, String> map = new HashMap<Long, String>();
			for (Object key: metadata.keySet()) {
				map.put(Long.parseLong((String)key),(String)((JSONObject)metadata.get(key)).get("url"));
			}
			// String origText = (String)obj.get("originalText");
			// String annoText = (String)obj.get("annotatedText");
			
			JSONArray mentions = (JSONArray) obj.get("mentions");
			for (Object mention : mentions) {
				JSONObject m = ((JSONObject) mention);
				JSONObject pred = (JSONObject)m.get("bestEntity");
				long id = (Long)pred.get("id");
				long start = (Long)m.get("offset");
				long end = start + (Long)m.get("length");
				System.out.println(start+","+end+":"+map.get(id).substring("http://en.wikipedia.org/wiki/".length()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
