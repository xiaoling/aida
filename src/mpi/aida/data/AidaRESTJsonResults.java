package mpi.aida.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpi.aida.util.htmloutput.ResultMention;
import mpi.tokenizer.data.Token;
import mpi.tokenizer.data.Tokens;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class AidaRESTJsonResults {
	private String jsonRepr;
	private JSONObject jsonObj;
	private String overallRunTime;
	// time taken to process the input

	public AidaRESTJsonResults(){
		
	}

	public AidaRESTJsonResults(String jsonStr) throws Exception{
		jsonRepr = jsonStr;
		parseJsonString();
	}

	public void setJsonString(String jsonStr) throws Exception{
		jsonRepr = jsonStr;
		parseJsonString();
	}

	public String getJsonString(){
		return jsonRepr;
	}

	public void setOverallRunTime(String time){
		overallRunTime = time;
	}

	public String getOverallRunTime(){
		return overallRunTime;
	}

	public Map<String, Set<Type>> getEntitiesTypes(){
		HashMap<String, Set<Type>> hshTypes = new HashMap<String, Set<Type>>();
		JSONArray jTypes = (JSONArray)jsonObj.get("entityTypes");
		Iterator itTypes = jTypes.iterator();
		while(itTypes.hasNext()){
			JSONObject entType = (JSONObject)itTypes.next();
			JSONArray jTmpArr = (JSONArray)entType.get("type");
			Set<Type> arrTypes = new HashSet<Type>();
			for(int i=0;i<jTmpArr.size();i++){
				arrTypes.add(new Type("", (String)jTmpArr.get(i)));
			}
			hshTypes.put((String)entType.get("entity"), arrTypes);
		}
		return hshTypes;
	}

	public Map<String, EntityMetaData> getEntitiesMetaData(){
		JSONArray jEntities = (JSONArray)jsonObj.get("entities");
		Iterator it = jEntities.iterator();
		HashMap<String, EntityMetaData> hshMetaData = new HashMap<String, EntityMetaData>();
		while(it.hasNext()){
			JSONObject jEntity = (JSONObject)it.next();
			int id = ((Long)jEntity.get("id")).intValue();
			hshMetaData.put((String)jEntity.get("name"), new EntityMetaData(id, (String)jEntity.get("readableRepr"), (String)jEntity.get("url")));
		}
		return hshMetaData;
	}

	public List<ResultMention> getResultMentions(){
		List<ResultMention> lstMentions = new LinkedList<ResultMention>();
		JSONArray jArrMentions = (JSONArray)jsonObj.get("mentions");
		Iterator itMention = jArrMentions.iterator();
		while(itMention.hasNext()){
			JSONObject jMention = (JSONObject)itMention.next();
			int offset = ((Long)jMention.get("offset")).intValue(); 
			int length = ((Long)jMention.get("length")).intValue();
			double dScore = Double.parseDouble((String)((JSONObject)jMention.get("bestEntity")).get("disambiguationScore"));
			ResultMention rMention = new ResultMention("html", offset, length,
		            (String)jMention.get("name"), (String)((JSONObject)jMention.get("bestEntity")).get("name"), dScore);
			lstMentions.add(rMention);
		}
		return lstMentions;
	}

	public List<ResultEntity> getBestEntities(){
		List<ResultEntity> lstEntities = new LinkedList<ResultEntity>();
		JSONArray jArrMentions = (JSONArray)jsonObj.get("mentions");
		Iterator itMention = jArrMentions.iterator();
		while(itMention.hasNext()){
			JSONObject jMention = (JSONObject)itMention.next();
			int offset = ((Long)jMention.get("offset")).intValue(); 
			int length = ((Long)jMention.get("length")).intValue();
			String name = (String)((JSONObject)jMention.get("bestEntity")).get("name");
			double dScore = Double.parseDouble((String)((JSONObject)jMention.get("bestEntity")).get("disambiguationScore"));
			ResultEntity rEntity = new ResultEntity(name, dScore);
			lstEntities.add(rEntity);
		}
		return lstEntities;
	}

	public Tokens getTokens(){
		JSONArray jArrTok = (JSONArray)jsonObj.get("tokens");
		Iterator itTok  = jArrTok.iterator();
		Tokens tokens = new Tokens();
		while(itTok.hasNext()){
			JSONObject jObj = (JSONObject)itTok.next();
			int stanfordId = ((Long)jObj.get("stanfordId")).intValue();
	        int beginInd = ((Long)jObj.get("beginIndex")).intValue();
	        int endInd = ((Long)jObj.get("endIndex")).intValue();
	        int sentence = ((Long)jObj.get("sentence")).intValue(); 
	        int para = ((Long)jObj.get("paragraph")).intValue();
	       
			Token token = new Token(stanfordId, (String)jObj.get("original"), (String)jObj.get("originalEnd"), beginInd, endInd, sentence, para, (String)jObj.get("POS"), (String)jObj.get("NE"));
			tokens.addToken(token);
		}
		return tokens;
	}

	public String getGTracerHtml(){
		if(jsonObj.containsKey("gTracerHTML")){
			return (String)jsonObj.get("gTracerHTML");
		}
		
		return "";
	}
	
	private void parseJsonString() throws Exception{
		JSONParser parser = new JSONParser();
		jsonObj = (JSONObject)parser.parse(jsonRepr);
	}

}