package mpi.aida.util.htmloutput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpi.aida.access.DataAccess;
import mpi.aida.data.AidaRESTJsonResults;
import mpi.aida.data.DisambiguationResults;
import mpi.aida.data.Entities;
import mpi.aida.data.PreparedInput;
import mpi.aida.data.ResultEntity;
import mpi.aida.data.ResultMention;
import mpi.aida.data.Type;
import mpi.aida.util.Result;
import mpi.tokenizer.data.Token;
import mpi.tokenizer.data.Tokens;
import mpi.tools.basics.Basics;
import mpi.tools.basics.Normalize;
import mpi.tools.javatools.parsers.Char;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateWebHtml {
  private static final Logger logger = 
      LoggerFactory.getLogger(GenerateWebHtml.class);

  private String javascriptArrayHoldingTypeInfo = "";

  private Set<String> prohibitedTypes = new HashSet<String>();

  private Map<String, Integer> typesCount = new HashMap<String, Integer>();

  public GenerateWebHtml() {
    prohibitedTypes.add("entity");
    prohibitedTypes.add("yagoLegalActorGeo");
    prohibitedTypes.add("yagoLegalActor");
  }

  public String process(String text, PreparedInput input, DisambiguationResults results, boolean generateTypeInformation) {
    if (results == null) {
      return "<div></div>";
    }
    Result result = new Result(input.getDocId(), text, input.getTokens(), "html");
    for (mpi.aida.data.ResultMention rm : results.getResultMentions()) {
      ResultEntity entity = results.getBestEntity(rm);
      String mentionString = rm.getMention();
      int charOffset = rm.getCharacterOffset();
      int charLength = rm.getCharacterLength();
      double confidence = entity.getDisambiguationScore();
      String entityName = entity.getEntity();
      mpi.aida.util.htmloutput.ResultMention rMention = 
          new mpi.aida.util.htmloutput.ResultMention(
              "html", charOffset, charLength, 
              mentionString, entityName, confidence);
      result.addFinalentity(rMention);
    }
    Map<String, Set<Type>> entitiesTypes;
    if(generateTypeInformation) {
    	entitiesTypes = loadEntitiesTypes(results);
    } else {
    	entitiesTypes = assignGenericType(results);
    }
    return toHtml(result, entitiesTypes);
  }

  
  /**
   * Similar to process() method, but accepts JSON instead of DisambiguationResults object reference.
   * 
   * @param text
   * @param input
   * @param jsonRepr
   * @param generateTypeInformation
   * @return  HTML version of result.
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  @Deprecated
  public String processJSON(String text, PreparedInput input, String jsonRepr, boolean generateTypeInformation) throws Exception {
    if (jsonRepr == null || jsonRepr.equals("")) {
      return "<div></div>";
    }
    Result result = new Result(input.getDocId(), text, input.getTokens(), "html");
    JSONParser jParser = new JSONParser();
    JSONObject temp = (JSONObject)jParser.parse(jsonRepr);
    JSONArray tmpJsonArr = (JSONArray)temp.get("mentions");
    Iterator it = tmpJsonArr.iterator();
    Map<String, Set<Type>> entitiesTypes = new HashMap<String, Set<Type>>();
    while(it.hasNext()){
      JSONObject mention = (JSONObject)it.next();
      
      JSONObject entity = (JSONObject)mention.get("bestEntity");
      Set<Type> entityTypes = new HashSet<Type>();
      entityTypes.add(new Type("", Basics.ENTITY));
      entitiesTypes.put((String)entity.get("name"), entityTypes);
      mpi.aida.util.htmloutput.ResultMention rMention = 
        new mpi.aida.util.htmloutput.ResultMention(
            "html", ((Long)mention.get("offset")).intValue(), ((Long)mention.get("length")).intValue(), 
            (String)mention.get("name"), (String)entity.get("name"), Double.parseDouble((String)entity.get("disambiguationScore")));
      result.addFinalentity(rMention);
    }
    return toHtml(result, entitiesTypes);
  }
 
  /**
   * Similar to process() method, but accepts JSONResults object instead of DisambiguationResults object reference.
   * 
   * @param text
   * @param input
   * @param jsonResult object
   * @param generateTypeInformation
   * @return  HTML version of result.
   * @throws Exception
   */
  public String processJsonResults(String text, PreparedInput input, AidaRESTJsonResults jResults, boolean generateTypeInformation) throws Exception {
	    if (jResults == null || jResults.getJsonString().equals("")) {
	      return "<div></div>";
	    }
	    Result result = new Result(input.getDocId(), text, input.getTokens(), "html");
	    Map<String, Set<Type>> entitiesTypes;
	    if(generateTypeInformation){
	    	entitiesTypes = jResults.getEntitiesTypes();
	    }else{
	    	entitiesTypes = new HashMap<String, Set<Type>>();
	    	for(ResultEntity re: jResults.getBestEntities()){
	    	  Set<Type> entityTypes = new HashSet<Type>();
	    	  entityTypes.add(new Type("",Basics.ENTITY));
	    	  entitiesTypes.put(re.getEntity(), entityTypes);
	    	}
	    }
	    
	    for(mpi.aida.util.htmloutput.ResultMention rm : jResults.getResultMentions()){
	    	result.addFinalentity(rm);
	    }
	    return toHtml(result, entitiesTypes);
	  }
  
  private Map<String, Set<Type>> loadEntitiesTypes(DisambiguationResults results) {
    Set<String> entities = new HashSet<String>();
    DisambiguationResults disResults = results;
    for (ResultMention rm : disResults.getResultMentions()) {
      ResultEntity re = disResults.getBestEntity(rm);
      if (!re.isNoMatchingEntity()) {
        entities.add(re.getEntity());
      }
    }
    if (entities.size() > 0) {
      return DataAccess.getTypes(entities);
    }
    else {
      return new HashMap<String, Set<Type>>();
    }
  }

	private Map<String, Set<Type>> assignGenericType(
			DisambiguationResults results) {
		DisambiguationResults disResults = results;
		Map<String, Set<Type>> entitiesTypes = new HashMap<String, Set<Type>>();
		for (ResultMention rm : disResults.getResultMentions()) {
			ResultEntity re = disResults.getBestEntity(rm);
			Set<Type> entityTypes = new HashSet<Type>();
			entityTypes.add(new Type("",Basics.ENTITY));
			entitiesTypes.put(re.getEntity(), entityTypes);
		}
		
		return entitiesTypes;
	}
  
  private String toHtml(Result result, Map<String, Set<Type>> entitiesTypes) {
    Map<String, Set<String>> htmlSpanIdTypesMapping = new HashMap<String, Set<String>>();
    logger.debug("Doing:" + result.getDocId());
    setTokens(result);
    Tokens tokens = result.getTokens();
    StringBuffer html = new StringBuffer();
    html.append("<div>\n");
    html.append(tokens.getOriginalStart());
    for (int i = 0; i < tokens.size(); i++) {
      Token token = tokens.getToken(i);
      if (token.containsData()) {
        int start = token.getId();
        token = tokens.getToken(start);
        mpi.aida.util.htmloutput.ResultMention mention = result.getMention(token.getBeginIndex()).get("html");
        int to = token.getId();
        int from = token.getId();
        String text = tokens.toText(from, to);
        while (!text.equalsIgnoreCase(mention.getMention()) && text.length() <= mention.getMention().length()) {
          to++;
          text = tokens.toText(from, to);
        }
        html.append("<small>[");
        if (Entities.isOokbEntity(mention.getEntity())) {
          html.append(mention.getEntity());
        } else {
          String name = Normalize.unEntity(mention.getEntity());
          String metaUriString = Char.encodeURIPathComponent(name);
          String displayString = Char.toHTML(Normalize.unNormalize(mention.getEntity()));
          double confidence = mention.getConfidence();
          html.append("<a title='" + confidence + "' target='_blank' href='http://en.wikipedia.org/wiki/" + metaUriString + "'>");
          html.append(displayString);
          html.append("</a>");
        }
        html.append("]</small>");
        String htmlSpanId = (mention.getMention() + "_" + from).replaceAll("[^a-zA-Z0-9]", "_");
        Set<String> types = fixTypesName(entitiesTypes.get(mention.getEntity()));
        updateTypesCounts(types);
        htmlSpanIdTypesMapping.put(htmlSpanId, types);
        html.append("<span class='eq' id='" + htmlSpanId + "' title='" + StringUtils.join(types, " | ") + "'>");
        html.append("<a  class='links'>");
        html.append(text);
        html.append("</a></span>");
        token = tokens.getToken(to);
        html.append(token.getOriginalEnd());
        i = to;
      } else {
        html.append(token.getOriginal());
        html.append(token.getOriginalEnd());
      }
    }
    html.append("</div>\n");
    String typesJSONObjectsStr = getJSONObjects(htmlSpanIdTypesMapping);
    html.append(typesJSONObjectsStr);
    return html.toString();
  }

  private String getJSONObjects(Map<String, Set<String>> htmlSpanIdTypesMapping) {

    Set<String> types = new HashSet<String>();
    for (Set<String> typeSublist : htmlSpanIdTypesMapping.values()) {
      for (String type : typeSublist) {
        types.add(type);
      }
    }
    Map<String, Set<String>> typesIdsMap = new HashMap<String, Set<String>>();
    for (String id : htmlSpanIdTypesMapping.keySet()) {
      for (String type : htmlSpanIdTypesMapping.get(id)) {
        Set<String> ids = typesIdsMap.get(type);
        if (ids == null) {
          ids = new HashSet<String>();
          typesIdsMap.put(type, ids);
        }
        ids.add(id);
      }
    }

    StringBuilder out = new StringBuilder();
    for (String type : typesIdsMap.keySet()) {
      out.append("idsTypes['" + type.replaceAll("[^a-zA-Z0-9]", "") + "'] = ['" + StringUtils.join(typesIdsMap.get(type), "','") + "'];\n");
    }
    javascriptArrayHoldingTypeInfo = out.toString();
    out = new StringBuilder();

    out.append("<br/> <br/> <div id='typesListDiv' class='typesDiv' style='display:none'>");
    List<String> sortedTypes = new ArrayList<String>(types);
    Collections.sort(sortedTypes);
    for (String type : sortedTypes) {
      out.append("<span style='cursor: pointer' onclick='highlight(\"" + type.replaceAll("[^a-zA-Z0-9]", "") + "\")'>" + type + "</span> | ");
    }

    out.append("</div>");
    out.append(getTypeListString());
    return out.toString();
  }

  private void setTokens(Result result) {
    Tokens tokens = result.getTokens();
    for (int i = 0; i < tokens.size(); i++) {
      Token token = tokens.getToken(i);
      if (result.containsMention(token.getBeginIndex())) {
        HashMap<String, mpi.aida.util.htmloutput.ResultMention> mapping = result.getMention(token.getBeginIndex());
        Iterator<String> idsIter = mapping.keySet().iterator();
        while (idsIter.hasNext()) {
          String id = idsIter.next();
          mpi.aida.util.htmloutput.ResultMention mention = mapping.get(id);
          int to = token.getId();
          int from = token.getId();
          String text = tokens.toText(from, to);
          while (!text.equalsIgnoreCase(mention.getMention()) && text.length() <= mention.getMention().length()) {
            to++;
            text = tokens.toText(from, to);
          }
          for (int j = from; j <= to; j++) {
            tokens.getToken(j).addFinalEntity(id, mention.getEntity(), to - j);
          }
        }
      }
    }
  }

  private Set<String> fixTypesName(Set<Type> types) {
    Set<String> fixedTypes = new HashSet<String>();
    if (types == null) return fixedTypes;
    for (Type type : types) {
      String fixedType = Normalize.unNormalize(type.getName());
      /*
      
      int start = type.indexOf("_") + 1;
      int end = type.lastIndexOf("_");
      
      System.out.println(type + "    " + start + "   " + end);
      if (start != -1 && end != -1 && start < end)
      	fixedType = type.substring(start, end).replace("_", " ");
      else
      	fixedType = type.replace("_", " ");*/
      fixedTypes.add(fixedType);
    }
    return fixedTypes;
  }

  private void updateTypesCounts(Set<String> types) {
    Set<String> uniqueTypes = new HashSet<String>();
    for (String type : types) {
      uniqueTypes.add(type);
    }
    for (String type : uniqueTypes) {
      Integer count = typesCount.get(type);
      if (count == null) {
        typesCount.put(type, 1);
      } else {
        typesCount.put(type, count + 1);
      }
    }
  }

  private String getTypeListString() {
    StringBuilder out = new StringBuilder();
    out.append("<script type=\"text/javascript\">");
    out.append("typesList = [\n");
    int typesTotalCount = 0;
    for (String type : typesCount.keySet()) {
      if (prohibitedTypes.contains(type)) continue;
      typesTotalCount++;
      int count = typesCount.get(type);
      out.append("{text: '" + type.replaceAll("[^a-zA-Z0-9]", " ") + "', weight: " + count + ", title: \"" + type + ":" + count + "\", url: 'javascript:highlight(\"" + type.replaceAll("[^a-zA-Z0-9]", "") + "\");'},");
    }

    //		var word_list = [
    //		                 {text: "Lorem", weight: 15},
    //		                 {text: "Ipsum", weight: 9, url: "http://jquery.com/", title: "jQuery Rocks!"},
    //		                 {text: "Dolor", weight: 6},
    //		                 {text: "Sit", weight: 7},
    //		                 {text: "Amet", weight: 5}
    //		                 // ...other words
    //		             ];
    if (typesTotalCount > 0) {
      out.deleteCharAt(out.lastIndexOf(","));
    }
    out.append("];");
    out.append("</script>");
    return out.toString();
  }

  public String getJavascriptArrayHoldingTypeInfo() {
    return javascriptArrayHoldingTypeInfo;
  }
}
