package mpi.aida.data;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mpi.aida.access.DataAccess;
import mpi.aida.config.settings.JsonSettings.JSONTYPE;
import mpi.tokenizer.data.Token;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *  Generates a JSON representation of the disambiguated results.
 */
public class ResultProcessor {

	private DisambiguationResults result;
	private String content;
	private String inputFile;
	private PreparedInput input;
	private int maxEntities;
	private long time;
	// Reference to final JSON object
	private JSONObject jFinalObj;
	
	private JSONArray jAllMentions;
	// HashMap to be used for Text annotation
	private Map<Integer, ResultMention> hshResultMention;

	private void init(){
		jFinalObj = new JSONObject();
		jAllMentions = new JSONArray();
	}

	public ResultProcessor(){
		init();
	}
	
	 public ResultProcessor(String content, DisambiguationResults result, String inputFile, PreparedInput input) {
	   this(content, result, inputFile, input, Integer.MAX_VALUE);
	 }
	
	/**
	 * Constructs ResultProcessor object.
	 * 
	 * @param content The original string submitted for disambiguation
	 * @param result Reference DisambiguationResult object
	 * @param docID  DocID
	 */
	public ResultProcessor(String content, DisambiguationResults result, String inputFile, PreparedInput input, int maxEntities) {
		this.result = result;
		this.content = content;
		this.input = input;
		this.inputFile = inputFile;
		this.maxEntities = maxEntities;
		
		hshResultMention = new HashMap<Integer, ResultMention>();
		init();
	}
	
	public void setContent(String content){
		this.content = content;
	}
	
	public void setResult(DisambiguationResults result){
		this.result = result;
	}

	public void setInput(PreparedInput input){
		this.input = input;
	}

	public void setOverallTime(long time){
		this.time = time;
	}

	/**
   * Constructs a JSON representation for given disambiguationResult object
   * 
   * @return A JSON string representation of result
   */
	@SuppressWarnings("unchecked")
  public JSONObject process(JSONTYPE jMode){
	  // prepare the compact json representation
	  jFinalObj.put("docID", input.getDocId());
    jFinalObj.put("originalFileName", inputFile);
    
    Map<String, EntityMetaData> hshEntityMetadata = result.getEntitiesMetaData();
    Map<Integer, EntityMetaData> hshIdMetadata = new HashMap<Integer, EntityMetaData>();
    Map<String, Set<Type>> hshEntityTypes = new HashMap<String, Set<Type>>();
    
    for (ResultMention rm : result.getResultMentions()) {
      // load the hashmap with offset-mention pair. Useful for mention-look up while constructing annotated text.
      hshResultMention.put(rm.getCharacterOffset(), rm);
      JSONObject jMention = generateJSONForMention(rm);
      // retrieve entity types for the best entity and generate json repr
      ResultEntity bestEntity = result.getBestEntity(rm);
      JSONObject jsonEntityType = new JSONObject();
      jsonEntityType.put("entity", bestEntity.getEntity());
      //Set<Type> entityTypesObjects = new HashSet<Type>();
      if (!bestEntity.getEntity().equals(Entity.OOKBE)) {
        JSONObject jsonEntity = new JSONObject();     
        EntityMetaData emData = hshEntityMetadata.get(bestEntity.getEntity());        
        hshIdMetadata.put(emData.getId(), emData);
        jsonEntity.put("name", bestEntity.getEntity());
        jsonEntity.put("id", emData.getId());
        NumberFormat df = NumberFormat.getInstance(Locale.ENGLISH);
        df.setMaximumFractionDigits(5);
        jsonEntity.put("disambiguationScore", df.format(bestEntity.getDisambiguationScore()));        
        // add best entity element to mention element
        jMention.put("bestEntity", jsonEntity);
        
        Set<Type> entityTypesObjects = DataAccess.getTypes(bestEntity.getEntity());
        hshEntityTypes.put(bestEntity.getEntity(), entityTypesObjects);
      }
      
      // Generate temporary JSON Object to store other candidate entities but no need to add to mention json
      JSONObject jsonEntity;
      JSONArray jsonEntityArr = new JSONArray();
      int i = 0;
      for (ResultEntity re : result.getResultEntities(rm)) {
        if (++i > maxEntities) {
          break;
        }
        
        if (!re.getEntity().equals(Entity.OOKBE)) {
          // create a json object for entity
          jsonEntity = new JSONObject();     
          EntityMetaData emData = hshEntityMetadata.get(re.getEntity());        
          hshIdMetadata.put(emData.getId(), emData);
          jsonEntity.put("name", re.getEntity());
          jsonEntity.put("id", emData.getId());
          NumberFormat df = NumberFormat.getInstance(Locale.ENGLISH);
          df.setMaximumFractionDigits(5);
          jsonEntity.put("disambiguationScore", df.format(re.getDisambiguationScore()));          
          jsonEntityArr.add(jsonEntity);
          
          Set<Type> entityTypesObjects = DataAccess.getTypes(re.getEntity());
          hshEntityTypes.put(re.getEntity(), entityTypesObjects);
        }
      }     
      jMention.put("allEntities", jsonEntityArr);
      jAllMentions.add(jMention);
    }
    jFinalObj.put("mentions", jAllMentions);
    jFinalObj.put("annotatedText", annotateInputText());
    jFinalObj.put("originalText", content);
    jFinalObj.put("cleanedText", input.getTokens().toText());
    jFinalObj.put("jsTypeInfo", "");
    jFinalObj.put("overallTime", ""+(time/1000));
    
    if(jMode == JSONTYPE.COMPACT){
      return jFinalObj;
    }
    
    // add all entities metadata (required for both WEB and EXT versions)
    JSONObject jMetadataArray = new JSONObject();
    for(Entry<Integer, EntityMetaData> e : hshIdMetadata.entrySet()){
      JSONObject jMetadata = new JSONObject();
      int entityId = e.getKey();
      EntityMetaData eData = e.getValue();
      if(eData != null){
        jMetadata.put("readableRepr", eData.getHumanReadableRepresentation());
        jMetadata.put("id", eData.getId());
        jMetadata.put("url", eData.getUrl());
        
        // Temporary JSONArray to store entity types for the best entity
        JSONArray tmpEntityTypes = new JSONArray();
        List<String> entityTypesStrings = new ArrayList<String>();
        Set<Type> hshTypeSet = hshEntityTypes.get(eData.getUrl());
        if(hshTypeSet!=null){
          for(Type type: hshTypeSet) {
            entityTypesStrings.add(type.toString());
          }
          tmpEntityTypes.addAll(entityTypesStrings);                                    
        }
        jMetadata.put("type", tmpEntityTypes);
      }      
      jMetadataArray.put(entityId, jMetadata);
    }    
    jFinalObj.put("entityMetadata", jMetadataArray);
    
    if(jMode == JSONTYPE.WEB){
	    jFinalObj.put("tracerHtml", result.getgTracerHtml());
	    return jFinalObj;
    }
	  
    jFinalObj.put("tokens", loadTokens());
	  return jFinalObj;
	}
	
	@SuppressWarnings("unchecked")
  private JSONArray loadTokens(){
		List<Token> lstToks = input.getTokens().getTokens();
		JSONArray jTokArr = new JSONArray();
		for(Token tk : lstToks){
			JSONObject jTok = new JSONObject();
			jTok.put("stanfordId", tk.getStandfordId());
			jTok.put("original", tk.getOriginal());
			jTok.put("originalEnd", tk.getOriginalEnd());
			jTok.put("beginIndex", tk.getBeginIndex());
			jTok.put("endIndex", tk.getEndIndex());
			jTok.put("sentence", tk.getSentence());
			jTok.put("paragraph", tk.getParagraph());
			jTok.put("POS", tk.getPOS());
			jTok.put("NE", tk.getNE());
			jTokArr.add(jTok);
		}
		return jTokArr;
	}

	@SuppressWarnings("unchecked")
  private JSONObject generateJSONForMention(ResultMention rm) {
		JSONObject jObj = new JSONObject();
		// create json representation for given mention
		String mentionName = rm.getMention();
		int offset = rm.getCharacterOffset();
		jObj.put("name", mentionName);
		jObj.put("length", rm.getCharacterLength());
		jObj.put("offset", offset);
		return jObj;
	}

	private String annotateInputText(){
		String text = input.getTokens().toText();
		StringBuffer sBuff = new StringBuffer();
		int len = text.length();
		int start = 0;
		int current = 0;
		while(current<len){
			if(hshResultMention.containsKey(current)){
				ResultMention rm = hshResultMention.get(current);
				sBuff.append(text.substring(start, current));
				sBuff.append(constructAnnotation(rm));
				start = current + rm.getCharacterLength();
				current = start;
			}else{
				current++;
			}
		}
		if(current>start){
			sBuff.append(text.substring(start, current));
		}
		return sBuff.toString();
	}

	private String constructAnnotation(ResultMention rm){
		StringBuffer sb = new StringBuffer();
		ResultEntity re = result.getBestEntity(rm);
		String url = Entity.OOKBE;
		if (!re.isNoMatchingEntity()) {
		  EntityMetaData entityMeta = result.getEntitiesMetaData().get(re.getEntity());
		  url = entityMeta.getUrl();
		}    
		sb.append("[[").append(url).append("|").append(rm.getMention()).append("]]");
		return sb.toString();
	}
}