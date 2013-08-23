package mpi.aida.data;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;

import mpi.aida.AidaManager;
import mpi.tokenizer.data.Token;
import mpi.tokenizer.data.Tokens;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *  Generates a JSON representation of the disambiguated results.
 */
public class ResultProcessor {

	private DisambiguationResults result;
	// Reference to final JSON object
	private JSONObject jFinalObj;
	
	private JSONArray jAllMaps;
	
	private String content;
	private PreparedInput input;
	
	// To store all identified mentions and their corresponding details
	private ArrayList<String> arrMentionName;
	private ArrayList<String> arrMentionURL;
	private ArrayList<Integer> arrMentionOffset;
	
	/**
	 * Constructs ResultProcessor object.
	 * 
	 * @param content The original string submitted for disambiguation
	 * @param result Reference DisambiguationResult object
	 * @param docID  DocID
	 */
	public ResultProcessor(String content, DisambiguationResults result, String inputFile, PreparedInput input) {
		this.result = result;
		this.content = content;
		this.input = input;

		arrMentionName = new ArrayList<String>();
		arrMentionURL = new ArrayList<String>();
		arrMentionOffset = new ArrayList<Integer>();

		jFinalObj = new JSONObject();
		jFinalObj.put("docID", input.getDocId());
		jFinalObj.put("originalFileName", inputFile);
		jAllMaps = new JSONArray();
	}
	
	 /**
   * Constructs a JSON representation for given disambiguationResult object
   * 
   * @return A JSON string representation of result
   */
  public String process() {
    jFinalObj.put("originalText", content);
    jFinalObj.put("cleanedText", input.getTokens().toText());
    for(ResultMention rm:this.result.getResultMentions()){
      JSONObject jMention = generateJSONForMention(rm);
      JSONArray jEntityArr = new JSONArray();
      // process all entities obtained
      for(ResultEntity re:this.result.getResultEntities(rm)){
        jEntityArr.add(generateJSONForEntity(re, false));
      }
      jMention.put("allEntities", jEntityArr);
      jAllMaps.add(jMention);
    }
    jFinalObj.put("annotatedText", annotateText());
    jFinalObj.put("mentions", jAllMaps);
    
    return jFinalObj.toJSONString();
  }
  
  	private JSONObject generateJSONForMention(ResultMention rm) {
		JSONObject jObj = new JSONObject();
		// create json representation for given mention
		String mentionName = rm.getMention();
		int offset = rm.getCharacterOffset();
		jObj.put("name", mentionName);
		jObj.put("length", rm.getCharacterLength());
		jObj.put("offset", offset);
		
		arrMentionName.add(mentionName);
		arrMentionOffset.add(offset);
		// create json representation for given entity
		ResultEntity entity = this.result.getBestEntity(rm);
		jObj.put("bestEntity", generateJSONForEntity(entity, true));
		return jObj;
	}
	
	private JSONObject generateJSONForEntity(ResultEntity entity,
			boolean retrieveURL) {
		JSONObject jObj = new JSONObject();
		jObj.put("name", entity.getEntity());
		NumberFormat df = NumberFormat.getInstance(Locale.ENGLISH);
		df.setMaximumFractionDigits(5);
		jObj.put("disambiguationScore",
				df.format(entity.getDisambiguationScore()));
		if (retrieveURL) {
			String url = AidaManager.getWikipediaUrl(entity);
			arrMentionURL.add(url);
			jObj.put("wikiUrl", url);
		}
		return jObj;
	}
	

	private String annotateText() {
		// A temporary list to keep sorted offsets which are unique just in case the original list is not sorted
		ArrayList<Integer> tmpArr = new ArrayList<Integer>();
		for(Integer offset:arrMentionOffset){
			tmpArr.add(offset);
		}
		Collections.sort(tmpArr);
		String text = input.getTokens().toText();
		StringBuffer sb = new StringBuffer();
		int curr_pos = 0;
		for(Integer offset:tmpArr){
			int idx = arrMentionOffset.indexOf(offset);
			String mention = arrMentionName.get(idx);
			String url = arrMentionURL.get(idx);
			sb.append(text.substring(curr_pos, offset));
			sb.append("[["+url+"|"+mention+"]]");
			curr_pos = (offset+mention.length());
		}
		// append remaining part of text(that contains no mention) as such
		sb.append(text.substring(curr_pos));
		return sb.toString();
	}
}