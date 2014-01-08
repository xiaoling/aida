package mpi.aida.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChunkDisambiguationResults {

  private Map<ResultMention, List<ResultEntity>> mentionMappings = new HashMap<ResultMention, List<ResultEntity>>();
  
  private String gTracerHtml;

  public ChunkDisambiguationResults() {
    // Does nothing.
  }
  
  public ChunkDisambiguationResults(Map<ResultMention, List<ResultEntity>> mentionMappings, String gTracerHtml) {
    super();
    this.mentionMappings = mentionMappings;
    this.gTracerHtml = gTracerHtml;    
  }

  public List<ResultMention> getResultMentions() {
    List<ResultMention> mentions = new ArrayList<ResultMention>(mentionMappings.keySet());
    Collections.sort(mentions);
    return mentions;
  }

  public List<ResultEntity> getResultEntities(ResultMention rm) {
    return mentionMappings.get(rm);
  }
  
  public void setResultEntities(ResultMention rm, List<ResultEntity> res) {
    mentionMappings.put(rm, res);
  }

  public ResultEntity getBestEntity(ResultMention rm) {
    List<ResultEntity> res = getResultEntities(rm);

    if (res.size() == 0) {
      return null;
    } else {
      return res.get(0);
    }
  }
  
  public String getgTracerHtml() {
    return gTracerHtml;
  }

  public void setgTracerHtml(String gTracerHtml) {
    this.gTracerHtml = gTracerHtml;
  }

  public String toString() {
    return mentionMappings.toString();
  }

  public void adjustForChunkOffset(int chunkOffset) {
    for (ResultMention rm : getResultMentions()) {
      rm.setCharacterOffset(rm.getCharacterOffset() + chunkOffset);
    }
  }

  public Map<ResultMention, List<ResultEntity>> getAllResults() {
    return mentionMappings;
  }
}
