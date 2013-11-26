package mpi.aida.graph.similarity.context;

import java.util.Map;


public class EntitiesContextSettings {
  public enum EntitiesContextType {
    MENTION_ENTITY, ENTITY_ENTITY
  }
  
  private EntitiesContextType contextType = EntitiesContextType.MENTION_ENTITY;
    
  private int numberOfEntityKeyphrases = Integer.MAX_VALUE;
  
  private boolean normalizeWeights = false; // default is not to normalize.
  private boolean useConfusableMIWeight = false;
  private boolean averageWeights = false;

  private int nGramLength = 2;
  
  public static final double DEFAULT_KEYPHRASE_ALPHA = 0.9713705285593512;
  public static final double DEFAULT_KEYWORD_ALPHA = 0.9713705285593512;

  private double entityCoherenceKeyphraseAlpha = DEFAULT_KEYPHRASE_ALPHA;
  private double entityCoherenceKeywordAlpha = DEFAULT_KEYWORD_ALPHA;
    
  // Different keyphrase source weights for MentionEntity and EntityEntity.
  private Map<String, Double> mentionEntityKeyphraseSourceWeights;
  private Map<String, Double> entityEntityKeyphraseSourceWeights;
  private double minimumEntityKeyphraseWeight;
  private int maxEntityKeyphraseCount;
  
  // LSH
  private int lshBandSize;
  private int lshBandCount;
  private String lshDatabaseTable;
    
  /**
   * 
   * @return Balance between Keyphrase MI/IDF. Use alpha*mi, (1-alpha)*idf 
   */
  public double getEntityCoherenceKeyphraseAlpha() {
    return entityCoherenceKeyphraseAlpha;
  }
  
  public void setEntityCoherenceKeyphraseAlpha(double entityCoherenceKeyphraseAlpha) {
    this.entityCoherenceKeyphraseAlpha = entityCoherenceKeyphraseAlpha;
  }

  /**
   * 
   * @return Balance between Keyword MI/IDF. Use alpha*mi, (1-alpha)*idf 
   */
  public double getEntityCoherenceKeywordAlpha() {
    return entityCoherenceKeywordAlpha;
  }
  
  public void setEntityCoherenceKeywordAlpha(double entityCoherenceKeywordAlpha) {
    this.entityCoherenceKeywordAlpha = entityCoherenceKeywordAlpha;
  }

  public int getNumberOfEntityKeyphrases() {
    return numberOfEntityKeyphrases;
  }
  
  public void setNumberOfEntityKeyphrases(int numberOfEntityKeyphrases) {
    this.numberOfEntityKeyphrases = numberOfEntityKeyphrases;
  }

  public Map<String, Double> getEntityEntityKeyphraseSourceWeights() {
    return entityEntityKeyphraseSourceWeights;
  }

  public void setEntityEntityKeyphraseSourceWeights(Map<String, Double> entityEntityKeyphraseSourceWeights) {
    this.entityEntityKeyphraseSourceWeights = entityEntityKeyphraseSourceWeights;
  }

  public Map<String, Double> getMentionEntityKeyphraseSourceWeights() {
    return mentionEntityKeyphraseSourceWeights;
  }

  public void setMentionEntityKeyphraseSourceWeights(
      Map<String, Double> mentionEntityKeyphraseSourceWeights) {
    this.mentionEntityKeyphraseSourceWeights = mentionEntityKeyphraseSourceWeights;
  }

  public boolean shouldNormalizeWeights() {
    return normalizeWeights;
  }
  
  public void setShouldNormalizeWeights(boolean flag) {
    normalizeWeights = flag;
  }

  public boolean shouldUseConfusableMIWeight() {
    return useConfusableMIWeight;
  }

  public void setUseConfusableMIWeight(boolean useConfusableMIWeight) {
    this.useConfusableMIWeight = useConfusableMIWeight;
  }

  public boolean shouldAverageWeights() {
    return averageWeights;
  }
  
  public void setShouldAverageWeights(boolean flag) {
    this.averageWeights = flag;
  }
  
  public void setNgramLength(int nGramLength) {
	  this.nGramLength = nGramLength;
  }
  
  public int getNgramLength() {
	  return nGramLength;
  }

  public int getLshBandSize() {
    return lshBandSize;
  }

  public void setLshBandSize(int lshBandSize) {
    this.lshBandSize = lshBandSize;
  }

  public int getLshBandCount() {
    return lshBandCount;
  }

  public void setLshBandCount(int lshBandCount) {
    this.lshBandCount = lshBandCount;
  }
  
  public String getLshDatabaseTable() {
    return lshDatabaseTable;
  }
 
  public void setLshDatabaseTable(String lshDatabaseTable) {
    this.lshDatabaseTable = lshDatabaseTable;
  }

  public double getMinimumEntityKeyphraseWeight() {
    return minimumEntityKeyphraseWeight;
  }

  public void setMinimumEntityKeyphraseWeight(double minimumEntityKeyphraseWeight) {
    this.minimumEntityKeyphraseWeight = minimumEntityKeyphraseWeight;
  }

  public EntitiesContextType getEntitiesContextType() {
    return contextType;
  }

  public void setEntitiesContextType(EntitiesContextType contextType) {
    this.contextType = contextType;
  }

  public int getMaxEntityKeyphraseCount() {
    return maxEntityKeyphraseCount;
  }

  public void setMaxEntityKeyphraseCount(int maxEntityKeyphraseCount) {
    this.maxEntityKeyphraseCount = maxEntityKeyphraseCount;
  }
} 
