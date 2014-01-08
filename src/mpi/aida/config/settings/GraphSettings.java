package mpi.aida.config.settings;

import java.util.HashMap;
import java.util.Map;

import mpi.aida.graph.similarity.util.SimilaritySettings;


public class GraphSettings {
  
  /** 
   * Balances the mention-entity edge weights (alpha) 
   * and the entity-entity edge weights (1-alpha)
   */
  private double alpha = 0;
  
  /**
   * Set to true to use exhaustive search in the final solving stage of
   * ALGORITHM.COCKTAIL_PARTY. Set to false to do a hill-climbing search
   * from a random starting point.
   */
  private boolean useExhaustiveSearch = false;

  /**
   * Set to true to normalize the minimum weighted degree in the 
   * ALGORITHM.COCKTAIL_PARTY by the number of graph nodes. This prefers
   * smaller solutions.
   */
  private boolean useNormalizedObjective = false;
  
  /**
   * Settings to compute the initial mention-entity edge weights when
   * using coherence robustness.
   */
  private SimilaritySettings coherenceSimilaritySetting = null;

  /**
   * Number of candidates to keep for for 
   * ALGORITHM.COCKTAIL_PARTY_SIZE_CONSTRAINED.
   */
  private int entitiesPerMentionConstraint = 5;
  
  /**
   * Set to true to enable the coherence robustness test, fixing mentions
   * with highly similar prior and similarity distribution to the most
   * promising candidate before running the graph algorithm.
   */
  private boolean useCoherenceRobustnessTest = true;

  /**
   * Threshold of the robustness test, below which the the L1-norm between
   * prior and sim results in the fixing of the entity candidate.
   */
  private double cohRobustnessThreshold = 0;
  
  public boolean shouldUseExhaustiveSearch() {
    return useExhaustiveSearch;
  }

  public void setUseExhaustiveSearch(boolean useExhaustiveSearch) {
    this.useExhaustiveSearch = useExhaustiveSearch;
  }

  public double getAlpha() {
    return alpha;
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }
  
  public SimilaritySettings getCoherenceSimilaritySetting() {
    return coherenceSimilaritySetting;
  }

  public void setCoherenceSimilaritySetting(SimilaritySettings similaritySettings) {
    this.coherenceSimilaritySetting = similaritySettings;
  }

  public int getEntitiesPerMentionConstraint() {
    return entitiesPerMentionConstraint;
  }

  public void setEntitiesPerMentionConstraint(int entitiesPerMentionConstraint) {
    this.entitiesPerMentionConstraint = entitiesPerMentionConstraint;
  }

  public double getCohRobustnessThreshold() {
    return cohRobustnessThreshold;
  }

  public void setCohRobustnessThreshold(double cohRobustnessThreshold) {
    this.cohRobustnessThreshold = cohRobustnessThreshold;
  }

  public boolean shouldUseNormalizedObjective() {
    return useNormalizedObjective;
  }

  public void setUseNormalizedObjective(boolean useNormalizedObjective) {
    this.useNormalizedObjective = useNormalizedObjective;
  }
  
  public boolean shouldUseCoherenceRobustnessTest() {
    return useCoherenceRobustnessTest;
  }

  public void setUseCoherenceRobustnessTest(boolean useCoherenceRobustnessTest) {
    this.useCoherenceRobustnessTest = useCoherenceRobustnessTest;
  }

  public Map<String, Object> getAsMap() {
    Map<String, Object> s = new HashMap<String, Object>();
    s.put("alpha", String.valueOf(alpha));
    s.put("useExhaustiveSearch", String.valueOf(useExhaustiveSearch));
    s.put("useNormalizedObjective", String.valueOf(useNormalizedObjective));
    if (coherenceSimilaritySetting != null) {
      s.put("coherenceSimilaritySetting", coherenceSimilaritySetting.getAsMap());
    }
    s.put("entitiesPerMentionConstraint", String.valueOf(entitiesPerMentionConstraint));
    s.put("cohRobustnessThreshold", String.valueOf(cohRobustnessThreshold));
    s.put("useCoherenceRobustnessTest", String.valueOf(useCoherenceRobustnessTest));
    return s;
  }
}
