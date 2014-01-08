package mpi.aida.config.settings;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import mpi.aida.graph.similarity.util.SimilaritySettings;
import mpi.experiment.trace.GraphTracer.TracingTarget;

/**
 * Settings for the Disambigutor. Configures the disambiguation process.
 * Pre-configured settings are available in the 
 * {@see mpi.aida.config.settings.disambiguation} package.
 */
public class DisambiguationSettings implements Serializable {

  private static final long serialVersionUID = 1210739181527236465L;

  /**
   * Technique to solve the disambiguation graph with. Most commonly this
   * is LOCAL for mention-entity similarity edges only and 
   * GRAPH to include the entity coherence.
   */
  private Settings.TECHNIQUE disambiguationTechnique = null;

  /**
   * If TECHNIQUE.GRAPH is chosen above, this specifies the algorithm to
   * solve the disambiguation graph. Can be COCKTAIL_PARTY for the full
   * disambiguation graph, COCKTAIL_PARTY_SIZE_CONSTRAINED for a heuristically
   * pruned graph, and RANDOM_WALK for using a PageRank style random walk
   * to find the best entity.
   */
  private Settings.ALGORITHM disambiguationAlgorithm = null;

  /**
   * Settings to compute the edge-weights of the disambiguation graph.
   */
  private SimilaritySettings similaritySettings = null;
  
  /**
   * Maximum (global) rank of the entity according to the entity_rank table.
   * Currently, entities are ranked by number of inlinks. 0.0 means no entity
   * is included, 1.0 means all are included. Default is to include all.
   */
  private double maxEntityRank = 1.0;
  
  private double nullMappingThreshold = -1.0;
  
  private boolean includeNullAsEntityCandidate = false;
  
  private boolean includeContextMentions = false;
  
  private String storeFile = null;

  private TracingTarget tracingTarget = null;
  
  /**
   * Settings to use for graph computation.
   */
  private GraphSettings graphSettings;
  
  /**
   * If true, compute the confidence of the mapping instead of assigning
   * the local (mention-entity) similarity as score.
   */
  private boolean computeConfidence = false;
    
  /**
   * Settings to use for confidence computation.
   */
  private ConfidenceSettings confidenceSettings;
  
  /**
   * Number of chunks to process in parallel.
   */
  private int numChunkThreads = 4;  
  
  public DisambiguationSettings() {
    graphSettings = new GraphSettings();
    confidenceSettings = new ConfidenceSettings();
  }
          
  public void setStoreFile(String path) {
    this.storeFile = path;
  }

  public String getStoreFile() {
    return storeFile;
  }

  public void setTracingTarget(TracingTarget tracingTarget) {
    this.tracingTarget = tracingTarget;
  }

  public TracingTarget getTracingTarget() {
    return tracingTarget;
  }

  public Settings.TECHNIQUE getDisambiguationTechnique() {
    return disambiguationTechnique;
  }

  public void setDisambiguationTechnique(Settings.TECHNIQUE disambiguationTechnique) {
    this.disambiguationTechnique = disambiguationTechnique;
  }

  public Settings.ALGORITHM getDisambiguationAlgorithm() {
    return disambiguationAlgorithm;
  }

  public void setDisambiguationAlgorithm(Settings.ALGORITHM disambiguationAlgorithm) {
    this.disambiguationAlgorithm = disambiguationAlgorithm;
  }

  public SimilaritySettings getSimilaritySettings() {
    return similaritySettings;
  }

  public void setSimilaritySettings(SimilaritySettings similaritySettings) {
    this.similaritySettings = similaritySettings;
  }

  public double getNullMappingThreshold() {
    return nullMappingThreshold;
  }
  
  public void setNullMappingThreshold(double nullMappingThreshold) {
    this.nullMappingThreshold = nullMappingThreshold;
  }

  public boolean isIncludeNullAsEntityCandidate() {
    return includeNullAsEntityCandidate;
  }

  public void setIncludeNullAsEntityCandidate(boolean includeNullAsEntityCandidate) {
    this.includeNullAsEntityCandidate = includeNullAsEntityCandidate;
  }

  public void setIncludeContextMentions(boolean flag) {
    this.includeContextMentions = flag;
  }
  
  public boolean isIncludeContextMentions() {
    return includeContextMentions;
  }

  public double getMaxEntityRank() {
    return maxEntityRank;
  }

  public void setMaxEntityRank(double maxEntityRank) {
    this.maxEntityRank = maxEntityRank;
  }

  public boolean shouldComputeConfidence() {
    return computeConfidence;
  }

  public void setComputeConfidence(boolean computeConfidence) {
    this.computeConfidence = computeConfidence;
  }

  public ConfidenceSettings getConfidenceSettings() {
    return confidenceSettings;
  }

  public void setConfidenceSettings(ConfidenceSettings confidenceSettings) {
    this.confidenceSettings = confidenceSettings;
  }

  public GraphSettings getGraphSettings() {
    return graphSettings;
  }

  public void setGraphSettings(GraphSettings graphSettings) {
    this.graphSettings = graphSettings;
  }
  
  public int getNumChunkThreads() {
    return numChunkThreads;
  }

  public void setNumChunkThreads(int numChunkThreads) {
    this.numChunkThreads = numChunkThreads;
  }

  public Map<String, Object> getAsMap() {
    Map<String, Object> s = new HashMap<String, Object>();
    if (disambiguationTechnique != null) {
      s.put("disambiguationTechnique", disambiguationTechnique.toString());
    }
    if (disambiguationAlgorithm != null) {
      s.put("disambiguationAlgorithm", disambiguationAlgorithm.toString());
    }
    s.put("maxEntityRank", String.valueOf(maxEntityRank));
    s.put("nullMappingThreshold", String.valueOf(nullMappingThreshold));
    s.put("includeNullAsEntityCandidate", String.valueOf(includeNullAsEntityCandidate));
    s.put("includeContextMentions", String.valueOf(includeContextMentions));
    s.put("computeConfidence", String.valueOf(computeConfidence));
    if (similaritySettings != null) {
      s.put("similaritySettings", similaritySettings.getAsMap());
    }
    if (confidenceSettings != null) {
      s.put("confidenceSettings", confidenceSettings.getAsMap());
    }
    if (graphSettings != null) {
      s.put("graphSettings", graphSettings.getAsMap());
    }
    return s;
  }
}
