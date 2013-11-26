package mpi.aida.graph.similarity.measure;

import mpi.aida.data.Entity;
import mpi.experiment.trace.Tracer;


public class UnnormalizedKeyphrasesBasedMISimilarity extends KeyphrasesBasedMentionEntitySimilarityMeasure {
   public UnnormalizedKeyphrasesBasedMISimilarity(Tracer tracer) {
    super(tracer);
  }

  protected double getKeywordScore(Entity entity, int keyword) {
    double score = keyphrasesContext.getKeywordMIWeight(entity, keyword);
    return score;
  }

  public String getIdentifier() {
    String identifier = "UnnormalizedKeyphrasesBasedMISimilarity";

    if (isUseDistanceDiscount()) {
      identifier += ",i";
    }

    return identifier;
  }

}
