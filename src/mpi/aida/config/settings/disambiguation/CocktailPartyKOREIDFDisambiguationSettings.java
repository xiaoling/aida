package mpi.aida.config.settings.disambiguation;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import mpi.aida.access.DataAccess;
import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.Settings.ALGORITHM;
import mpi.aida.config.settings.Settings.TECHNIQUE;
import mpi.aida.graph.similarity.exception.MissingSettingException;
import mpi.aida.graph.similarity.util.SimilaritySettings;
import mpi.experiment.trace.GraphTracer.TracingTarget;

/**
 * Preconfigured settings for the {@see Disambiguator} using the mention-entity
 * the keyphrase based similarity using idf scores only, and the KORE keyphrase based
 * entity coherence.
 */
public class CocktailPartyKOREIDFDisambiguationSettings extends DisambiguationSettings {
    
  private static final long serialVersionUID = 5867674989478781057L;

  public CocktailPartyKOREIDFDisambiguationSettings() throws MissingSettingException {
    setAlpha(0.6);
    setTracingTarget(TracingTarget.WEB_INTERFACE);
     
    setDisambiguationTechnique(TECHNIQUE.GRAPH);
    setDisambiguationAlgorithm(ALGORITHM.COCKTAIL_PARTY_SIZE_CONSTRAINED);
    setUseExhaustiveSearch(true);
    setUseNormalizedObjective(true);
    setEntitiesPerMentionConstraint(5);
    setUseCoherenceRobustnessTest(false);
    
    Map<String, double[]> minMaxs = new HashMap<String, double[]>();
    minMaxs.put("UnnormalizedKeyphrasesBasedIDFSimilarity:KeyphrasesContext", new double[] { 0.0, 63207.231647131});
    minMaxs.put("AidaEntityImportance", new double[] { 0.0, 1.0});

    List<String[]> simConfigs = new LinkedList<String[]>();
    simConfigs.add(new String[] { "UnnormalizedKeyphrasesBasedIDFSimilarity", "KeyphrasesContext", "0.5" });  
    List<String[]> cohConfigs = new LinkedList<String[]>();
    cohConfigs.add(new String[] { "KOREEntityEntitySimilarity", "1.0" });
    List<String[]> eisConfigs = new LinkedList<String[]>();
    eisConfigs.add(new String[] { "AidaEntityImportance:0.5" });
        
    SimilaritySettings settings = new SimilaritySettings(simConfigs, cohConfigs, eisConfigs, 0.0, minMaxs);
    settings.setIdentifier("idf-sims");
    settings.setEntityCohKeyphraseAlpha(0.0);
    settings.setEntityCohKeywordAlpha(0.0);
    settings.setShouldNormalizeCoherenceWeights(true);
    settings.setEntityEntityKeyphraseSourceExclusion(DataAccess.KPSOURCE_INLINKTITLE);
    setSimilaritySettings(settings);
  }
}