package mpi.aida.config.settings.disambiguation;

import java.util.LinkedList;
import java.util.List;

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
    getGraphSettings().setAlpha(0.6);
    setTracingTarget(TracingTarget.WEB_INTERFACE);
     
    setDisambiguationTechnique(TECHNIQUE.GRAPH);
    setDisambiguationAlgorithm(ALGORITHM.COCKTAIL_PARTY_SIZE_CONSTRAINED);
    getGraphSettings().setUseExhaustiveSearch(true);
    getGraphSettings().setUseNormalizedObjective(true);
    getGraphSettings().setEntitiesPerMentionConstraint(5);
    getGraphSettings().setUseCoherenceRobustnessTest(false);
    
    List<String[]> simConfigs = new LinkedList<String[]>();
    simConfigs.add(new String[] { "UnnormalizedKeyphrasesBasedIDFSimilarity", "KeyphrasesContext", "0.5" });  
    List<String[]> cohConfigs = new LinkedList<String[]>();
    cohConfigs.add(new String[] { "KOREEntityEntitySimilarity", "1.0" });
    List<String[]> eisConfigs = new LinkedList<String[]>();
    eisConfigs.add(new String[] { "AidaEntityImportance", "0.5" });
        
    SimilaritySettings settings = new SimilaritySettings(simConfigs, cohConfigs, eisConfigs, 0.0);
    settings.setIdentifier("idf-sims");
    settings.setEntityCohKeyphraseAlpha(0.0);
    settings.setEntityCohKeywordAlpha(0.0);
    settings.setShouldNormalizeCoherenceWeights(true);
    List<String[]> sourceWeightConfigs = new LinkedList<String[]>();
    sourceWeightConfigs.add(new String[] { DataAccess.KPSOURCE_INLINKTITLE, "0.0" });
    settings.setEntityEntityKeyphraseSourceWeights(sourceWeightConfigs);
    setSimilaritySettings(settings);
  }
}