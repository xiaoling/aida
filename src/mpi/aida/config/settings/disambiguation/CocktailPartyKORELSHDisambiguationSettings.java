package mpi.aida.config.settings.disambiguation;

import java.util.LinkedList;
import java.util.List;

import mpi.aida.access.DataAccess;
import mpi.aida.access.DataAccessSQL;
import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.Settings.ALGORITHM;
import mpi.aida.config.settings.Settings.TECHNIQUE;
import mpi.aida.graph.similarity.exception.MissingSettingException;
import mpi.aida.graph.similarity.util.SimilaritySettings;
import mpi.experiment.trace.GraphTracer.TracingTarget;

/**
 * Preconfigured settings for the {@see Disambiguator} using the mention-entity
 * prior, the keyphrase based similarity, and the KORE keyphrase based
 * entity coherence.
 */
public class CocktailPartyKORELSHDisambiguationSettings extends DisambiguationSettings {
    
  private static final long serialVersionUID = 5867674989478781057L;

  public CocktailPartyKORELSHDisambiguationSettings() throws MissingSettingException {
    setAlpha(0.6);
    setTracingTarget(TracingTarget.WEB_INTERFACE);
     
    setDisambiguationTechnique(TECHNIQUE.GRAPH);
    setDisambiguationAlgorithm(ALGORITHM.COCKTAIL_PARTY_SIZE_CONSTRAINED);
    setUseExhaustiveSearch(true);
    setUseNormalizedObjective(true);
    setEntitiesPerMentionConstraint(5);
    setUseCoherenceRobustnessTest(true);
    setCohRobustnessThreshold(0.9);
    
    List<String[]> cohConfigs = new LinkedList<String[]>();
    cohConfigs.add(new String[] { "KORELSHEntityEntitySimilarity", "1.0" });

    SimilaritySettings switchedKPsettings = new SimilaritySettings(
        LocalDisambiguationSettings.simConfigs, cohConfigs, 
        LocalDisambiguationSettings.priorWeight);
    switchedKPsettings.setIdentifier("SwitchedKP");
    switchedKPsettings.setPriorThreshold(0.9);
    switchedKPsettings.setEntityCohKeyphraseAlpha(1.0);
    switchedKPsettings.setEntityCohKeywordAlpha(0.0);
    switchedKPsettings.setShouldNormalizeCoherenceWeights(true);
    List<String[]> sourceWeightConfigs = new LinkedList<String[]>();
    sourceWeightConfigs.add(new String[] { DataAccess.KPSOURCE_INLINKTITLE, "0.0" });
    switchedKPsettings.setEntityEntityKeyphraseSourceWeights(sourceWeightConfigs);
    switchedKPsettings.setLshBandSize(2);
    switchedKPsettings.setLshBandCount(100);
    switchedKPsettings.setLshDatabaseTable(DataAccessSQL.ENTITY_LSH_SIGNATURES);
    setSimilaritySettings(switchedKPsettings);
        
    SimilaritySettings unnormalizedKPsettings = new SimilaritySettings(
        CocktailPartyDisambiguationSettings.coherenceRobustnessSimConfigs, null, 0.0);
    switchedKPsettings.setIdentifier("CoherenceRobustnessTest");
    setCoherenceSimilaritySetting(unnormalizedKPsettings);
  }
}