package mpi.aida.config.settings.disambiguation;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.Settings.ALGORITHM;
import mpi.aida.config.settings.Settings.TECHNIQUE;
import mpi.aida.graph.similarity.exception.MissingSettingException;
import mpi.aida.graph.similarity.util.SimilaritySettings;
import mpi.experiment.trace.GraphTracer.TracingTarget;

/**
 * Preconfigured settings for the {@see Disambiguator} using the mention-entity
 * prior, the keyphrase based similarity, and the MilneWitten Wikipedia link
 * based entity coherence.
 * 
 * This gives the best quality and should be used in comparing results against
 * AIDA. 
 */
public class CocktailPartyDisambiguationSettings extends DisambiguationSettings {
    
  private static final long serialVersionUID = 5867674989478781057L;
  
  public CocktailPartyDisambiguationSettings() throws MissingSettingException {
    getGraphSettings().setAlpha(0.6);
    setTracingTarget(TracingTarget.WEB_INTERFACE);
     
    setDisambiguationTechnique(TECHNIQUE.GRAPH);
    setDisambiguationAlgorithm(ALGORITHM.COCKTAIL_PARTY_SIZE_CONSTRAINED);
    getGraphSettings().setUseExhaustiveSearch(true);
    getGraphSettings().setUseNormalizedObjective(true);
    getGraphSettings().setEntitiesPerMentionConstraint(5);
    getGraphSettings().setUseCoherenceRobustnessTest(true);
    getGraphSettings().setCohRobustnessThreshold(0.9);
    
    List<String[]> cohConfigs = new LinkedList<String[]>();
    cohConfigs.add(new String[] { "MilneWittenEntityEntitySimilarity", "1.0" });

    SimilaritySettings switchedKPsettings = 
        new SimilaritySettings(
            LocalDisambiguationSettings.getSimConfigs(), 
            cohConfigs, LocalDisambiguationSettings.priorWeight);
    switchedKPsettings.setIdentifier("SwitchedKP");
    switchedKPsettings.setPriorThreshold(0.9);
    setSimilaritySettings(switchedKPsettings);
        
    SimilaritySettings unnormalizedKPsettings = new SimilaritySettings(getCoherenceRobustnessSimConfigs(), null, 0.0);
    switchedKPsettings.setIdentifier("CoherenceRobustnessTest");
    getGraphSettings().setCoherenceSimilaritySetting(unnormalizedKPsettings);
  }
  
  public static List<String[]> getCoherenceRobustnessSimConfigs() { 
      return Arrays.asList(new String[][] {
          new String[] { "UnnormalizedKeyphrasesBasedMISimilarity", "KeyphrasesContext", "0.8360808680254525" },
          new String[] { "UnnormalizedKeyphrasesBasedIDFSimilarity", "KeyphrasesContext", "0.16391913197454755" }          
      });
  }
}