package mpi.aida.config.settings.disambiguation;

import java.util.Arrays;
import java.util.List;

import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.Settings.TECHNIQUE;
import mpi.aida.graph.similarity.exception.MissingSettingException;
import mpi.aida.graph.similarity.util.SimilaritySettings;

/**
 * Preconfigured settings for the {@see Disambiguator} using only the 
 * mention-entity prior and the keyphrase based similarity.
 */
public class LocalDisambiguationSettings extends DisambiguationSettings {
    
  public static final Double priorWeight = 0.5650733990091601;
    
  private static final long serialVersionUID = -1943862223862927646L;

  public LocalDisambiguationSettings() throws MissingSettingException {
    setDisambiguationTechnique(TECHNIQUE.LOCAL);
   
    SimilaritySettings switchedKPsettings = new SimilaritySettings(getSimConfigs(), null, priorWeight);
    switchedKPsettings.setIdentifier("SwitchedKP");
    switchedKPsettings.setPriorThreshold(0.9);
    setSimilaritySettings(switchedKPsettings);
    
    setIncludeNullAsEntityCandidate(false);
  }
  
  public static List<String[]> getSimConfigs() {
    return Arrays.asList(new String[][] {
        new String[] { "UnnormalizedKeyphrasesBasedMISimilarity", "KeyphrasesContext", "1.0236515882369545E-4" },
        new String[] { "UnnormalizedKeyphrasesBasedIDFSimilarity", "KeyphrasesContext", "7.372542270717097E-5" },
        new String[] { "UnnormalizedKeyphrasesBasedMISimilarity", "KeyphrasesContext", "0.10121900379778125" },
        new String[] { "UnnormalizedKeyphrasesBasedIDFSimilarity", "KeyphrasesContext", "0.33353150661152775" }
    });
  }
}
