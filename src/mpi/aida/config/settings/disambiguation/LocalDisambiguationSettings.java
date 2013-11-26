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
  
  public static final List<String[]> simConfigs = 
      Arrays.asList(new String[][] {
          new String[] { "UnnormalizedKeyphrasesBasedMISimilarity", "KeyphrasesContext", "2.23198783427544E-6" },
          new String[] { "UnnormalizedKeyphrasesBasedIDFSimilarity", "KeyphrasesContext", "2.6026462624132183E-4" },
          new String[] { "UnnormalizedKeyphrasesBasedMISimilarity", "KeyphrasesContext", "0.0817134645946377" },
          new String[] { "UnnormalizedKeyphrasesBasedIDFSimilarity", "KeyphrasesContext", "0.3220317242447891" }
      });
  
  public static final Double priorWeight = 0.5959923145464976;
  
  private static final long serialVersionUID = -1943862223862927646L;

  public LocalDisambiguationSettings() throws MissingSettingException {
    setDisambiguationTechnique(TECHNIQUE.LOCAL);
   
    SimilaritySettings switchedKPsettings = new SimilaritySettings(simConfigs, null, priorWeight);
    switchedKPsettings.setIdentifier("SwitchedKP");
    switchedKPsettings.setPriorThreshold(0.9);
    setSimilaritySettings(switchedKPsettings);
    
    setIncludeNullAsEntityCandidate(false);
  }
}
