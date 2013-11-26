package mpi.aida.config.settings.disambiguation;

import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.Settings.TECHNIQUE;
import mpi.aida.graph.similarity.exception.MissingSettingException;
import mpi.aida.graph.similarity.util.SimilaritySettings;

/**
 * Preconfigured settings for the {@see Disambiguator} using only the 
 * mention-entity prior.
 */
public class PriorOnlyDisambiguationSettings extends DisambiguationSettings {
    
  private static final long serialVersionUID = 2212272023159361340L;

  public PriorOnlyDisambiguationSettings() throws MissingSettingException {
    setDisambiguationTechnique(TECHNIQUE.LOCAL);
    
    SimilaritySettings priorSettings = new SimilaritySettings(null, null, 1.0);
    priorSettings.setIdentifier("Prior");
    setSimilaritySettings(priorSettings);
    
    setIncludeNullAsEntityCandidate(false);
  }
}


