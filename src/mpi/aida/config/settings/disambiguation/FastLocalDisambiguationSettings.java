package mpi.aida.config.settings.disambiguation;

import mpi.aida.graph.similarity.exception.MissingSettingException;

/**
 * Preconfigured settings for the {@see Disambiguator} using only the 
 * mention-entity prior and the keyphrase based similarity.
 */
public class FastLocalDisambiguationSettings extends LocalDisambiguationSettings{
    
  private static final long serialVersionUID = -1943862223862927646L;

  public FastLocalDisambiguationSettings() throws MissingSettingException {
    super();
    getSimilaritySettings().setMaxEntityKeyphraseCount(1000);
    getSimilaritySettings().setMinimumEntityKeyphraseWeight(0.001);
  }
}
