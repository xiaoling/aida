package mpi.aida.config.settings.disambiguation;

import mpi.aida.graph.similarity.exception.MissingSettingException;

/**
 * Preconfigured settings for the {@see Disambiguator} using the mention-entity
 * prior, the keyphrase based similarity, and the MilneWitten Wikipedia link
 * based entity coherence.
 * 
 * This gives the best quality and should be used in comparing results against
 * AIDA. 
 */
public class FastCocktailPartyDisambiguationSettings extends CocktailPartyDisambiguationSettings {
    
  private static final long serialVersionUID = 5867674989478781057L;

  public FastCocktailPartyDisambiguationSettings() throws MissingSettingException {
    super();
    getSimilaritySettings().setMaxEntityKeyphraseCount(1000);
    getSimilaritySettings().setMinimumEntityKeyphraseWeight(0.001);
  }
}