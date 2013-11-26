package mpi.aida.config.settings.disambiguation;

import java.util.LinkedList;
import java.util.List;

import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.Settings.TECHNIQUE;
import mpi.aida.graph.similarity.exception.MissingSettingException;
import mpi.aida.graph.similarity.util.SimilaritySettings;

/**
 * Preconfigured settings for the {@see Disambiguator} using only the 
 * keyphrase based similarity based only on idf counts.
 */
public class ImportanceOnlyDisambiguationSettings extends DisambiguationSettings {

  private static final long serialVersionUID = -6391627336407534940L;

  public ImportanceOnlyDisambiguationSettings() throws MissingSettingException {
    setDisambiguationTechnique(TECHNIQUE.LOCAL);

    List<String[]> eisConfigs = new LinkedList<String[]>();
    eisConfigs.add(new String[] { "AidaEntityImportance","0.5" });
    
    SimilaritySettings localIDFPsettings = new SimilaritySettings(null, null, eisConfigs, 0);
    localIDFPsettings.setIdentifier("importance");
    setSimilaritySettings(localIDFPsettings);
    
    setIncludeNullAsEntityCandidate(false);
  }
}
