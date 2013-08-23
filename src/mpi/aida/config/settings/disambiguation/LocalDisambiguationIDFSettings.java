package mpi.aida.config.settings.disambiguation;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.Settings.TECHNIQUE;
import mpi.aida.graph.similarity.exception.MissingSettingException;
import mpi.aida.graph.similarity.util.SimilaritySettings;

/**
 * Preconfigured settings for the {@see Disambiguator} using only the 
 * keyphrase based similarity based only on idf counts.
 */
public class LocalDisambiguationIDFSettings extends DisambiguationSettings {

  private static final long serialVersionUID = -6391627336407534940L;

  public LocalDisambiguationIDFSettings() throws MissingSettingException {
    setDisambiguationTechnique(TECHNIQUE.LOCAL);
    
    List<String[]> simConfigs = new LinkedList<String[]>();
    simConfigs.add(new String[] { "UnnormalizedKeyphrasesBasedIDFSimilarity", "KeyphrasesContext", "0.5" });   
    
    Map<String, double[]> minMaxs = new HashMap<String, double[]>();
    minMaxs.put("UnnormalizedKeyphrasesBasedIDFSimilarity:KeyphrasesContext", new double[] { 0.0, 63207.231647131});
    minMaxs.put("AidaEntityImportance", new double[] { 0.0, 1.0});

    List<String[]> eisConfigs = new LinkedList<String[]>();
    eisConfigs.add(new String[] { "AidaEntityImportance:0.5" });
    
    SimilaritySettings localIDFPsettings = new SimilaritySettings(simConfigs, null, eisConfigs, 0, minMaxs);
    localIDFPsettings.setIdentifier("LocalIDF");
    setSimilaritySettings(localIDFPsettings);
    
    setIncludeNullAsEntityCandidate(false);
  }
}
