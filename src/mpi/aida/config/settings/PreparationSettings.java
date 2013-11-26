package mpi.aida.config.settings;

import java.io.Serializable;

import mpi.aida.config.AidaConfig;
import mpi.aida.data.Type;
import mpi.aida.preparation.mentionrecognition.FilterMentions.FilterType;

/**
 * Settings for the preparator. Predefined settings are available in
 * {@see mpi.aida.config.settings.preparation}.
 */
public class PreparationSettings implements Serializable {

  private static final long serialVersionUID = -2825720730925914648L;

  private FilterType mentionsFilter = FilterType.STANFORD_NER;
  
  private boolean useHybridMentionDetection = true;
  
  /** 
   * Minimum number of mention occurrence to be considered in disambiguation.
   * Default is to consider all mentions.
   */
  private int minMentionOccurrenceCount = 1;

  private Type[] filteringTypes = AidaConfig.getFilteringTypes();
  
  //default to the language in AIDA configuration
  private LANGUAGE language = AidaConfig.getLanguage();
  
  public static enum LANGUAGE {
    en, de
  }

  public FilterType getMentionsFilter() {
    return mentionsFilter;
  }

  public void setMentionsFilter(FilterType mentionsFilter) {
    this.mentionsFilter = mentionsFilter;
  }

  public Type[] getFilteringTypes() {
    return filteringTypes;
  }

  public void setFilteringTypes(Type[] filteringTypes) {
    this.filteringTypes = filteringTypes;
  }
  
  public LANGUAGE getLanguage() {
    return language;
  }
  
  public void setLanguage(LANGUAGE language) {
    this.language = language;
  }
  
  public boolean isUseHybridMentionDetection() {
    return useHybridMentionDetection;
  }
  
  public void setUseHybridMentionDetection(boolean useHybridMentionDetection) {
    this.useHybridMentionDetection = useHybridMentionDetection;
  }

  public int getMinMentionOccurrenceCount() {
    return minMentionOccurrenceCount;
  }

  public void setMinMentionOccurrenceCount(int minMentionOccurrenceCount) {
    this.minMentionOccurrenceCount = minMentionOccurrenceCount;
  }
}
