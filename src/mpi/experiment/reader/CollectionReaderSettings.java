package mpi.experiment.reader;

/**
 * Configuration object for CollectionReaders.
 */
public class CollectionReaderSettings {
  
  /**
   * Set to true to include mentions without a ground truth annotation.
   */
  private boolean includeNMEMentions_;
  
  /**
   * Set to true to include mentions that do not have a dictionary entry.
   */
  private boolean includeOutOfDictionaryMentions_;
  
  /** 
   * Used for NewsStreamReader.
   */
  private int minMentionOccurrence_ = 0;
 
  public boolean isIncludeNMEMentions() {
    return includeNMEMentions_;
  }
  
  public void setIncludeNMEMentions(boolean includeNMEMentions) {
    this.includeNMEMentions_ = includeNMEMentions;
  }

  public boolean isIncludeOutOfDictionaryMentions() {
    return includeOutOfDictionaryMentions_;
  }

  public void setIncludeOutOfDictionaryMentions(
      boolean includeOutOfDictionaryMentions) {
    this.includeOutOfDictionaryMentions_ = includeOutOfDictionaryMentions;
  }

  public int getMinMentionOccurrence() {
    return minMentionOccurrence_;
  }

  public void setMinMentionOccurrence(int minMentionOccurrence_) {
    this.minMentionOccurrence_ = minMentionOccurrence_;
  }
}
