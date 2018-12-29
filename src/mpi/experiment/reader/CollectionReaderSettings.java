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

	/**
	 * Set to true to keep a space before a punctuation
	 * 
	 * @author xiaoling
	 */
	private boolean keepSpaceBeforePunctuations_ = false;

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

	public boolean isKeepSpaceBeforePunctuations() {
		return keepSpaceBeforePunctuations_;
	}

	public void setKeepSpaceBeforePunctuations(
			boolean keepSpaceBeforePunctuations_) {
		this.keepSpaceBeforePunctuations_ = keepSpaceBeforePunctuations_;
	}
}
