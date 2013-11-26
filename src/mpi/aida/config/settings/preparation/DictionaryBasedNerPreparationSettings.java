package mpi.aida.config.settings.preparation;

import mpi.aida.config.settings.PreparationSettings;
import mpi.aida.preparation.mentionrecognition.FilterMentions.FilterType;


public class DictionaryBasedNerPreparationSettings extends PreparationSettings {

  private static final long serialVersionUID = 3743560957961384100L;

  public DictionaryBasedNerPreparationSettings() {
    this.setMentionsFilter(FilterType.DICTIONARY);
  }
}
