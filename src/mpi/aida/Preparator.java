package mpi.aida;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mpi.aida.config.settings.PreparationSettings;
import mpi.aida.data.PreparedInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Preparator {
  private Logger logger_ = LoggerFactory.getLogger(Preparator.class);
  
  private Integer processedDocuments = 0;
  
  private Map<String, Integer> processedDocuments_ =
      new HashMap<String, Integer>();
  
  /**
   * Convenience wrapper around the prepare() method, creating a docId
   * internally.
   * 
   * @param text  Text to prepare, i.e. tokenize and recognize mentions.
   * @param settings  Settings to use for the preparation.
   * @return  Text prepared for use with the Disambiguator.
   */
  public PreparedInput prepare(
      String text, PreparationSettings settings) {
    String docId = null;
    synchronized(processedDocuments) {
      ++processedDocuments;
      docId = String.valueOf(processedDocuments);
    }
    return prepare(docId, text, settings);
  }

  /**
   * Prepare an input text to be disambiguated. Mentions of named entities 
   * will be detected depending on the passed PreparationSettings.
   * 
   * @param docId Distinct id for this document (must not be re-used)
   * @param text  Text to prepare, i.e. tokenize and recognize mentions.
   * @param settings  Settings to use for the preparation.
   * @return  Text prepared for use with the Disambiguator.
   */
  public PreparedInput prepare(
      String docId, String text, PreparationSettings settings) {
    Integer hash = text.hashCode();
    Integer processedHash = processedDocuments_.get(docId);
    if (processedHash != null) {
      if (hash != processedHash) {
        logger_.warn("A document with the id '" + docId + "' has already " +
        		"been processed, but the content has changed. Make sure to use " +
        		"distinct docId parameters for distinct documents, otherwise " +
        		"the disambiguation will not work properly!");
        throw new IllegalArgumentException("Same docId used for distinct " +
        		"documents.");
      }
    } else {
      processedDocuments_.put(docId, hash);
    }
    PreparedInput preparedInput = AidaManager.prepareInputData(text, docId, settings.getMentionsFilter());
    String[] types = settings.getFilteringTypes();
    if (types != null) {
      List<String> filteringTypes = Arrays.asList(settings.getFilteringTypes());
      preparedInput.getMentions().setEntitiesTypes(filteringTypes);
    }
    return preparedInput;
  }
}
