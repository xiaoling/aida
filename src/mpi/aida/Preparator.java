package mpi.aida;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpi.aida.config.settings.PreparationSettings;
import mpi.aida.data.Mention;
import mpi.aida.data.Mentions;
import mpi.aida.data.PreparedInput;
import mpi.aida.data.Type;
import mpi.aida.preparation.documentchunking.DocumentChunker;
import mpi.aida.preparation.mentionrecognition.FilterMentions;
import mpi.aida.preparation.mentionrecognition.FilterMentions.FilterType;
import mpi.aida.util.RunningTimer;
import mpi.tokenizer.data.Tokens;
import mpi.tools.javatools.datatypes.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Preparator {
  private Logger logger_ = LoggerFactory.getLogger(Preparator.class);
  
  private Integer processedDocuments = 0;
  
  private static FilterMentions filterMention = new FilterMentions();
  
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
    Integer timerId = RunningTimer.start("Preparator");
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
    PreparedInput preparedInput = prepareInputData(text, docId, settings);

    Type[] types = settings.getFilteringTypes();
    if (types != null) {
      logger_.info("Entity Types Filter Set!");
      Set<Type> filteringTypes = new HashSet<Type>(Arrays.asList(settings.getFilteringTypes()));
      preparedInput.setMentionEntitiesTypes(filteringTypes);
    }
    RunningTimer.end("Preparator", timerId);
    return preparedInput;
  }  

  private PreparedInput prepareInputData(String text, String docId, PreparationSettings settings) {
    Pair<Tokens, Mentions> tokensMentions = null;
    if (settings.getMentionsFilter().equals(FilterType.Manual)) {
      tokensMentions = filterMention.filter(text, settings.getMentionsFilter(), false, settings.getLanguage());
    } else {
      tokensMentions = filterMention.filter(text, settings.getMentionsFilter(), settings.isUseHybridMentionDetection(), settings.getLanguage());
    }
    
    // Drop mentions below min occurrence count.
    if (settings.getMinMentionOccurrenceCount() > 1) {
      dropMentionsBelowOccurrenceCount(tokensMentions.second, settings.getMinMentionOccurrenceCount());
    }
    
    DocumentChunker chunker = settings.getDocumentChunker();
    PreparedInput preparedInput = 
        chunker.process(docId, tokensMentions.first, tokensMentions.second);
    
    return preparedInput;
  }

  public static void dropMentionsBelowOccurrenceCount(Mentions docMentions,
      int minMentionOccurrenceCount) {
    TObjectIntHashMap<String> mentionCounts = new TObjectIntHashMap<String>();
    for (Mention m : docMentions.getMentions()) {
      mentionCounts.adjustOrPutValue(m.getMention(), 1, 1);
    }
    List<Mention> mentionsToRemove = new ArrayList<Mention>();
    for (Mention m : docMentions.getMentions()) {
      if (mentionCounts.get(m.getMention()) < minMentionOccurrenceCount) {
        mentionsToRemove.add(m);
      }
    }
    for (Mention m : mentionsToRemove) {
      docMentions.remove(m);
    }
  }

}
