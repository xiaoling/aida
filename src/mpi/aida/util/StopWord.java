package mpi.aida.util;

import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpi.aida.access.DataAccess;
import mpi.aida.config.AidaConfig;
import mpi.aida.config.settings.PreparationSettings;
import mpi.aida.config.settings.PreparationSettings.LANGUAGE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopWord {
  private static final Logger logger = 
      LoggerFactory.getLogger(StopWord.class);

  
  private Map<LANGUAGE,String> pathStopWords = null;
  private Map<LANGUAGE,String> pathSymbols = null;

  private static StopWord stopwords = null;

  //This method is left synchronized intentionally for performance reasons,
  //It doesn't cause problems because the data don't change, in the worst case, 
  //this would be instantiated multiple times
  private static StopWord getInstance() {
    if (stopwords == null) {
      LANGUAGE language = AidaConfig.getLanguage();
      stopwords = new StopWord(language);
    }
    return stopwords;
  }

  private Set<String> words = null;
  private TIntHashSet wordIds = null;

  private Set<String> symbols = null;
  private TIntHashSet symbolIds = null;

  private StopWord(LANGUAGE language) {
    words = new HashSet<String>();
    symbols = new HashSet<String>();
    init(language);
    load(language);
  }
  
  private void init(LANGUAGE language) {
    pathStopWords = new HashMap<PreparationSettings.LANGUAGE, String>();
    pathStopWords.put(LANGUAGE.en, "tokens/stopwords6.txt");
    pathStopWords.put(LANGUAGE.de, "tokens/stopwords-german.txt");
    
    pathSymbols = new HashMap<PreparationSettings.LANGUAGE, String>();
    pathSymbols.put(LANGUAGE.en, "tokens/symbols.txt");
    pathSymbols.put(LANGUAGE.de, "tokens/symbols.txt");
    
    
  }

  private void load(LANGUAGE language) {
    
    try{
      List<String> stopwords = ClassPathUtils.getContent(pathStopWords.get(language));
      for(String stopword: stopwords){
        words.add(stopword.trim());
      }
    } catch (IOException e){
      logger.error(e.getMessage());
    }
    try{
      List<String> str = ClassPathUtils.getContent(pathSymbols.get(language));
      for(String word: str){
        word = word.trim();
        words.add(word);
        symbols.add(word);
      }
    } catch (IOException e){
      logger.error(e.getMessage());
    }
    wordIds = new TIntHashSet(DataAccess.getIdsForWords(words).values());
    symbolIds = new TIntHashSet(DataAccess.getIdsForWords(symbols).values());
  }

  private boolean isStopWord(String word) {
    return words.contains(word);
  }
  
  private boolean isStopWord(int wordId) {
    return wordIds.contains(wordId);
  }

  private boolean isSymbol(String word) {
    return symbols.contains(word);
  }
  
  private boolean isSymbol(int word) {
    return symbolIds.contains(word);
  }

  public static boolean isStopwordOrSymbol(String word) {
    StopWord sw = StopWord.getInstance();
    boolean is = sw.isStopWord(word.toLowerCase()) || sw.isSymbol(word);
    return is;
  }

  public static boolean isStopwordOrSymbol(int word) {
    StopWord sw = StopWord.getInstance();
    boolean is = sw.isStopWord(word) || sw.isSymbol(word);
    return is;
  }

  public static boolean symbol(char word) {
    return StopWord.getInstance().isSymbol(word);
  }

  public static void main(String[] args) {
    String test = "ueber";
    System.out.println(StopWord.isStopwordOrSymbol(test));
  }

  public static boolean isOnlySymbols(String value) {
    for (int i = 0; i < value.length(); i++) {
      if (!StopWord.symbol(value.charAt(i))) {
        return false;
      }
    }
    return true;
  }
}
