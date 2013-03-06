package mpi.aida.util;

import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import mpi.aida.access.DataAccess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopWord {
  private static final Logger logger = 
      LoggerFactory.getLogger(StopWord.class);

  public static String pathStopWords = "tokens/stopwords6.txt";

  public static String pathSymbols = "tokens/symbols.txt";

  private static StopWord stopwords = null;

  private static StopWord getInstance() {
    if (stopwords == null) {
      stopwords = new StopWord();
    }
    return stopwords;
  }

  private HashSet<String> words = null;
  private TIntHashSet wordIds = null;

  private HashSet<Character> symbols = null;

  private StopWord() {
    words = new HashSet<String>();
    symbols = new HashSet<Character>();
    load();
  }

  private void load() {
    try{
      List<String> stopwords = ClassPathUtils.getContent(pathStopWords);
      for(String stopword: stopwords){
        words.add(stopword.trim());
      }
    } catch (IOException e){
      logger.error(e.getMessage());
    }
    try{
      List<String> str = ClassPathUtils.getContent(pathSymbols);
      for(String word: str){
        word = word.trim();
        words.add(word);
        symbols.add(word.charAt(0));
      }
    } catch (IOException e){
      logger.error(e.getMessage());
    }
    wordIds = new TIntHashSet(DataAccess.getIdsForWords(words).values());
  }

  private boolean isStopWord(String word) {
    return words.contains(word);
  }
  
  private boolean isStopWord(int wordId) {
    return wordIds.contains(wordId);
  }

  private boolean isSymbol(char word) {
    return symbols.contains(word);
  }

  public static boolean is(String word) {
    return StopWord.getInstance().isStopWord(word.toLowerCase());
  }

  public static boolean is(int word) {
    return StopWord.getInstance().isStopWord(word);
  }

  public static boolean symbol(char word) {
    return StopWord.getInstance().isSymbol(word);
  }

  public static boolean firstCharSymbol(String word) {
    if (word == null || word.length() == 0) {
      return false;
    }
    return StopWord.getInstance().isSymbol(word.charAt(0));
  }

  public static void main2(String[] args) {
    HashSet<String> words = new HashSet<String>();
    File f = new File("./conf/stopwords6.txt");
    if (f.exists()) {
      try {
        FileReader fileReader = new FileReader(f);
        BufferedReader reader = new BufferedReader(fileReader);
        String word = reader.readLine();
        while (word != null) {
          if (!word.trim().equals("")) {
            words.add(word);
          }
          word = reader.readLine();
        }
        reader.close();
        fileReader.close();
      } catch (Exception e) {
        logger.error(e.getLocalizedMessage());
      }
    } else {
      logger.error("Path does not exists " + "./conf/stopwords2.txt");
    }
    f = new File("./conf/stopwords.txt");
    if (f.exists()) {
      try {
        FileReader fileReader = new FileReader(f);
        BufferedReader reader = new BufferedReader(fileReader);
        String word = reader.readLine();
        while (word != null) {
          if (!word.trim().equals("")) {
            words.add(word);
          }
          word = reader.readLine();
        }
        reader.close();
        fileReader.close();
      } catch (Exception e) {
        logger.error(e.getLocalizedMessage());
      }
    } else {
      logger.error("Path does not exists " + "./conf/stopwords.txt");
    }
    List<String> sortedWords = new LinkedList<String>();
    Iterator<String> iter = words.iterator();
    while (iter.hasNext()) {
      sortedWords.add(iter.next());
    }
    System.out.println(words.size());
    f = new File("./conf/stopwords7.txt");
    Collections.sort(sortedWords);
    try {
      FileWriter writer = new FileWriter(f);
      iter = sortedWords.iterator();
      if (iter.hasNext()) {
        writer.write(iter.next());
      }
      while (iter.hasNext()) {
        writer.write("\n" + iter.next());
      }
      writer.flush();
      writer.close();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    }
    System.out.println("done");
  }

  public static void main(String[] args) {
    String test = "!";
    System.out.println(StopWord.symbol(test.charAt(0)));
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
