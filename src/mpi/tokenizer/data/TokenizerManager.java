package mpi.tokenizer.data;

import mpi.tokenizer.data.Tokenizer.type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenizerManager {
  private Logger logger_ = LoggerFactory.getLogger(TokenizerManager.class);
  
  private static TokenizerManager manager = null;

  public static void init() {
    TokenizerManager.getInstance();
  }

  public static Tokens parse(String docId, String text, Tokenizer.type type, boolean lemmatize) {
    return TokenizerManager.getInstance().parseText(docId, text, type, lemmatize);
  }

  private static TokenizerManager getInstance() {
    if (manager == null) {
      manager = new TokenizerManager();
    }
    return manager;
  }

  private Tokenizer tokenizer = null;
  
  private Tokenizer tokenizerGerman = null;
  
  private Tokenizer tokenizerNER = null;
  
  private Tokenizer tokenizerGermanNER = null;

  private Tokenizer tokenizerPOS = null;
  
  private Tokenizer tokenizerGermanPOS = null;

  private Tokenizer tokenizerParse = null;

  private TokenizerManager() {

  }

  private Tokens parseText(String docId, String text, Tokenizer.type type, boolean lemmatize) {
    synchronized (manager) {      
      loadTokenizerForType(type);
    }
    switch (type) {
      case tokens:
        return tokenize(docId, text, lemmatize);
      case germantokens:
        return tokenizeGerman(docId, text, lemmatize);
      case pos:
        return tokenizePOS(docId, text, lemmatize);
      case germanpos:
        return tokenizeGermanPOS(docId, text, lemmatize);
      case ner:
        return tokenizeNER(docId, text, lemmatize);
      case germanner:
        return tokenizeGermanNER(docId, text, lemmatize);
      case parse:
        return tokenizePARSE(docId, text, lemmatize);
      default:
        return null;
    }
  }

  private void loadTokenizerForType(type type) {
    switch (type) {
      case tokens:
        if (tokenizer == null) {
          logger_.info("Loading Tokenizer (ssplit, tokenize)");
          tokenizer = new Tokenizer(Tokenizer.type.tokens);
        }
        break;
      case germantokens:
        if (tokenizerGerman == null) {
          logger_.info("Loading Tokenizer (ssplit, germantokenize)");
          tokenizerGerman = new Tokenizer(Tokenizer.type.germantokens);
        }
        break;
      case pos:
        if (tokenizerPOS == null) {
          logger_.info("Loading Tokenizer (ssplit, tokenize, pos)");
          tokenizerPOS = new Tokenizer(Tokenizer.type.pos);
        }
        break;
      case germanpos:
        if (tokenizerGermanPOS == null) {
          logger_.info("Loading Tokenizer (ssplit, tokenize, germanpos)");
          tokenizerGermanPOS = new Tokenizer(Tokenizer.type.germanpos);
        }
        break;
      case ner:
        if (tokenizerNER == null) {
          logger_.info("Loading Tokenizer (ssplit, tokenize, pos, ner)");
          tokenizerNER = new Tokenizer(Tokenizer.type.ner);
        }
        break;
      case germanner:
        if (tokenizerGermanNER == null) {
          logger_.info("Loading Tokenizer (ssplit, tokenize, germanpos, germanner)");
          tokenizerGermanNER = new Tokenizer(Tokenizer.type.germanner);
        }
        break;
      case parse:
        if (tokenizerParse == null) {
          logger_.info("Loading Tokenizer (ssplit, tokenize, pos, ner, parse)");
          tokenizerParse = new Tokenizer(Tokenizer.type.parse);
        }
        break;
      default:
        // Nothing.
    }    
  }

  private Tokens tokenize(String docId, String text, boolean lemmatize) {
    return tokenizer.parse(text, docId, lemmatize);
  }
  
  private Tokens tokenizeGerman(String docId, String text, boolean lemmatize) {
    return tokenizerGerman.parse(text, docId, lemmatize);
  }

  private Tokens tokenizeNER(String docId, String text, boolean lemmatize) {
    return tokenizerNER.parse(text, docId, lemmatize);
  }
  
  private Tokens tokenizeGermanNER(String docId, String text, boolean lemmatize) {
    return tokenizerGermanNER.parse(text, docId, lemmatize);
  }

  private Tokens tokenizePOS(String docId, String text, boolean lemmatize) {
    return tokenizerPOS.parse(text, docId, lemmatize);
  }
  
  private Tokens tokenizeGermanPOS(String docId, String text, boolean lemmatize) {
    return tokenizerGermanPOS.parse(text, docId, lemmatize);
  }

  private Tokens tokenizePARSE(String docId, String text, boolean lemmatize) {
    return tokenizerParse.parse(text, docId, lemmatize);
  }

}
