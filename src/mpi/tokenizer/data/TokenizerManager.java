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

  public static Tokens tokenize(String text, Tokenizer.type type, boolean lemmatize) {
    return TokenizerManager.getInstance().parseText(text, type, lemmatize);
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

  private Tokens parseText(String text, Tokenizer.type type, boolean lemmatize) {
    synchronized (manager) {      
      loadTokenizerForType(type);
    }
    switch (type) {
      case tokens:
        return tokenize(text, lemmatize);
      case germantokens:
        return tokenizeGerman(text, lemmatize);
      case pos:
        return tokenizePOS(text, lemmatize);
      case germanpos:
        return tokenizeGermanPOS(text, lemmatize);
      case ner:
        return tokenizeNER(text, lemmatize);
      case germanner:
        return tokenizeGermanNER(text, lemmatize);
      case parse:
        return tokenizePARSE(text, lemmatize);
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

  private Tokens tokenize(String text, boolean lemmatize) {
    return tokenizer.parse(text, lemmatize);
  }
  
  private Tokens tokenizeGerman(String text, boolean lemmatize) {
    return tokenizerGerman.parse(text, lemmatize);
  }

  private Tokens tokenizeNER(String text, boolean lemmatize) {
    return tokenizerNER.parse(text, lemmatize);
  }
  
  private Tokens tokenizeGermanNER(String text, boolean lemmatize) {
    return tokenizerGermanNER.parse(text, lemmatize);
  }

  private Tokens tokenizePOS(String text, boolean lemmatize) {
    return tokenizerPOS.parse(text, lemmatize);
  }
  
  private Tokens tokenizeGermanPOS(String text, boolean lemmatize) {
    return tokenizerGermanPOS.parse(text, lemmatize);
  }

  private Tokens tokenizePARSE(String text, boolean lemmatize) {
    return tokenizerParse.parse(text, lemmatize);
  }

}
