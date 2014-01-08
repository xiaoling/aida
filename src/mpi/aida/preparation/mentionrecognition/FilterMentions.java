package mpi.aida.preparation.mentionrecognition;

import java.io.Serializable;

import mpi.aida.config.settings.PreparationSettings;
import mpi.aida.config.settings.PreparationSettings.LANGUAGE;
import mpi.aida.data.Mentions;
import mpi.tokenizer.data.Tokenizer;
import mpi.tokenizer.data.TokenizerManager;
import mpi.tokenizer.data.Tokens;
import mpi.tools.javatools.datatypes.Pair;

public class FilterMentions implements Serializable { 
  private static final long serialVersionUID = 6260499966421708963L;

  private NamedEntityFilter namedEntityFilter = null;

  private ManualFilter manualFilter = null;

  private HybridFilter hybridFilter = null;
  
  public FilterMentions() {
    namedEntityFilter = new NamedEntityFilter();
    manualFilter = new ManualFilter();
    hybridFilter = new HybridFilter();
  }

  /** which type of tokens to get*/
  public static enum FilterType {
    STANFORD_NER, Manual, DICTIONARY, Manual_NER, ManualPOS;
  };
  
  public Pair<Tokens, Mentions> filter(String text, FilterType by, boolean isHybrid, PreparationSettings.LANGUAGE language) {
    Mentions mentions = null;
    Mentions manualMentions = null;
    Tokens tokens = null;
    
    //manual case handled separately
    if (by.equals(FilterType.Manual) || by.equals(FilterType.ManualPOS) || by.equals(FilterType.Manual_NER)) {
      Pair<Tokens, Mentions> tokensMentions = manualFilter.filter(text, by, language);
      return tokensMentions;
    }
    
    //if hybrid mention detection, use manual filter to get the tokens
    //and then pass them the appropriate ner 
    if(isHybrid) {
      Pair<Tokens, Mentions> tokensMentions = manualFilter.filter(text, by, language);
      manualMentions = tokensMentions.second();
      tokens = tokensMentions.first();
    } else { //otherwise tokenize normally
      Tokenizer.type type = buildTokenizerType(by, language);
      tokens = TokenizerManager.tokenize(text, type, false);
    }
    
    if(by.equals(FilterType.STANFORD_NER)) {
      mentions = namedEntityFilter.filter(tokens);
    }
    
    //if hybrid mention detection, merge both types mentions
    if(isHybrid) {
      mentions = hybridFilter.parse(manualMentions, mentions);
    }
   
    return new Pair<Tokens, Mentions>(tokens, mentions);
  }

  private mpi.tokenizer.data.Tokenizer.type buildTokenizerType(FilterType by, LANGUAGE language) {
    if (by == FilterType.STANFORD_NER) {
      switch (language) {
        case de:
          return Tokenizer.type.germanner;
        case en:
          return Tokenizer.type.ner;
        default:
          break;
      }
    } else if (by == FilterType.DICTIONARY) {
      switch (language) {
        case de:
          return Tokenizer.type.germantokens;
        case en:
          return Tokenizer.type.tokens;
        default:
          break;
      }
    }
    return Tokenizer.type.ner;
  }
}