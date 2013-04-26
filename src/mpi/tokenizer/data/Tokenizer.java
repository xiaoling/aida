package mpi.tokenizer.data;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.WordTag;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.Morphology;
import edu.stanford.nlp.util.CoreMap;

public class Tokenizer {
  private static final Logger logger = 
      LoggerFactory.getLogger(Tokenizer.class);

  /** type of tokenizer */
  public static enum type {
    tokens, pos, ner, germanner, parse
  };

  public static final String TEXT = "TEXT";

  public static final String NUMBER = "NUMBER";

  public static final String REST = "REST";

  public static final String NPWORD = "NPWORD";

  private HashSet<Character> newline = null;

  private HashSet<Character> whitespace = null;

  private StanfordCoreNLP pipeline = null;

  private Pattern p = Pattern.compile(".*[\\n\\r]+.*[\\n\\r]+.*");
  
  // German Models
//  private final String GERMAN_NER_DEWAC = 
//      "resources/corenlp/germanmodels/ner/dewac_175m_600.crf.ser.gz";
  private final String GERMAN_NER_HGC = 
      "resources/corenlp/germanmodels/ner/hgc_175m_600.crf.ser.gz";
  private final String GERMAN_POS_HGC = 
      "resources/corenlp/germanmodels/pos/german-hgc.tagger";
//  private static final String GERMAN_PARSER =
//	  "resources/corenlp/germanmodels/parser/germanFactored.ser.gz";
//	  "resources/corenlp/germanmodels/parser/germanPCFG.ser.gz";
  
  public Tokenizer(Tokenizer.type type) {
    Properties props = new Properties();

    if (type.equals(Tokenizer.type.tokens)) {
      props.put("annotators", "tokenize, ssplit");
    } else if (type.equals(Tokenizer.type.ner)) {
      props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
    } else if (type.equals(Tokenizer.type.pos)) {
      props.put("annotators", "tokenize, ssplit, pos, lemma");
      pipeline = new StanfordCoreNLP(props, true);
    } else if (type.equals(Tokenizer.type.parse)) {
      props.put("annotators", "tokenize, ssplit, parse, pos, lemma");
    } 
    else if (type.equals(Tokenizer.type.germanner)) {
      props.put("pos.model", GERMAN_POS_HGC);
      props.put("ner.model", GERMAN_NER_HGC);
      props.put("ner.useSUTime", "false"); //false not for english
      props.put("ner.applyNumericClassifiers", "false"); //false not for english
    }
    pipeline = new StanfordCoreNLP(props, true);
    init();
  }

  public Tokens parse(String text, String docId, boolean lemmatize) {
    Tokens tokens = new Tokens(docId);
    parse(tokens, text, lemmatize);
    return tokens;
  }

  private void parse(Tokens tokens, String text, boolean lemmatize) {
    try {
      if (text.trim().length() == 0) {
        return;
      }
      Annotation document = new Annotation(text);
      pipeline.annotate(document);
      List<CoreMap> sentences = document.get(SentencesAnnotation.class);
      Wrapper wrapper = new Wrapper();
      Morphology morphology = null;
      if (lemmatize) {
        morphology = new Morphology();
      }
      for (CoreMap sentence : sentences) {
        wrapper.sentence(wrapper.sentence() + 1);
        for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
          wrapper.setStandfordId(wrapper.getStandfordId() + 1);
          String ne = token.get(NamedEntityTagAnnotation.class);
          String pos = token.get(PartOfSpeechAnnotation.class);
          int start = token.get(CharacterOffsetBeginAnnotation.class);
          int end = token.get(CharacterOffsetEndAnnotation.class);
          
          
          allocateBranch(start, end, ne, pos, tokens, wrapper, text, lemmatize, morphology);
        }
      }
      if (tokens.size() > 0) {
        int lastCharIndex = tokens.getToken(tokens.size() - 1).getEndIndex();
        if (lastCharIndex < text.length()) {
          tokens.setOriginalEnd(text.substring(lastCharIndex));
        }
      } else {
        tokens.setOriginalEnd(text);
      }
    } catch (Exception e) {
      logger.warn("Parser failed: " + e.getLocalizedMessage());
      tokens.logProblem("Parser failed: " + e);
    }
  }

  private void allocateBranch(int beginPosition, int endPosition, String ne, String pos, Tokens tokens, Wrapper wrapper, String text, boolean lemmatize, Morphology morphology) {
    String word = text.substring(beginPosition, endPosition);
    int lastTokenId = tokens.size() - 1;
    if (lastTokenId >= 0) {
      int spacesStartIndex = tokens.getToken(lastTokenId).getEndIndex();
      String spaces = text.substring(spacesStartIndex, beginPosition);
      tokens.getToken(lastTokenId).setOriginalEnd(spaces);
      if (p.matcher(spaces).matches()) {
        wrapper.setParagraph(wrapper.paragraph() + 1);
      }
    } else {
      tokens.setOriginalStart(text.substring(0, beginPosition));
    }
    Token token = new Token(wrapper.getStandfordId(), word, beginPosition, endPosition, wrapper.paragraph());
    token.setSentence(wrapper.sentence());
    token.setNE(ne);
    if (lemmatize) {
      String subword = token.getOriginal();
      WordTag wordtag = new WordTag(subword, pos);
      token.setLemma(morphology.lemmatize(wordtag).lemma());
    }    
    token.setPOS(pos);
    
    tokens.addToken(token);
    
  }

  private void init() {
    newline = new HashSet<Character>();
    newline.add(new Character('\n'));
    newline.add(new Character('\r'));
    whitespace = new HashSet<Character>();
    whitespace.add(' ');
    whitespace.add('\t');
  }
}
