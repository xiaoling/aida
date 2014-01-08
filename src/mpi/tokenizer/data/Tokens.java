package mpi.tokenizer.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import mpi.tools.javatools.datatypes.Pair;

public class Tokens implements Iterable<Token>, Serializable {

  private static final long serialVersionUID = 8015832523759790735L;

  private List<Token> tokens = null;

  private String originalStart = "";

  private String originalEnd = "";

  private List<Pair<Integer, Integer>> sentencesBorders = null;

  public Tokens() {
    tokens = new LinkedList<Token>();
  }

  public Token getToken(int position) {
    return tokens.get(position);
  }

  public List<Token> getTokens() {
    return tokens;
  }

  public int size() {
    return tokens.size();
  }

  public void addToken(Token token) {
    tokens.add(token);
  }

  public String toString() {
    StringBuffer sb = new StringBuffer(200);
    for (int i = 0; i < tokens.size(); i++) {
      sb.append(tokens.get(i).toString()).append('\n');
    }
    return sb.toString();
  }

  public String toText(int startToken, int endToken) {
    StringBuffer sb = new StringBuffer(200);
    for (int i = startToken; i <= endToken; i++) {
      sb.append(tokens.get(i).getOriginal());
      if (i + 1 <= endToken) {
        sb.append(tokens.get(i).getOriginalEnd());
      }
    }
    return sb.toString();
  }

  public String toTextLemmatized(int startToken, int endToken) {
    StringBuffer sb = new StringBuffer(200);
    for (int i = startToken; i <= endToken; i++) {
      sb.append(tokens.get(i).getLemma());
      if (i + 1 <= endToken) {
        sb.append(tokens.get(i).getOriginalEnd());
      }
    }
    return sb.toString();
  }

  public String toText() {
    StringBuffer sb = new StringBuffer(200);
    sb.append(originalStart);
    for (int i = 0; i < tokens.size(); i++) {
      sb.append(tokens.get(i).getOriginal());
      sb.append(tokens.get(i).getOriginalEnd());
    }
    sb.append(originalEnd);
    return sb.toString();
  }

  /**
   * Use the method for evaluation with combination tokenizer.type.*conll.
   * (by converting tokens read from CoNLL dataset into text which).
   * @return
   */
  public String toTextCoNLL() {
    StringBuffer sb = new StringBuffer(200);
    sb.append(originalStart);
    for (int i = 0; i < tokens.size(); i++) {
      sb.append(tokens.get(i).getOriginal());
      sb.append(" ");
    }
    sb.append(originalEnd);
    return sb.toString();
  }

  public void setOriginalStart(String text) {
    originalStart = text;
  }

  public void setOriginalEnd(String text) {
    originalEnd = text;
  }

  public String getOriginalStart() {
    return originalStart;
  }

  public String getOriginalEnd() {
    return originalEnd;
  }

  @Override
  public Iterator<Token> iterator() {
    return tokens.iterator();
  }

  public List<List<Token>> getSentenceTokens() {
    List<List<Token>> sentences = new ArrayList<List<Token>>();
    int currentSentenceId = -1;
    List<Token> currentSentence = null;
    for (Token t : this) {
      if (t.getSentence() != currentSentenceId) {
        List<Token> sentence = new ArrayList<Token>();
        sentences.add(sentence);
        currentSentence = sentence;
        ++currentSentenceId;
      }
      currentSentence.add(t);
    }
    return sentences;
  }
  
   /**
   * Copies tokens from interval.
   * 
   * @param from inclusive
   * @param to exclusive
   * @return range of original tokens
   */
  public Tokens copyOfRange(int from, int to) {
    int newLength = to - from;
    if (newLength < 0) throw new IllegalArgumentException(from + " > " + to);

    Tokens copy = new Tokens();

    for (Token token : tokens.subList(from, to))
      copy.addToken(token);

    return copy;
  }

  /**
   * Returns list of sentences.
   * 
   * @return
   */
  public List<Tokens> getSentences() {
    List<Tokens> sentences = new LinkedList<Tokens>();

    int from = 0;
    int to = 0;
    for (int i = 0; i < tokens.size(); ++i) {
      to = i;
      if (tokens.get(from).getSentence() != tokens.get(to).getSentence()) {
        sentences.add(copyOfRange(from, to));
        from = to;
        continue;
      }
      if (to == tokens.size() - 1) {
        sentences.add(copyOfRange(from, to + 1));
        break;
      }
    }

    return sentences;
  }

  /**
   * Returns list of borders (left token number of a sentence, right (exclusive) token number of a sentence).
   * 
   * @return
   */
  public List<Pair<Integer, Integer>> getSentencesBorders() {
    if (sentencesBorders == null) {
      sentencesBorders = new LinkedList<Pair<Integer, Integer>>();
      int from = 0;
      int to = 0;
      for (int i = 0; i < tokens.size(); ++i) {
        to = i;
        if (tokens.get(from).getSentence() != tokens.get(to).getSentence()) {
          sentencesBorders.add(new Pair<Integer, Integer>(from, to));
          from = to;
          continue;
        }
        if (to == tokens.size() - 1) {
          sentencesBorders.add(new Pair<Integer, Integer>(from, to + 1));
          break;
        }
      }
    }
    return sentencesBorders;
  }
}
