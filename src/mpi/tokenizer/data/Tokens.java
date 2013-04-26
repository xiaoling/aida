package mpi.tokenizer.data;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Tokens implements Iterable<Token>, Serializable {

  private static final long serialVersionUID = 8015832523759790735L;

  private String docId = null;

  private List<Token> tokens = null;

  private List<String> problems = null;

  private List<String> standfordTokens = null;

  private String originalStart = "";

  private String originalEnd = "";

  public Tokens() {
    tokens = new LinkedList<Token>();
    problems = new LinkedList<String>();
  }

  public Tokens(String docId) {
    this.docId = docId;
    tokens = new LinkedList<Token>();
    problems = new LinkedList<String>();
  }

  public String getDocId() {
    return docId;
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

  public void logProblem(String problem) {
    problems.add(problem);
  }

  public boolean hasProblem() {
    return problems.size() > 0;
  }

  public List<String> getProblems() {
    return problems;
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

  private List<String> createStanfordList() {
    int iterPosition = -1;
    List<String> stokens = new LinkedList<String>();
    String nextWord = null;
    while (iterPosition + 1 < tokens.size()) {
      iterPosition++;
      int start = iterPosition;
      Token t = tokens.get(iterPosition);
      int sId = t.getStandfordId();
      while (iterPosition + 1 < tokens.size() && tokens.get(iterPosition + 1).getStandfordId() == sId) {
        iterPosition++;
      }
      nextWord = toText(start, iterPosition);
      stokens.add(nextWord);
    }
    return stokens;
  }

  public List<String> getStanfordTokens() {
    if (standfordTokens == null) {
      standfordTokens = createStanfordList();
    }
    return standfordTokens;
  }

}
