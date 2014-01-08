package mpi.aida.data;

import mpi.tokenizer.data.Tokens;

public class PreparedInputChunk {
  
  private String chunkId_;
  
  private Tokens tokens_;

  /** Used by the local similarity methods in the disambiguation. It holds
   * the document tokens both as strings and converted to word ids. */ 
  private Context context_;
  
  private Mentions mentions_;
      
  public PreparedInputChunk(String chunkId) {
    chunkId_ = chunkId;
  }

  public PreparedInputChunk(String chunkId, Tokens tokens, Mentions mentions) {
    chunkId_ = chunkId;
    tokens_ = tokens;
    mentions_ = mentions;
    context_ = createContextFromTokens(tokens);
  }
  
  public Tokens getTokens() {
    return tokens_;
  }

  public void setTokens(Tokens tokens) {
    this.tokens_ = tokens;
    context_ = createContextFromTokens(tokens);
  }

  public Mentions getMentions() {
    return mentions_;
  }

  public void setMentions(Mentions mentions) {
    mentions_ = mentions;
  }
  
  public Context getContext() {
    return context_;
  }

  private Context createContextFromTokens(Tokens t) {
    return new Context(t);
  }

  public String getChunkId() {
    return chunkId_;
  }
  
  public String[] getMentionContext(Mention m, int windowSize) {
    int start = Math.max(0, m.getStartToken() - windowSize);
    int end = Math.min(tokens_.size(), m.getEndToken() + windowSize);
    StringBuilder before = new StringBuilder();
    for (int i = start; i < m.getStartToken(); ++i) {
      before.append(tokens_.getToken(i).getOriginal()).append(" ");
    }
    StringBuilder after = new StringBuilder();
    for (int i = m.getEndToken() + 1; i < end; ++i) {
      after.append(tokens_.getToken(i).getOriginal()).append(" ");
    }
    return new String[] { before.toString(), after.toString() };
  }
}
