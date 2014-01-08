package mpi.aida.preparation.documentchunking;

import mpi.aida.data.Mentions;
import mpi.aida.data.PreparedInput;
import mpi.tokenizer.data.Tokens;


public abstract class DocumentChunker {
  public abstract PreparedInput process(String docId, Tokens tokens, Mentions mentions);
}
