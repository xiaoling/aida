package mpi.aida.preparation.documentchunking;

import java.util.ArrayList;
import java.util.List;

import mpi.aida.data.Mentions;
import mpi.aida.data.PreparedInput;
import mpi.aida.data.PreparedInputChunk;
import mpi.tokenizer.data.Tokens;

public class SingleChunkDocumentChunker extends DocumentChunker {

  @Override
  public PreparedInput process(String docId, Tokens tokens, Mentions mentions) {
     String chunkId = docId + "_singlechunk";
     PreparedInputChunk singleChunk = 
         new PreparedInputChunk(chunkId, tokens, mentions);
          
     List<PreparedInputChunk> chunks = new ArrayList<PreparedInputChunk>(1);
     chunks.add(singleChunk);
     PreparedInput preparedInput = new PreparedInput(docId, chunks);
     return preparedInput;
  }
}
