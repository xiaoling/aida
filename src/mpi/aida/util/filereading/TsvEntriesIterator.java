package mpi.aida.util.filereading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.NoSuchElementException;


public class TsvEntriesIterator implements Iterator<String[]> {

  private FileEntriesIterator internalIterator_;
  
  public TsvEntriesIterator(File tsvFile, int progressAfterLines) 
      throws UnsupportedEncodingException, FileNotFoundException {
    internalIterator_ = 
        new FileEntriesIterator(tsvFile, progressAfterLines, 100000);
  }
  
  @Override
  public boolean hasNext() {
    return internalIterator_.hasNext();
  }

  @Override
  public String[] next() {
    return internalIterator_.next().split("\t");
  }

  @Override
  public void remove() {
    throw new NoSuchElementException();
  }
}
