package mpi.aida.util.filereading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates over a tsv file, returns each line as String[] split on <TAB>.
 */
public class TsvEntries implements Iterable<String[]> {

  private Logger logger = LoggerFactory.getLogger(TsvEntries.class);
  
  private File file_;
  private int progressAfterLines_;
  
  public TsvEntries(String filename) {
    file_ = new File(filename);
  }
  
  public TsvEntries(File file) {
    file_ = file;
  }
  
  /**
   * Read file line by line, posting progress every progressAfterLines
   * lines. Default is after 100000.
   * 
   * @param file  File to read.
   * @param progressAfterLines  Gap between progress posting. 
   * Set to 0 to disable.
   */
  public TsvEntries(File file, int progressAfterLines) {
    file_ = file;
    progressAfterLines_ = progressAfterLines;
  }
  
  @Override
  public Iterator<String[]> iterator() {
    try {
      return new TsvEntriesIterator(file_, progressAfterLines_);
    } catch (UnsupportedEncodingException e) {
      logger.error("Unsupported Encoding: " + e.getLocalizedMessage());
      return null;
    } catch (FileNotFoundException e) {
      logger.error("File not found: " + e.getLocalizedMessage());
      return null;
    }
  }
}
