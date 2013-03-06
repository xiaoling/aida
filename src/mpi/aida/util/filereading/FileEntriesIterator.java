package mpi.aida.util.filereading;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates over a file, line by line. It is not thread-safe. 
 */
public class FileEntriesIterator implements Iterator<String> {
  private Logger logger = LoggerFactory.getLogger(FileEntriesIterator.class);
  
  private BufferedReader reader_;
  private int maxLineLengthForMark_;
  private int progressAfterLines_ = 100000;
  private long fileByteLength_;
  private long currentByteLength_ = 0;
  private long currentFileLine_ = 0;
  private NumberFormat percentFormat = NumberFormat.getPercentInstance();
  private String lastReadLineInHasNext;
  private boolean done = false;
  
  public FileEntriesIterator(File tsvFile, int progressAfterLines) 
      throws UnsupportedEncodingException, FileNotFoundException {
    this(tsvFile, progressAfterLines, 10000);
  }
  
  public FileEntriesIterator(
      File tsvFile, int progressAfterLines, int maxLineLength) 
      throws UnsupportedEncodingException, FileNotFoundException {
    reader_ = 
        new BufferedReader(new InputStreamReader(
            new FileInputStream(tsvFile), "UTF-8"));
    progressAfterLines_ = progressAfterLines;
    fileByteLength_ = tsvFile.length();
    maxLineLengthForMark_ = maxLineLength;
  }
  
  @Override
  public boolean hasNext() {
    synchronized(reader_) {
      if (done) { return false; }
      try {
        reader_.mark(maxLineLengthForMark_);
        lastReadLineInHasNext = reader_.readLine();
        boolean hasNext = (lastReadLineInHasNext != null);
        if (hasNext) {
          reader_.reset();
        } else {
          // Close the reader, not used anymore.
          reader_.close();
          done = true;
        }
        return hasNext;
      } catch (IOException e) {
        logger.error("IOException: " + e.getLocalizedMessage() + ". " +
        		"This might happen when lines are too long, exceeding " + 
            maxLineLengthForMark_ +	" characters. Try increasing it.");
        try {
          reader_.close();
        } catch (IOException e2) {
          logger.error("Failed to close reader: " + e2.getLocalizedMessage());
        }
        return false;
      }
    }
  }

  @Override
  public String next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    } else {
      String line;
      try {
        line = reader_.readLine();
        
        if (progressAfterLines_ != 0) {
          currentByteLength_ += line.length();
        
          // Post progress every few lines.
          if (++currentFileLine_ % progressAfterLines_ == 0) {
            float progress = 
                (float) currentByteLength_ / (float) fileByteLength_; 
            logger.info("Read " + percentFormat.format(progress) + " (" + 
                        currentFileLine_ + " lines)");
          }
        }
        return line;
      } catch (IOException e) {
        throw new NoSuchElementException(e.getLocalizedMessage());
      }
    }     
  }
  
  public String peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    } else {
      return lastReadLineInHasNext;
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}