package mpi.aida.access;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class DataAccessCacheTarget {
  
  private Logger logger_ = LoggerFactory.getLogger(DataAccessCacheTarget.class);
  
  protected int[] data_;
  
  public abstract String getId();

  protected abstract File getCacheFile();
  
  public int getData(int id) {
    assert id >= 0 : "id must not be negative.";
    assert id < data_.length : "id out of range.";
    return data_[id];
  }

  public void createAndLoadCache(boolean needsCacheCreation) throws FileNotFoundException, IOException {
    boolean requireReadFromDB = false;
    File cacheFile = getCacheFile();
    if (cacheFile.exists()) { 
      if (!needsCacheCreation) {  
        logger_.info("Loading " + getId() + " from cache.");
        loadFromDisk();
      } else {
        cacheFile.delete();
        requireReadFromDB = true;
      }
    } else {
      logger_.info(getId() + " cache file doesn't exist.");
      requireReadFromDB = true;
    }    
  
    if (requireReadFromDB) {
      logger_.info("Loading " + getId() + " from DB.");
      loadFromDb();
      logger_.info("Caching " + getId() + " to disk.");
      cacheToDisk(); 
    }
  }

  protected abstract void loadFromDb();
  
  private void loadFromDisk() throws IOException {
    File cacheFile = getCacheFile();
    DataInputStream in = new DataInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(cacheFile))));
    data_ = new int[in.readInt()];
    for (int i = 0; i < data_.length; ++i) {
      data_[i] = in.readInt();
    }
    in.close();    
  }

  private void cacheToDisk() throws IOException {
    File cacheFile = getCacheFile();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(cacheFile))));
    out.writeInt(data_.length);
    for (int exp : data_) {
      out.writeInt(exp);
    }
    out.flush();
    out.close();    
  }
}
