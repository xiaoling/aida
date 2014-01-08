package mpi.aida.access;

import java.io.File;


public class DataAccessKeywordCountCacheTarget extends DataAccessCacheTarget {

  public static final String ID = "KEYWORD_COUNT";
  
  @Override
  public String getId() {
    return ID;
  }

  @Override
  protected File getCacheFile() {
    return new File("aida-keyword_count.cache");
  }

  @Override
  protected void loadFromDb() {
    data_ = DataAccess.getAllKeywordDocumentFrequencies();
  }
}
