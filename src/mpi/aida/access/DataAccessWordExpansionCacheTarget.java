package mpi.aida.access;

import java.io.File;


public class DataAccessWordExpansionCacheTarget extends DataAccessCacheTarget {

  public static final String ID = "WORD_EXPANSION";
  
  @Override
  public String getId() {
    return ID;
  }

  @Override
  protected File getCacheFile() {
    return new File("aida-word_expansions.cache");
  }

  @Override
  protected void loadFromDb() {
    data_ = DataAccess.getAllWordExpansions();    
  }
}
