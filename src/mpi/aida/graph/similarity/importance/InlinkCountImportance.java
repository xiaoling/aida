package mpi.aida.graph.similarity.importance;

import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.sql.SQLException;

import javax.xml.crypto.Data;

import mpi.aida.access.DataAccess;
import mpi.aida.data.Entities;
import mpi.aida.data.Entity;
import mpi.aida.util.YagoUtil;
import mpi.database.DBConnection;

/**
 * Measures the importance of an entity by the number of
 * incoming links in Wikipedia/YAGO
 * 
 *
 */
public class InlinkCountImportance extends EntityImportance {

  private TIntDoubleHashMap inlinkImportance;

  DBConnection con;

  public InlinkCountImportance(Entities entities) throws SQLException {
    super(entities);
  }

  @Override
  protected void setupEntities(Entities e) throws SQLException {
    TIntObjectHashMap<int[]> neighbors = DataAccess.getInlinkNeighbors(e);
    double collectionSize = (double) DataAccess.getCollectionSize();
    for (int eId : e.getUniqueIds()) {
      double importance = 
          (double) neighbors.get(eId).length 
          / (double) collectionSize;
      inlinkImportance.put(eId, importance);
    }
  }

  @Override
  public double getImportance(Entity entity) {
    return inlinkImportance.get(entity.getId());
  }

  public String toString() {
    return "InlinkCountImportance";
  }
}
