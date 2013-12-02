package mpi.aida.graph.similarity.importance;

import gnu.trove.map.hash.TIntDoubleHashMap;

import java.sql.SQLException;
import java.util.Collection;

import mpi.aida.access.DataAccess;
import mpi.aida.data.Entities;
import mpi.aida.data.Entity;


public class AidaEntityImportance extends EntityImportance {
  private TIntDoubleHashMap entitiesImportances;

  public AidaEntityImportance(Entities entities) throws SQLException {
    super(entities);
  }

  @Override
  protected void setupEntities(Entities e) throws SQLException {
    Collection<Integer> entitiesIdsCollection =  e.getUniqueIds();
    int[] entitiesIds = new int[entitiesIdsCollection.size()];
    int index = 0;
    for(int id: entitiesIdsCollection) {
      entitiesIds[index++] = id;
    }
    entitiesImportances = DataAccess.getEntitiesImportances(entitiesIds);
  }

  @Override
  public double getImportance(Entity entity) {
    return 1-entitiesImportances.get(entity.getId());
  }
  
  @Override
  public String toString() {
    return "AidaEntityImportance";
  }

}
