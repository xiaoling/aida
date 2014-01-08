package mpi.aida.access;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mpi.aida.access.DataAccessSQLCache.EntityKeyphraseData;
import mpi.tools.javatools.datatypes.Pair;


public class KeyphrasesCache 
implements Iterable<Pair<Integer, EntityKeyphraseData>> {
  
  
  private List<Pair<Integer, EntityKeyphraseData>> entries =
      new ArrayList<Pair<Integer, EntityKeyphraseData>>();
  

  public void add(int entityId, EntityKeyphraseData ekd) {
    entries.add(new Pair<Integer, EntityKeyphraseData>(entityId, ekd));
  }

  public void addAll(int eId, List<EntityKeyphraseData> ekds) {
    for (EntityKeyphraseData ekd : ekds) {
      add(eId, ekd);
    }
  }
  
  @Override
  public Iterator<Pair<Integer, EntityKeyphraseData>> iterator() {
    return entries.iterator();
  }
}
