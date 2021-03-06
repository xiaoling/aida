package mpi.aida.data;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Entities implements Serializable, Iterable<Entity> {

  private static final long serialVersionUID = -5405018666688695438L;
  
  private boolean includesOokbeEntities;
    
  private HashMap<String, Integer> entitiesNames;

  private Set<Entity> entities = null;

  public Entities() {
    this.entitiesNames = new HashMap<String, Integer>();
    entities = new HashSet<Entity>();
  }

  public Entities(Set<Entity> entities) {
    this.entities = entities;
    this.entitiesNames = new HashMap<String, Integer>();
    for (Entity entity : entities) {
      this.entitiesNames.put(entity.getName(), entity.getId());
    }
  }

  public int getId(String entity) {
    return entitiesNames.get(entity);
  }

  public boolean contains(String entity) {
    return entitiesNames.containsKey(entity);
  }

  public Set<String> getUniqueNames() {
    return entitiesNames.keySet();
  }
  
  public Set<String> getUniqueNamesNormalizingNME() {
    Set<String> names = new HashSet<String>();
    
    for (Entity e : entities) {
      if (e.isOOKBentity()) {
        names.add(e.getNMEnormalizedName());
      } else {
        names.add(e.getName());
      }
    }
    
    return names;
  }
  
  public Collection<Integer> getUniqueIds() {
    return entitiesNames.values();
  }

  public Set<Entity> getEntities() {
    return entities;
  }

  /**
   * Should only be used for testing or if you know the exact id for each entity
   * @param entity
   * @param id
   */
  public void add(Entity entity) {
    entities.add(entity);
    entitiesNames.put(entity.getName(), entity.getId());
  }

  public void addAll(Entities entities) {
    this.entities.addAll(entities.entities);
    this.entitiesNames.putAll(entities.entitiesNames);
  }

  public int uniqueNameSize() {
    return entitiesNames.size();
  }

  public int size() {
    return entities.size();
  }

  @Override
  public Iterator<Entity> iterator() {
    return entities.iterator();
  }

  public boolean isEmpty() {
    return entities.isEmpty();
  }

  public boolean isIncludesOokbeEntities() {
    return includesOokbeEntities;
  }

  public void setIncludesOokbeEntities(boolean includesOokbeEntities) {
    this.includesOokbeEntities = includesOokbeEntities;
  }

  public static String getMentionNMEKey(String mentionName) {
    return mentionName+"-"+Entity.OOKBE;
  }

  public static boolean isOokbeName(String name) {
    return (name.endsWith("-"+Entity.OOKBE) || name.endsWith("---NME--"));
  }
  
  public static boolean isOokbEntity(String entity) {
    return (entity.equals(Entity.OOKBE) || entity.equals("--NME--"));
  }
  
  public static String getNameForOokbe(String nmeName) {
    String name = nmeName.replace("-" + Entity.OOKBE, "");
    return name;
  }
}
