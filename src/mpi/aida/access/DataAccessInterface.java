package mpi.aida.access;

import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpi.aida.data.Entities;
import mpi.aida.data.Entity;
import mpi.aida.data.EntityMetaData;
import mpi.aida.data.Keyphrases;
import mpi.aida.util.YagoUtil.Gender;

public interface DataAccessInterface {

  public DataAccess.type getAccessType();

  public Entities getEntitiesForMention(String mention, double maxEntityRank);

  public int[] getInlinkNeighbors(Entity e);

  public TIntObjectHashMap<int[]> getInlinkNeighbors(Entities entities);
 
  public Map<String, Gender> getGenderForEntities(Entities entities);

  public Map<String, List<String>> getTypes(Set<String> entities);

  public List<String> getTypes(String Entity);

  public TIntIntHashMap getKeyphraseDocumentFrequencies(TIntHashSet keyphrases);

  public List<String> getParentTypes(String queryType);

  public String getKeyphraseSource(String entityName, String keyphrase);
  
  public Map<String, List<String>> getKeyphraseEntities(Set<String> keyphrases);

  public Map<Entity, int[]> getEntityLSHSignatures(Entities entities);

  public Map<Entity, int[]> getEntityLSHSignatures(Entities entities, String table);

  public String getFamilyName(String entity);

  public String getGivenName(String entity);

  public TIntDoubleHashMap getEntityPriors(String mention);

  public void getEntityKeyphraseTokens(
      Entities entities, String keyphraseSourceExclusion,
      TIntObjectHashMap<int[]> entityKeyphrases,
      TIntObjectHashMap<int[]> kpTokens);

  public TIntIntHashMap getKeywordDocumentFrequencies(TIntHashSet keywords);

  public TIntIntHashMap getEntitySuperdocSize(Entities entities);

  public TIntObjectHashMap<TIntIntHashMap> getEntityKeywordIntersectionCount(Entities entities);

  public TIntObjectHashMap<String> getYagoEntityIdsForIds(int[] ids);

  public TObjectIntHashMap<String> getIdsForYagoEntityIds(Collection<String> entityIds);

  public TIntObjectHashMap<String> getWordsForIds(int[] keywordIds);

  public TObjectIntHashMap<String> getIdsForWords(Collection<String> keywords);

  public TObjectIntHashMap<String> getAllEntityIds();

  public Entities getAllEntities();

  public int[] getAllWordExpansions();

  public boolean checkEntityNameExists(String entity);
  
  public boolean isYagoEntity(Entity entity);

  public Keyphrases getEntityKeyphrases(Entities entities,
      String keyphraseSourceExclusion);
  
  public Keyphrases getEntityKeyphrases(
      Entities entities, String keyphraseSourceExclusion,
      double minKeyphraseWeight, int maxEntityKeyphraseCount);

  public TIntObjectHashMap<int[]> getAllInlinks();

  public TObjectIntHashMap<String> getAllWordIds();

  public int getCollectionSize();

  public int getWordExpansion(int wordId);

  public TIntObjectHashMap<String> getTypeNamesForIds(int[] ids);

  public TObjectIntHashMap<String> getIdsForTypeNames(Collection<String> typeNames);
  
  public TIntObjectHashMap<int[]> getTypesIdsForEntitiesIds(int[] entitiesIds);
  
  public TIntObjectHashMap<int[]> getEntitiesIdsForTypesIds(int[] typesIds);
  
  public TIntObjectHashMap<int[]> getTypesIdsForEntities(Entities entities);

  public Map<String, EntityMetaData> getEntitiesMetaData(Set<String> entities);

  public EntityMetaData getEntityMetaData(String entity);

  public TIntObjectHashMap<EntityMetaData> getEntitiesMetaData(int[] entitiesIds);

  public EntityMetaData getEntityMetaData(int entityId);

  public Map<String, Double> getEntitiesImportances(Set<String> entities);

  public double getEntityImportance(String entity);

  public TIntDoubleHashMap getEntitiesImportances(int[] entitiesIds);

  public double getEntityImportance(int entityId);  
 }
