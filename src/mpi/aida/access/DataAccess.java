package mpi.aida.access;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import mpi.aida.config.AidaConfig;
import mpi.aida.data.Entities;
import mpi.aida.data.Entity;
import mpi.aida.data.EntityMetaData;
import mpi.aida.data.Keyphrases;
import mpi.aida.data.Type;
import mpi.aida.util.YagoUtil.Gender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataAccess {
  private static final Logger logger = 
      LoggerFactory.getLogger(DataAccess.class);

  private static DataAccessInterface dataAccess = null;

  /* Keyphrase sources */
  public static final String KPSOURCE_LINKANCHOR = "linkAnchor";

  public static final String KPSOURCE_INLINKTITLE = "inlinkTitle";

  public static final String KPSOURCE_CATEGORY = "wikipediaCategory";

  public static final String KPSOURCE_CITATION = "citationTitle";
  
  /** which type of data access*/
  public static enum type {
    sql, testing
  };

  private static synchronized void initDataAccess() {
    if (dataAccess != null) {
      return;
    }
    if (DataAccess.type.sql.toString().equalsIgnoreCase(AidaConfig.get(AidaConfig.DATAACCESS))) {
      dataAccess = new DataAccessSQL();
    } else if (DataAccess.type.testing.toString().equalsIgnoreCase(AidaConfig.get(AidaConfig.DATAACCESS))) {
      dataAccess = new DataAccessForTesting();
    } else {
      // Default is sql.
      logger.info("No dataAccess given in 'settings/aida.properties', " +
      		        "using 'sql' as default.");
      dataAccess = new DataAccessSQL();
    }
  }  

  private static DataAccessInterface getInstance() {
    if (dataAccess == null) {
      initDataAccess();
    }
    return dataAccess;
  }

  public static void init() {
    DataAccess.getInstance();
  }

  public static DataAccess.type getAccessType() {
    return DataAccess.getInstance().getAccessType();
  }

  public static Entities getEntitiesForMention(String mention, double maxEntityRank) {
    List<String> mentionAsList = new ArrayList<String>(1);
    mentionAsList.add(mention);
    Map<String, Entities> candidates = getEntitiesForMentions(mentionAsList, maxEntityRank);
    return candidates.get(mention);
  }
  
  public static Map<String, Entities> getEntitiesForMentions(Collection<String> mention, double maxEntityRank) {
    return DataAccess.getInstance().getEntitiesForMentions(mention, maxEntityRank);
  }
  
  public static Keyphrases getEntityKeyphrases(Entities entities) {
    return DataAccess.getInstance().getEntityKeyphrases(entities, null);
  }
  
  public static Keyphrases getEntityKeyphrases(
      Entities entities, Map<String, Double> keyphraseSourceWeights) {
    return DataAccess.getInstance().
        getEntityKeyphrases(entities, keyphraseSourceWeights);
  }
  
  /**
   * Retrieves all the Keyphrases for the given entities. Does not return
   * keyphrases when the source has a weight of 0.0.
   * 
   * If keyphraseSourceWeights is not null, the return object will also
   * contain all keyphrase sources. This will increase the data transfer,
   * so use wisely.
   * 
   * If minKeyphraseWeight > 0.0, keyphrases with a weight lower than
   * minKeyphraseWeight will not be returned.
   * 
   * If maxEntityKeyphraseCount > 0, at max maxEntityKeyphraseCount will
   * be returned for each entity.
   * 
   * @param entities
   * @param keyphraseSourceWeights
   * @param minKeyphraseWeight
   * @param maxEntityKeyphraseCount
   * @return
   */
  public static Keyphrases getEntityKeyphrases(
      Entities entities, Map<String, Double> keyphraseSourceWeights, 
      double minKeyphraseWeight, int maxEntityKeyphraseCount) {
    return DataAccess.getInstance().
        getEntityKeyphrases(entities, keyphraseSourceWeights, 
            minKeyphraseWeight, maxEntityKeyphraseCount);
  }

  public static void getEntityKeyphraseTokens(
      Entities entities,
      TIntObjectHashMap<int[]> entityKeyphrases,
      TIntObjectHashMap<int[]> keyphraseTokens) {
    DataAccess.getInstance().getEntityKeyphraseTokens(
        entities, entityKeyphrases, keyphraseTokens);
  }

  public static int[] getInlinkNeighbors(Entity e) {
    return DataAccess.getInstance().getInlinkNeighbors(e);
  }

  public static TIntObjectHashMap<int[]> getInlinkNeighbors(Entities entities) {
    return DataAccess.getInstance().getInlinkNeighbors(entities);
  }

  public static int getIdForYagoEntityId(String entity) {
    List<String> entities = new ArrayList<String>(1);
    entities.add(entity);
    return getIdsForYagoEntityIds(entities).get(entity);
  }

  public static TObjectIntHashMap<String> getIdsForYagoEntityIds(Collection<String> entityIds) {
    return DataAccess.getInstance().getIdsForYagoEntityIds(entityIds);
  }
  
  
  public static String getYagoEntityIdForId(int entity) {
    int[] entities = new int[1];
    entities[0] = entity;
    return getYagoEntityIdsForIds(entities).get(entity);
  }
   public static TIntObjectHashMap<String> getYagoEntityIdsForIds(int[] ids) {
    return DataAccess.getInstance().getYagoEntityIdsForIds(ids);
  }
   
   public static int getIdForTypeName(String typeName) {
     List<String> typeNames = new ArrayList<String>(1);
     typeNames.add(typeName);
     return getIdsForTypeNames(typeNames).get(typeName);
   }

   public static TObjectIntHashMap<String> getIdsForTypeNames(Collection<String> typeNames) {
     return DataAccess.getInstance().getIdsForTypeNames(typeNames);
   }
   
   
  public static Type getTypeNameForId(int typeId) {
    int[] types = new int[1];
    types[0] = typeId;
    return getTypeNamesForIds(types).get(typeId);
  }
  
  public static TIntObjectHashMap<Type> getTypeNamesForIds(int[] ids) {
    return DataAccess.getInstance().getTypeNamesForIds(ids);
  }
  
  public static int[] getTypeIdsForEntityId(int entityId) {
    int[] entities = new int[1];
    entities[0] = entityId;
    return getTypesIdsForEntitiesIds(entities).get(entityId);
  }
  
  public static TIntObjectHashMap<int[]> getTypesIdsForEntitiesIds(int[] ids) {
    return DataAccess.getInstance().getTypesIdsForEntitiesIds(ids);
  }
  
  
  public static int[] getEntitiesIdsForTypeId(int typeId) {
    int[] types = new int[1];
    types[0] = typeId;
    return getEntitiesIdsForTypesIds(types).get(typeId);
  }
  
  public static TIntObjectHashMap<int[]> getEntitiesIdsForTypesIds(int[] ids) {
    return DataAccess.getInstance().getEntitiesIdsForTypesIds(ids);
  }


  public static String getWordForId(int wordId) {
    int[] words = new int[1];
    words[0] = wordId;
    return getWordsForIds(words).get(wordId);
  }

  public static int getIdForWord(String word) {
    List<String> words = new ArrayList<String>(1);
    words.add(word);
    return getIdsForWords(words).get(word);  
  }
  
  public static TIntObjectHashMap<String> getWordsForIds(int[] wordIds) {
    return DataAccess.getInstance().getWordsForIds(wordIds);
  }
  
  /**
   * Transform words into the token ids. Unknown words will be assigned
   * the id -1
   * 
   * @param   words
   * @return  Ids for words ranging from 1 to MAX, -1 for missing.
   */
  public static TObjectIntHashMap<String> getIdsForWords(
      Collection<String> words) {
    return DataAccess.getInstance().getIdsForWords(words);
  }

  public static Map<String, Gender> getGenderForEntities(Entities entities) {
    return getInstance().getGenderForEntities(entities);
  }

  public static Map<String, Set<Type>> getTypes(Set<String> entities) {
    return getInstance().getTypes(entities);
  }

  public static Set<Type> getTypes(String entity) {
    return getInstance().getTypes(entity);
  }

  public static Map<String, List<String>> getAllEntitiesMetaData(String startingWith){
    return getInstance().getAllEntitiesMetaData(startingWith);
  }

  public static Map<String, EntityMetaData> getEntitiesMetaData(Set<String> entities) {
    return getInstance().getEntitiesMetaData(entities);
  }

  public static EntityMetaData getEntityMetaData(String Entity) {
    return getInstance().getEntityMetaData(Entity);
  }
  
  public static TIntObjectHashMap<EntityMetaData> getEntitiesMetaData(int[] entitiesIds) {
    return getInstance().getEntitiesMetaData(entitiesIds);
  }

  public static EntityMetaData getEntityMetaData(int entityId) {
    return getInstance().getEntityMetaData(entityId);
  }
  
  public static Map<String, Double> getEntitiesImportances(Set<String> entities) {
    return getInstance().getEntitiesImportances(entities);
  }

  public static double getEntityImportance(String Entity) {
    return getInstance().getEntityImportance(Entity);
  }
  
  public static TIntDoubleHashMap getEntitiesImportances(int[] entitiesIds) {
    return getInstance().getEntitiesImportances(entitiesIds);
  }

  public static double getEntityImportance(int entityId) {
    return getInstance().getEntityImportance(entityId);
  }
  
  public static TIntIntHashMap getKeyphraseDocumentFrequencies(TIntHashSet keyphrases) {    
    return getInstance().getKeyphraseDocumentFrequencies(keyphrases);
  }

  public static List<String> getParentTypes(String queryType) {
    return getInstance().getParentTypes(queryType);
  }

  public static boolean checkEntityNameExists(String entityName) {
    return getInstance().checkEntityNameExists(entityName);
  }

  public static String getKeyphraseSource(String entityName, String keyphrase) {
    return getInstance().getKeyphraseSource(entityName, keyphrase);
  }

  public static TIntObjectHashMap<int[]> getEntityLSHSignatures(Entities entities, String table) {
    return getInstance().getEntityLSHSignatures(entities, table);
  }

  public static TIntObjectHashMap<int[]> getEntityLSHSignatures(Entities entities) {
    return getInstance().getEntityLSHSignatures(entities);
  }

  public static String getFamilyName(String entity) {
    return getInstance().getFamilyName(entity);
  }

  public static String getGivenName(String entity) {
    return getInstance().getGivenName(entity);
  }
  
  public static TIntDoubleHashMap getEntityPriors(String mention) {
    return getInstance().getEntityPriors(mention);
  }

  public static TIntIntHashMap getKeywordDocumentFrequencies(TIntHashSet keywords) {
    TIntIntHashMap keywordCounts = new TIntIntHashMap(keywords.size(), 1.0f);
    for (TIntIterator itr = keywords.iterator(); itr.hasNext(); ) {
      int keywordId = itr.next();      
      int count = DataAccessCache.singleton().getKeywordCount(keywordId);
      keywordCounts.put(keywordId, count);
    }    
    return keywordCounts;   
  }

  public static TIntIntHashMap getEntitySuperdocSize(Entities entities) {
    return getInstance().getEntitySuperdocSize(entities);
  }

  public static TIntObjectHashMap<TIntIntHashMap> getEntityKeywordIntersectionCount(Entities entities) {
    return getInstance().getEntityKeywordIntersectionCount(entities);
  }

  public static TObjectIntHashMap<String> getAllEntityIds() {
    return getInstance().getAllEntityIds();
  }
  
  public static Entities getAllEntities() {
    return getInstance().getAllEntities();
  }

  // Will return an array where the index is the id of the word to be expanded
  // and the value is the id of the expanded word. Expansion is done by
  // fully uppercasing the original word behind the id.
  public static int[] getAllWordExpansions() {
    return getInstance().getAllWordExpansions();
  }

  public static boolean isYagoEntity(Entity entity) {
    return getInstance().isYagoEntity(entity);
  }

  public static TIntObjectHashMap<int[]> getAllInlinks() {
    return getInstance().getAllInlinks();
  }
  
  public static TObjectIntHashMap<String> getAllWordIds() {
    return getInstance().getAllWordIds();
  }
  
  
  /**
   * Used for the weight computation. This returns the total number of 
   * documents in the collection for the computation of the keyword IDF weights.
   * In the original AIDA setting with YAGO-entities this is the number of 
   * Wikipedia entities.
   * @return  Collection Size.
   */
  public static int getCollectionSize() {
    return getInstance().getCollectionSize();
  }

  public static int getWordExpansion(int wordId) {
    return getInstance().getWordExpansion(wordId);
  }
  
  public static String getConfigurationName() {
    return getInstance().getConfigurationName();
  }

  public static int expandTerm(int wordId) {
    return DataAccessCache.singleton().expandTerm(wordId);
  }

  public static String expandTerm(String term) {
    return term.toUpperCase(Locale.ENGLISH);
  }

  public static int[] getAllKeywordDocumentFrequencies() {
    return getInstance().getAllKeywordDocumentFrequencies();
  }  
}
