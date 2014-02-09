package mpi.aida.access;

import edu.stanford.nlp.util.StringUtils;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import mpi.aida.AidaManager;
import mpi.aida.access.DataAccessSQLCache.EntityKeyphraseData;
import mpi.aida.data.Entities;
import mpi.aida.data.Entity;
import mpi.aida.data.EntityMetaData;
import mpi.aida.data.Keyphrases;
import mpi.aida.data.Type;
import mpi.aida.graph.similarity.PriorProbability;
import mpi.aida.util.Util;
import mpi.aida.util.YagoUtil;
import mpi.aida.util.YagoUtil.Gender;
import mpi.tools.basics.Normalize;
import mpi.tools.database.DBConnection;
import mpi.tools.database.interfaces.DBStatementInterface;
import mpi.tools.javatools.datatypes.Pair;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataAccessSQL implements DataAccessInterface {
  private static final Logger logger = 
      LoggerFactory.getLogger(DataAccessSQL.class);
  
  public static final String ENTITY_KEYPHRASES = "entity_keyphrases";
  public static final String ENTITY_IDS = "entity_ids";
  public static final String TYPE_IDS = "type_ids";
  public static final String NAME_IDS = "named_ids";
  public static final String WORD_IDS = "word_ids";
  public static final String WORD_EXPANSION = "word_expansion";
  public static final String KEYPHRASE_COUNTS = "keyphrase_counts";
  public static final String KEYPHRASES_SOURCES_WEIGHT = "keyphrases_sources_weights";
  public static final String KEYWORD_COUNTS = "keyword_counts";
  public static final String ENTITY_COUNTS = "entity_counts";
  public static final String ENTITY_KEYWORDS = "entity_keywords";
  public static final String ENTITY_LSH_SIGNATURES = "entity_lsh_signatures_2000";
  public static final String ENTITY_INLINKS = "entity_inlinks";
  public static final String ENTITY_TYPES = "entity_types";
  public static final String TYPE_ENTITIES = "type_entities";
  public static final String ENTITY_RANK = "entity_rank";
  public static final String DICTIONARY = "dictionary";
  public static final String YAGO_FACTS = "facts";
  public static final String METADATA = "meta";
  public static final String ENTITY_METADATA = "entity_metadata";
  public static final String KEYPHRASE_SOURCE_WEIGHTS = "keyphrases_sources_weights";
  
  public static final String YAGO_HAS_CITATIONS_TITLE_RELATION = "hasCitationTitle";
  public static final String YAGO_HAS_WIKIPEDIA_CATEGORY_RELATION = "hasWikipediaCategory";
  public static final String YAGO_HAS_WIKIPEDIA_ANCHOR_TEXT_RELATION = "hasWikipediaAnchorText";
  public static final String YAGO_HAS_INTERNAL_WIKIPEDIA_LINK_TO_RELATION = "hasInternalWikipediaLinkTo";
  public static final String YAGO_TYPE_RELATION = "type";
  
  @Override
  public DataAccess.type getAccessType() {
    return DataAccess.type.sql;
  }

  @Override
  public Map<String, Entities> getEntitiesForMentions(Collection<String> mentions, double maxEntiyRank) {
    Map<String, Entities> candidates = new HashMap<String, Entities>(mentions.size(), 1.0f);
    if (mentions.size() == 0) {
      return candidates;
    }
    List<String> queryMentions = new ArrayList<String>(mentions.size());
    for (String m : mentions) {
      queryMentions.add(Normalize.string(PriorProbability.conflateMention(m)));
      // Add an emtpy candidate set as default.
      candidates.put(m, new Entities());
    }
    DBConnection mentionEntityCon = null;
    DBStatementInterface statement = null;
    Map<String, TIntHashSet> queryMentionCandidates = new HashMap<String, TIntHashSet>();
    try {
      mentionEntityCon = 
          AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "mentions for entity");
      statement = mentionEntityCon.getStatement();
      String sql = null;
      String query = YagoUtil.getPostgresEscapedConcatenatedQuery(queryMentions);
      if (maxEntiyRank < 1.0) {
        sql = "SELECT " + DICTIONARY + ".mention, " + 
            DICTIONARY + ".entity FROM " + DICTIONARY + 
            " JOIN " + ENTITY_RANK +
            " ON " + DICTIONARY + ".entity=" + ENTITY_RANK + ".entity" +
            " WHERE mention IN (" + query + ")" +
            " AND rank<" + maxEntiyRank; 
      } else {
        sql = "SELECT mention, entity FROM " + DICTIONARY + 
              " WHERE mention IN (" + query + ")";
      }
      ResultSet r = statement.executeQuery(sql);
      while (r.next()) {
        String mention = r.getString(1);
        int entity = r.getInt(2);
        TIntHashSet entities = queryMentionCandidates.get(mention);
        if (entities == null) {
          entities = new TIntHashSet();
          queryMentionCandidates.put(mention, entities);
        }
        entities.add(entity);
      }
      // Get the candidates for the original Strings.
      for (Entry<String, Entities> entry: candidates.entrySet()) {
        String queryMention = Normalize.string(
            PriorProbability.conflateMention(entry.getKey()));
        TIntHashSet entityIds = queryMentionCandidates.get(queryMention);
        if (entityIds != null) {
          int[] ids = entityIds.toArray();
          TIntObjectHashMap<String> yagoEntityIds = getYagoEntityIdsForIds(ids);
          Entities entities = entry.getValue();
          for (int i = 0; i < ids.length; ++i) {
            entities.add(new Entity(yagoEntityIds.get(ids[i]), ids[i]));
          }
        }
      }
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
      e.printStackTrace();
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, mentionEntityCon);
    }
    return candidates;
  }

  @Override
  public Keyphrases getEntityKeyphrases(
      Entities entities, Map<String, Double> keyphraseSourceWeights) {
    return getEntityKeyphrases(entities, keyphraseSourceWeights, 0.0, 0);
  }
  
  @Override
  public Keyphrases getEntityKeyphrases(
      Entities entities, Map<String, Double> keyphraseSourceWeights,
      double minKeyphraseWeight, int maxEntityKeyphraseCount) {
    boolean useSources = keyphraseSourceWeights != null && !keyphraseSourceWeights.isEmpty();
    KeyphrasesCache kpc = 
        DataAccessSQLCache.singleton().
        getEntityKeyphrasesCache(entities, keyphraseSourceWeights, minKeyphraseWeight,
            maxEntityKeyphraseCount, useSources);
    
    // Create and fill return object with empty maps.
    Keyphrases keyphrases = new Keyphrases();
    
    TIntObjectHashMap<int[]> entityKeyphrases = 
        new TIntObjectHashMap<int[]>();
    TIntObjectHashMap<int[]> keyphraseTokens = 
        new TIntObjectHashMap<int[]>();
    TIntObjectHashMap<TIntDoubleHashMap> entity2keyphrase2mi = 
        new TIntObjectHashMap<TIntDoubleHashMap>();
    TIntObjectHashMap<TIntDoubleHashMap> entity2keyword2mi = 
        new TIntObjectHashMap<TIntDoubleHashMap>();
    
    // Fill the keyphrases object with all data.
    keyphrases.setEntityKeyphrases(entityKeyphrases);
    keyphrases.setKeyphraseTokens(keyphraseTokens);
    keyphrases.setEntityKeyphraseWeights(entity2keyphrase2mi);
    keyphrases.setEntityKeywordWeights(entity2keyword2mi);
    
    if (useSources) {
      TIntObjectHashMap<TIntIntHashMap> entity2keyphrase2source =
          new TIntObjectHashMap<TIntIntHashMap>();
      keyphrases.setEntityKeyphraseSources(entity2keyphrase2source);
      TObjectIntHashMap<String> keyphraseSource2id = 
          new TObjectIntHashMap<String>();
      keyphrases.setKeyphraseSource2id(keyphraseSource2id);
      TIntDoubleHashMap keyphraseSourceId2weight =
          new TIntDoubleHashMap();
      keyphrases.setKeyphraseSourceWeights(keyphraseSourceId2weight);
    }
    
    if (entities == null || entities.getUniqueNames().size() == 0) {            
      return keyphrases;
    }

    TIntObjectHashMap<TIntHashSet> eKps = 
        new TIntObjectHashMap<TIntHashSet>();
    for (Entity e : entities) {
      eKps.put(e.getId(), new TIntHashSet());
    }
        
    for (Pair<Integer, EntityKeyphraseData> p : kpc) {
      int entity = p.first;
      EntityKeyphraseData ekd = p.second;
      int keyphrase = ekd.keyphrase;
      double keyphraseWeight = ekd.weight;
              
      // Add keyphrase.
      TIntHashSet kps = eKps.get(entity);
      if (kps == null) {
        kps = new TIntHashSet();
        eKps.put(entity, kps);
      }
      kps.add(keyphrase);
      
      // Add keyphrase weight.
      TIntDoubleHashMap keyphrase2mi = entity2keyphrase2mi.get(entity);
      if (keyphrase2mi == null) {
        keyphrase2mi = new TIntDoubleHashMap();
        entity2keyphrase2mi.put(entity, keyphrase2mi);
      }
      keyphrase2mi.put(keyphrase, keyphraseWeight);
      
      // Add keywords and weights.
      Integer[] tokens = ekd.keyphrase_tokens;
      Double[] tokenWeights = ekd.keyphrase_token_weights;
      TIntDoubleHashMap keyword2mi = entity2keyword2mi.get(entity);
      if (keyword2mi == null) {
        keyword2mi = new TIntDoubleHashMap();
        entity2keyword2mi.put(entity, keyword2mi);
      }
      int[] tokenIds = new int[tokens.length];
      for (int i = 0; i < tokens.length; ++i) {
        tokenIds[i] = tokens[i];
        // Fill missing weights with 0.
        Double weight = 0.0;
        if (i < tokenWeights.length) {
          weight = tokenWeights[i];
        }
        keyword2mi.put(tokenIds[i], weight);
      }
      if (!keyphraseTokens.containsKey(keyphrase)) {
        keyphraseTokens.put(keyphrase, tokenIds);
      }
      
      if (useSources) {
        String source = ekd.source;
        TObjectIntHashMap<String> s2id = keyphrases.getKeyphraseSource2id();
        int sourceId = s2id.size();
        if (s2id.contains(source)) {
          sourceId = s2id.get(source);
        } else {
          s2id.put(source, sourceId);
        }
        TIntIntHashMap keyphraseSources = 
            keyphrases.getEntityKeyphraseSources().get(entity);
        if (keyphraseSources == null) {
          keyphraseSources = new TIntIntHashMap();
          keyphrases.getEntityKeyphraseSources().put(entity, keyphraseSources);
        }
        keyphraseSources.put(keyphrase, sourceId);
      }
    }

    // Transform eKps to entityKeyphrases.
    for (Entity e : entities) {
      entityKeyphrases.put(e.getId(), eKps.get(e.getId()).toArray());
    }
    
    return keyphrases;
  }
  
  @Override
  public void getEntityKeyphraseTokens(
      Entities entities,
      TIntObjectHashMap<int[]> entityKeyphrases,
      TIntObjectHashMap<int[]> keyphraseTokens) {
    if (entities == null | entities.getUniqueNames().size() == 0) {
      return;
    }

    DBConnection con = null;
    DBStatementInterface statement = null;

    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting keyphrases");
      String entityQueryString = StringUtils.join(entities.getUniqueIds(), ",");
      statement = con.getStatement();

      String sql = "SELECT entity,keyphrase,keyphrase_tokens" +
      		         " FROM " + ENTITY_KEYPHRASES +
      		         " WHERE entity IN (" + entityQueryString + ")";
      ResultSet rs = statement.executeQuery(sql);
      TIntObjectHashMap<TIntHashSet> eKps = 
          new TIntObjectHashMap<TIntHashSet>();
      for (Entity e : entities) {
        eKps.put(e.getId(), new TIntHashSet());
      }
      while (rs.next()) {
        int entity = rs.getInt("entity");
        int keyphrase = rs.getInt("keyphrase");
        TIntHashSet kps = eKps.get(entity);
        if (kps == null) {
          kps = new TIntHashSet();
          eKps.put(entity, kps);
        }
        kps.add(keyphrase);
        Integer[] tokens = 
            (Integer[]) rs.getArray("keyphrase_tokens").getArray();
        if (!keyphraseTokens.containsKey(keyphrase)) {
          int[] tokenIds = new int[tokens.length];
          for (int i = 0; i < tokens.length; ++i) {
            tokenIds[i] = tokens[i];
          }
          keyphraseTokens.put(keyphrase, tokenIds);
        }
      }
      rs.close();
      statement.commit();
      
      // Transform eKps to entityKeyphrases.
      for (Entity e : entities) {
        entityKeyphrases.put(e.getId(), eKps.get(e.getId()).toArray());
      }
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
  }

  @Override
  public TIntIntHashMap getKeywordDocumentFrequencies(TIntHashSet keywords) {
    TIntIntHashMap keywordDF = new TIntIntHashMap();
    
    if (keywords == null || keywords.size() == 0) {
      return keywordDF;
    }

    DBConnection con = null;
    DBStatementInterface statement = null;
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting keyword frequencies");
      statement = con.getStatement();
      String keywordQuery = YagoUtil.getIdQuery(keywords);
      String sql = "SELECT keyword,count" +
      		          " FROM " + KEYWORD_COUNTS + 
      		          " WHERE keyword IN (" + keywordQuery + ")";
      ResultSet r = statement.executeQuery(sql);
      while (r.next()) {
        int kw = r.getInt("keyword");
        int c = r.getInt("count");
        keywordDF.put(kw, c);
      }
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    
    return keywordDF;
  }

  @Override
  public TIntIntHashMap getEntitySuperdocSize(Entities entities) {
    TIntIntHashMap entitySuperDocSizes = new TIntIntHashMap();
    
    if (entities == null || entities.size() == 0) {
      return entitySuperDocSizes;
    }

    DBConnection con = null;
    DBStatementInterface statement = null;
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting entity superdoc sizes");
      statement = con.getStatement();
      String entitiesQuery = StringUtils.join(entities.getUniqueIds(), ",");
      String sql = "SELECT entity, count" +
      		          " FROM " + ENTITY_COUNTS +
      		          " WHERE entity IN (" + entitiesQuery + ")";
      ResultSet r = statement.executeQuery(sql);
      while (r.next()) {
        int entity = r.getInt("entity");
        int entityDocCount = r.getInt("count");
        entitySuperDocSizes.put(entity, entityDocCount);
      }
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    
    return entitySuperDocSizes;
  }

  @Override
  public TIntObjectHashMap<TIntIntHashMap> getEntityKeywordIntersectionCount(Entities entities) {
    TIntObjectHashMap<TIntIntHashMap> entityKeywordIC = new TIntObjectHashMap<TIntIntHashMap>();
    for (Entity entity : entities) {
     TIntIntHashMap keywordsIC = new TIntIntHashMap();
      entityKeywordIC.put(entity.getId(), keywordsIC);
    }
    
    if (entities == null || entities.size() == 0) {
      return entityKeywordIC;
    }

    DBConnection con = null;
    DBStatementInterface statement = null;
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting entity-keyword intersection counts");
      statement = con.getStatement();
      String entitiesQuery = StringUtils.join(entities.getUniqueIds(), ",");
      String sql = "SELECT entity, keyword, count" +
      		         " FROM " + ENTITY_KEYWORDS +
      		         " WHERE entity IN (" + entitiesQuery + ")";
      ResultSet r = statement.executeQuery(sql);
      while (r.next()) {
        int entity = r.getInt("entity");
        int keyword = r.getInt("keyword");
        int keywordCount = r.getInt("count");
        entityKeywordIC.get(entity).put(keyword, keywordCount);
      }
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    
    return entityKeywordIC;
  }

  public int[] getInlinkNeighbors(Entity entity) {
    Entities entities = new Entities();
    entities.add(entity);
    TIntObjectHashMap<int[]> neighbors = getInlinkNeighbors(entities);
    return neighbors.get(entity.getId());
  }

  @Override
  public TIntObjectHashMap<int[]> getInlinkNeighbors(Entities entities) {
    TIntObjectHashMap<int[]> neighbors = new TIntObjectHashMap<int[]>();
    for (int entityId : entities.getUniqueIds()) {
      neighbors.put(entityId, new int[0]);
    }

    DBConnection con = null;
    DBStatementInterface statement = null;
    
    String entitiesQuery = StringUtils.join(entities.getUniqueIds(), ",");
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      statement = con.getStatement();
      String sql = "SELECT entity, inlinks FROM " + 
                   DataAccessSQL.ENTITY_INLINKS + 
                   " WHERE entity IN (" + entitiesQuery + ")";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        Integer[] neigbors = (Integer[]) rs.getArray("inlinks").getArray();
        int entity = rs.getInt("entity");
        neighbors.put(entity, ArrayUtils.toPrimitive(neigbors));
      }
      rs.close();
      statement.commit();
      return neighbors;
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage() +"###SQL="+"SELECT entity, inlinks FROM " + 
              DataAccessSQL.ENTITY_INLINKS + 
              " WHERE entity IN (" + entitiesQuery + ")");
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return neighbors;
  }

  @Override
  public Map<String, Gender> getGenderForEntities(Entities entities) {
    Map<String, Gender> entityGenders = new HashMap<String, Gender>();

    DBConnection con = null;
    DBStatementInterface statement = null;
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_YAGO2_FULL, "YNG");
      statement = con.getStatement();
      String sql = "SELECT arg1,arg2 FROM " + YAGO_FACTS + 
                   " WHERE arg1 IN (" + YagoUtil.getPostgresEscapedConcatenatedQuery(entities.getUniqueNames()) + ") " + "AND relation='hasGender'";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        String entity = rs.getString("arg1");
        String gender = rs.getString("arg2");

        Gender g = Gender.FEMALE;

        if (gender.equals("male")) {
          g = Gender.MALE;
        }

        entityGenders.put(entity, g);
      }
      rs.close();
      statement.commit();
      return entityGenders;
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_YAGO2_FULL, con);
    }
    return entityGenders;
  }

  public Set<Type> getTypes(String entity) {
    Set<String> entities = new HashSet<String>();
    entities.add(entity);
    Map<String, Set<Type>> results = getTypes(entities);
    return results.get(entity);
  }

  public Map<String, Set<Type>> getTypes(Set<String> entities) {
    Map<String, Set<Type>> entityTypesNames = new HashMap<String, Set<Type>>();
    
    TObjectIntHashMap<String> entitiesIds = getIdsForYagoEntityIds(entities);
    TIntObjectHashMap<int[]> entitiesTypesIds = getTypesIdsForEntitiesIds(entitiesIds.values());
    
    for(String entity: entities) {
      int entityId = entitiesIds.get(entity);
      int[] typesIds = entitiesTypesIds.get(entityId);
      TIntObjectHashMap<Type> typeNamesMap = getTypeNamesForIds(typesIds);
      Type[] typesNames = new Type[typeNamesMap.values().length]; 
      typesNames = typeNamesMap.values(typesNames);
      Set<Type> types = new HashSet<Type>();
      for (Type t : typesNames) {
        types.add(t);
      }
      entityTypesNames.put(entity, types);
      
    }
    return entityTypesNames;
  }

 
  @Override
  public TIntIntHashMap getKeyphraseDocumentFrequencies(
      TIntHashSet keyphrases) {
    TIntIntHashMap keyphraseCounts = new TIntIntHashMap();

    if (keyphrases == null || keyphrases.size() == 0) {
      return keyphraseCounts;
    }

    DBConnection con = null;
    DBStatementInterface statement = null;

    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting keyphrase counts");
      statement = con.getStatement();

      String keyphraseQueryString = YagoUtil.getIdQuery(keyphrases);

      String sql = "SELECT keyphrase,count " + "FROM " + KEYPHRASE_COUNTS + 
                   " WHERE keyphrase IN (" + keyphraseQueryString + ")";
      ResultSet rs = statement.executeQuery(sql);

      while (rs.next()) {
        int keyphrase = rs.getInt("keyphrase");
        int count= rs.getInt("count");

        keyphraseCounts.put(keyphrase, count);
      }

      rs.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }

    return keyphraseCounts;
  }

  /**
   * Retrieves all parent types for the given YAGO2 type (via the subClassOf relation).
   * 
   * @param type  type (in YAGO2 format) to retrieve parent types for 
   * @return        List of types in YAGO2 format
   * @throws SQLException
   */
  @Override
  public List<String> getParentTypes(String queryType) {
    List<String> types = new LinkedList<String>();
    DBConnection con = null;
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_YAGO2, "YN");
      DBStatementInterface statement = con.getStatement();
      String sql = "SELECT arg2 FROM facts " + "WHERE arg1=E'" + YagoUtil.getPostgresEscapedString(queryType) + "' " + "AND relation='subclassOf'";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        String type = rs.getString(1);
        types.add(type);
      }
      rs.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_YAGO2, con);
    }
    return types;
  }

  @Override
  public boolean checkEntityNameExists(String entity) {
    return isYagoEntity(YagoUtil.getEntityForYagoId(entity));
  }

  @Override
  public String getKeyphraseSource(String entityName, String keyphrase) {
    DBConnection con = null;
    DBStatementInterface statement = null;

    String source = null;

    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting Wikipedia superdoc keyphrases");
      statement = con.getStatement();

      String sql = "SELECT source FROM " + ENTITY_KEYPHRASES + " WHERE entity='" + YagoUtil.getPostgresEscapedString(entityName) + "'" + " AND keyphrase='" + YagoUtil.getPostgresEscapedString(keyphrase) + "'";
      ResultSet rs = statement.executeQuery(sql);

      while (rs.next()) {
        source = rs.getString("source");
      }

      rs.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }

    return source;
  }

  @Override
  public Map<String, List<String>> getKeyphraseEntities(Set<String> keyphrases) {
    DBConnection con = null;
    DBStatementInterface statement = null;

    Map<String, List<String>> keyphraseEntities = new HashMap<String, List<String>>();

    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting keyphrase-entities");
      String keyphraseQueryString = YagoUtil.getPostgresEscapedConcatenatedQuery(keyphrases);
      statement = con.getStatement();

      String sql = "SELECT entity,keyphrase FROM entity_keyphrases WHERE keyphrase IN (" + keyphraseQueryString + ")";

      ResultSet rs = statement.executeQuery(sql);
      for (String kp : keyphrases) {
        keyphraseEntities.put(kp, new LinkedList<String>());
      }
      while (rs.next()) {
        String entity = rs.getString("entity");
        String keyphrase = rs.getString("keyphrase");
        List<String> entities = keyphraseEntities.get(keyphrase);
        entities.add(entity);
      }
      rs.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }

    return keyphraseEntities;
  }

  @Override
  public TIntObjectHashMap<int[]> getEntityLSHSignatures(Entities entities) {
    return getEntityLSHSignatures(entities, ENTITY_LSH_SIGNATURES);
  }

  @Override
  public TIntObjectHashMap<int[]> getEntityLSHSignatures(Entities entities, String table) {
//    Map<String, int[]> tmpEntitySignatures = new HashMap<String, int[]>();
    TIntObjectHashMap<int[]> entitySignatures = 
        new TIntObjectHashMap<int[]>();
    
    DBConnection con = null;
    DBStatementInterface statement = null;

    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting entity-keyphrases");
//      String entityQueryString = YagoUtil.getPostgresEscapedConcatenatedQuery(entities.getUniqueNames());
      String entityQueryString = StringUtils.join(entities.getUniqueIds(), ",");
      statement = con.getStatement();
      
      String sql = "SELECT entity, signature FROM " + table + " WHERE entity IN (" + entityQueryString + ")";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        int entity = rs.getInt("entity");
        int[] sig = org.apache.commons.lang.ArrayUtils.toPrimitive((Integer[]) rs.getArray("signature").getArray());
        entitySignatures.put(entity, sig);        
      }
      rs.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }

    return entitySignatures;
  }

  @Override
  public String getFamilyName(String entity) {
    String familyName = null;
    DBConnection mentionEntityCon = null;
    DBStatementInterface statement = null;
    try {
      entity = YagoUtil.getPostgresEscapedString(entity);
      mentionEntityCon = AidaManager.getConnectionForDatabase(AidaManager.DB_YAGO2_FULL, "mentions for entity");
      statement = mentionEntityCon.getStatement();
      String sql = "select arg2 from facts where arg1 = '" + entity + "' and relation = 'hasFamilyName'";
      ResultSet r = statement.executeQuery(sql);
      if (r.next()) {
        familyName = r.getString("arg2");
      }
      if (familyName != null) {
        familyName = Normalize.unString(familyName);
      }
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_YAGO2_FULL, mentionEntityCon);
    }
    return familyName;
  }

  @Override
  public String getGivenName(String entity) {
    String givenName = null;
    DBConnection mentionEntityCon = null;
    DBStatementInterface statement = null;
    try {
      entity = YagoUtil.getPostgresEscapedString(entity);
      mentionEntityCon = AidaManager.getConnectionForDatabase(AidaManager.DB_YAGO2_FULL, "mentions for entity");
      statement = mentionEntityCon.getStatement();
      String sql = "select arg2 from facts where arg1 = '" + entity + "' and relation = 'hasGivenName'";
      ResultSet r = statement.executeQuery(sql);
      if (r.next()) {
        givenName = r.getString("arg2");
      }
      if (givenName != null) {
        givenName = Normalize.unString(givenName);
      }
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_YAGO2_FULL, mentionEntityCon);
    }
    return givenName;
  }

  public TIntDoubleHashMap getEntityPriors(String mention) {
    mention = PriorProbability.conflateMention(mention);
    TIntDoubleHashMap entityPriors = new TIntDoubleHashMap();
    DBConnection con = null;
    DBStatementInterface statement = null;
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, 
                                           "getting priors");
      statement = con.getStatement();
      String sql = "SELECT entity,prior FROM " + DICTIONARY +
                   " WHERE mention=E'" + 
                    YagoUtil.getPostgresEscapedString(
                        Normalize.string(mention)) + "'";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        int entity = rs.getInt("entity");
        double prior = rs.getDouble("prior");
        entityPriors.put(entity, prior);
      }
      rs.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return entityPriors;
  }

  @Override
  public TIntObjectHashMap<String> getYagoEntityIdsForIds(int[] ids) {
    TIntObjectHashMap<String> entityIds = new TIntObjectHashMap<String>();
    if (ids.length == 0) {
      return entityIds;
    }
    DBConnection con = null;
    DBStatementInterface stmt = null;
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting Ids");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(100000);
      String idQuery = YagoUtil.getIdQuery(ids);
      String sql = "SELECT entity, id FROM " + DataAccessSQL.ENTITY_IDS + 
                   " WHERE id IN (" + idQuery + ")";
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        String entity = rs.getString("entity");
        int id = rs.getInt("id");
        entityIds.put(id, entity);

        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " entity ids.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return entityIds;
  }

  @Override
  public TObjectIntHashMap<String> getIdsForYagoEntityIds(
      Collection<String> yagoIds) {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    TObjectIntHashMap<String> entityIds = new TObjectIntHashMap<String>();
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting Ids");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(100000);
      String idQuery = YagoUtil.getPostgresEscapedConcatenatedQuery(yagoIds);
      String sql = "SELECT entity, id FROM " + DataAccessSQL.ENTITY_IDS + 
                   " WHERE entity IN (" + idQuery + ")";
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        String entity = rs.getString("entity");
        int id = rs.getInt("id");
        entityIds.put(entity, id);

        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " entity ids.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return entityIds;
  }

  @Override
  public TIntObjectHashMap<String> getWordsForIds(int[] ids) {
    TIntObjectHashMap<String> wordIds = new TIntObjectHashMap<String>();
    if (ids.length == 0) {
      return wordIds;
    }
    DBConnection con = null;
    DBStatementInterface stmt = null;
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting Ids");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(100000);
      String idQuery = YagoUtil.getIdQuery(ids);
      String sql = "SELECT word, id FROM " + DataAccessSQL.WORD_IDS + 
                   " WHERE id IN (" + idQuery + ")";
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        String word = rs.getString("word");
        int id = rs.getInt("id");
        wordIds.put(id, word);

        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " word ids.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return wordIds;
  }

  @Override
  public TObjectIntHashMap<String> getIdsForWords(Collection<String> keywords) {
    TObjectIntHashMap<String> wordIds = new TObjectIntHashMap<String>();
    if (keywords.isEmpty()) {
      return wordIds;
    }
    DBConnection con = null;
    DBStatementInterface stmt = null;
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting Ids");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(100000);
      String idQuery = YagoUtil.getPostgresEscapedConcatenatedQuery(keywords);
      String sql = "SELECT word, id FROM " + DataAccessSQL.WORD_IDS + 
                   " WHERE word IN (" + idQuery + ")";
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        String word = rs.getString("word");
        int id = rs.getInt("id");
        wordIds.put(word, id);

        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " word ids.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return wordIds;
  }

  @Override
  public TObjectIntHashMap<String> getAllEntityIds() {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    TObjectIntHashMap<String> entityIds = new TObjectIntHashMap<String>();
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting Entity Ids");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(100000);
      String sql = "SELECT entity, id FROM " + DataAccessSQL.ENTITY_IDS +
                   " WHERE entity NOT LIKE '\"%'";
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        String entity = rs.getString("entity");
        int id = rs.getInt("id");
        entityIds.put(entity, id);

        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " entity ids.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return entityIds;
  }

  @Override
  public TObjectIntHashMap<String> getAllWordIds() {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    TObjectIntHashMap<String> wordIds = new TObjectIntHashMap<String>(10000000);
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting Word Ids");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(100000);
      String sql = "SELECT word, id FROM " + DataAccessSQL.WORD_IDS;
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        String word = rs.getString("word");
        int id = rs.getInt("id");
        wordIds.put(word, id);

        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " word ids.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return wordIds;
  }

  @Override
  public Entities getAllEntities() {
    TObjectIntHashMap<String> entityIds = getAllEntityIds();
    Entities entities = new Entities();
    for (TObjectIntIterator<String> itr = entityIds.iterator(); 
        itr.hasNext(); ) {
      itr.advance();
      entities.add(new Entity(itr.key(), itr.value()));
    }
    return entities;
  }

  @Override
  public int[] getAllWordExpansions() {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    TIntIntHashMap wordExpansions = new TIntIntHashMap();
    int maxId = -1;
    try {
      logger.info("Reading word expansions.");
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting word expansions");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(1000000);
      String sql = "SELECT word, expansion FROM " + WORD_EXPANSION;
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        int word = rs.getInt("word");
        int expansion = rs.getInt("expansion");
        wordExpansions.put(word, expansion);
        if (word > maxId) {
          maxId = word;
        }

        if (++read % 1000000 == 0) {
         logger.debug("Read " + read + " word expansions.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    
    // Transform hash to int array.
    int[] expansions = new int[maxId + 1];
    for (TIntIntIterator itr = wordExpansions.iterator(); itr.hasNext(); ) {
      itr.advance();
      assert itr.key() < expansions.length && itr.key() > 0;  // Ids start at 1.
      expansions[itr.key()] = itr.value();
    }
    return expansions;
  }
  
  @Override
  public int getWordExpansion(int wordId) {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    int wordExpansion = 0;
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting word expansions");
      stmt = con.getStatement();
      String sql = "SELECT expansion FROM " + WORD_EXPANSION + 
                   " WHERE word=" + wordId;
      ResultSet rs = stmt.executeQuery(sql);
      if (rs.next()) {
        wordExpansion = rs.getInt("expansion");
      }
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return wordExpansion;
  }

  @Override
  public boolean isYagoEntity(Entity entity) {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    boolean isYagoEntity = false;
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_YAGO2, "Checking YAGO entity");
      stmt = con.getStatement();
      String sql = "SELECT arg1 FROM facts WHERE arg1=E'" + 
                    YagoUtil.getPostgresEscapedString(entity.getName()) + 
                    "' AND relation='hasWikipediaUrl'";
      ResultSet rs = stmt.executeQuery(sql);
      
      // if there is a result, it means it is a YAGO entity
      if (rs.next()) {
        isYagoEntity = true;
      } 
      rs.close();
      stmt.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_YAGO2, con);
    }
    return isYagoEntity;
  }

  @Override
  public TIntObjectHashMap<int[]> getAllInlinks() {
    TIntObjectHashMap<int[]> inlinks = new TIntObjectHashMap<int[]>();
    DBConnection con = null;
    DBStatementInterface statement = null;    
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      con.setAutoCommit(false);
      statement = con.getStatement();
      statement.setFetchSize(100000);
      int read = 0;
      String sql = "SELECT entity, inlinks FROM " + DataAccessSQL.ENTITY_INLINKS; 
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        Integer[] neigbors = (Integer[]) rs.getArray("inlinks").getArray();
        int entity = rs.getInt("entity");
        inlinks.put(entity, ArrayUtils.toPrimitive(neigbors));
        
        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " entity inlinks.");
        }
      }
      rs.close();
      statement.commit();
      con.setAutoCommit(true);
      return inlinks;
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return inlinks;
  }

  @Override
  public int getCollectionSize() {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    int collectionSize = 0;
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "CollectionSize");
      stmt = con.getStatement();
      String sql = "SELECT value FROM " + METADATA +
      		         " WHERE key='collection_size'";
      ResultSet rs = stmt.executeQuery(sql);
      
      // if there is a result, it means it is a YAGO entity
      if (rs.next()) {
        String sizeString = rs.getString("value");
        collectionSize = Integer.parseInt(sizeString);        
      } 
      rs.close();
      stmt.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
      logger.error("You might have an outdated entity repository, please " +
      		"download the latest version from the AIDA website. Also check " +
      		"above for other error messages, maybe the connection to the " +
      		"Postgres database is not working properly.");
      throw new IllegalStateException(
          "You might have an outdated entity repository, please " +
              "download the latest version from the AIDA website. Also check " +
              "above for other error messages, maybe the connection to the " +
              "Postgres database is not working properly.");
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return collectionSize;
  }

  @Override
  public TIntObjectHashMap<Type> getTypeNamesForIds(int[] ids) {
    TIntObjectHashMap<Type> typeNames = new TIntObjectHashMap<Type>();
    if (ids.length == 0) {
      return typeNames;
    }
    DBConnection con = null;
    DBStatementInterface stmt = null;
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting Types Names");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(100000);
      String idQuery = YagoUtil.getIdQuery(ids);
      String sql = "SELECT type, knowledgeBase, id FROM " + DataAccessSQL. TYPE_IDS + 
                   " WHERE id IN (" + idQuery + ")";
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        String typeName = rs.getString("type");
        String knowledgeBase = rs.getString("knowledgeBase");
        int id = rs.getInt("id");
        typeNames.put(id, new Type(knowledgeBase,typeName));

        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " type names.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return typeNames;
  }

  @Override
  public TObjectIntHashMap<String> getIdsForTypeNames(Collection<String> typeNames) {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    TObjectIntHashMap<String> typesIds = new TObjectIntHashMap<String>();
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting types Ids");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(100000);
      String idQuery = YagoUtil.getPostgresEscapedConcatenatedQuery(typeNames);
      String sql = "SELECT type, id FROM " + DataAccessSQL.TYPE_IDS + 
                   " WHERE type IN (" + idQuery + ")";
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        String type = rs.getString("type");
        int id = rs.getInt("id");
        typesIds.put(type, id);

        if (++read % 1000000 == 0) {
          logger.info("Read " + read + " types ids.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return typesIds;
  }

  @Override
  public TIntObjectHashMap<int[]> getTypesIdsForEntitiesIds(int[] entitiesIds) {
    TIntObjectHashMap<int[]> typesIds = new TIntObjectHashMap<int[]>();
    for (int entityId : entitiesIds) {
      typesIds.put(entityId, new int[0]);
    }

    DBConnection con = null;
    DBStatementInterface statement = null;
    
    
    String entitiesQuery = StringUtils.join(Util.asIntegerList(entitiesIds), ",");
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      statement = con.getStatement();
      String sql = "SELECT entity, types FROM " + 
                   DataAccessSQL.ENTITY_TYPES + 
                   " WHERE entity IN (" + entitiesQuery + ")";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        Integer[] types = (Integer[]) rs.getArray("types").getArray();
        int entity = rs.getInt("entity");
        typesIds.put(entity, ArrayUtils.toPrimitive(types));
      }
      rs.close();
      statement.commit();
      return typesIds;
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return typesIds;
  }

  @Override
  public TIntObjectHashMap<int[]> getEntitiesIdsForTypesIds(int[] typesIds) {
    TIntObjectHashMap<int[]> entitiesIds = new TIntObjectHashMap<int[]>();
    for (int typeId : typesIds) {
      entitiesIds.put(typeId, new int[0]);
    }

    DBConnection con = null;
    DBStatementInterface statement = null;
    
    
    String typesQuery = StringUtils.join(Util.asIntegerList(typesIds), ",");
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      statement = con.getStatement();
      String sql = "SELECT type, entities FROM " + 
                   DataAccessSQL.ENTITY_TYPES + 
                   " WHERE type IN (" + typesQuery + ")";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        Integer[] entities = (Integer[]) rs.getArray("entities").getArray();
        int type = rs.getInt("type");
        entitiesIds.put(type, ArrayUtils.toPrimitive(entities));
      }
      rs.close();
      statement.commit();
      return entitiesIds;
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return entitiesIds;
  }

  @Override
  public TIntObjectHashMap<int[]> getTypesIdsForEntities(Entities entities) {
    Integer[] entitiesIds = (Integer[]) entities.getUniqueIds().toArray();
    
    return getTypesIdsForEntitiesIds(ArrayUtils.toPrimitive(entitiesIds));
  }

  @Override
  public Map<String, List<String>> getAllEntitiesMetaData(String startingWith){
    Map<String, List<String>> entitiesMetaData = new TreeMap<String, List<String>>();

    DBConnection con = null;
    DBStatementInterface statement = null;
    
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      statement = con.getStatement();
      String sql = "SELECT humanreadablererpresentation, url FROM " + 
                   DataAccessSQL.ENTITY_METADATA + " WHERE humanreadablererpresentation ILIKE '"+startingWith+"%' order by humanreadablererpresentation";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        String humanReadableRepresentation = rs.getString("humanreadablererpresentation");
        String url = rs.getString("url");
        if(entitiesMetaData.containsKey(humanReadableRepresentation)){
          entitiesMetaData.get(humanReadableRepresentation).add(url);
        }else{
          List<String> newList = new ArrayList<String>();
          newList.add(url);
          entitiesMetaData.put(humanReadableRepresentation, newList);
        }
      }
      rs.close();
      statement.commit();
      return entitiesMetaData;
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return entitiesMetaData;
  }
  
  @Override
  public Map<String, EntityMetaData> getEntitiesMetaData(Set<String> entities) {
    if (entities == null || entities.size() == 0) {
      return new HashMap<String, EntityMetaData>();
    }
    TObjectIntHashMap<String> entitiesIds = getIdsForYagoEntityIds(entities);
    TIntObjectHashMap<EntityMetaData> entitiesIdsMetaData = getEntitiesMetaData(entitiesIds.values());
    
    Map<String, EntityMetaData> entitiesMetaData = new HashMap<String, EntityMetaData>();

    for(String entity: entities) {
      int entityId = entitiesIds.get(entity);
      entitiesMetaData.put(entity, entitiesIdsMetaData.get(entityId));
    }
    
    return entitiesMetaData;
  }

  @Override
  public EntityMetaData getEntityMetaData(String entity) {
    Set<String> entities = new HashSet<String>();
    entities.add(entity);
    Map<String, EntityMetaData> results = getEntitiesMetaData(entities);
    return results.get(entity);
  }

  @Override
  public TIntObjectHashMap<EntityMetaData> getEntitiesMetaData(int[] entitiesIds) {
    TIntObjectHashMap<EntityMetaData> entitiesMetaData = new TIntObjectHashMap<EntityMetaData>();
    if (entitiesIds == null || entitiesIds.length == 0) {
      return entitiesMetaData;
    }

    DBConnection con = null;
    DBStatementInterface statement = null;
    
    String entitiesQuery = StringUtils.join(Util.asIntegerList(entitiesIds), ",");
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      statement = con.getStatement();
      String sql = "SELECT entity, humanreadablererpresentation, url FROM " + 
                   DataAccessSQL.ENTITY_METADATA + 
                   " WHERE entity IN (" + entitiesQuery + ")";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        int entity = rs.getInt("entity");
        String humanReadableRepresentation = rs.getString("humanreadablererpresentation");
        String url = rs.getString("url");
        entitiesMetaData.put(entity, new EntityMetaData(entity, humanReadableRepresentation, url));
      }
      rs.close();
      statement.commit();
      return entitiesMetaData;
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return entitiesMetaData;
  }

  @Override
  public EntityMetaData getEntityMetaData(int entityId) {
    int[] entitiesIds = new int[1];
    entitiesIds[0] = entityId;
    TIntObjectHashMap<EntityMetaData> results = getEntitiesMetaData(entitiesIds);
    return results.get(entityId);
  }

  @Override
  public Map<String, Double> getEntitiesImportances(Set<String> entities) {
    TObjectIntHashMap<String> entitiesIds = getIdsForYagoEntityIds(entities);
    TIntDoubleHashMap entitiesIdsImportances = getEntitiesImportances(entitiesIds.values());
    
    Map<String, Double> entitiesImportances = new HashMap<String, Double>();

    for(String entity: entities) {
      int entityId = entitiesIds.get(entity);
      entitiesImportances.put(entity, entitiesIdsImportances.get(entityId));
    }
    
    return entitiesImportances;
  }

  @Override
  public double getEntityImportance(String entity) {
    Set<String> entities = new HashSet<String>();
    entities.add(entity);
    Map<String, Double> results = getEntitiesImportances(entities);
    return results.get(entity);
  }

  @Override
  public TIntDoubleHashMap getEntitiesImportances(int[] entitiesIds) {
    TIntDoubleHashMap entitiesImportances = new TIntDoubleHashMap();
    if (entitiesIds == null || entitiesIds.length == 0) {
      return entitiesImportances;
    }
    
    DBConnection con = null;
    DBStatementInterface statement = null;
    
    String entitiesQuery = StringUtils.join(Util.asIntegerList(entitiesIds), ",");
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      statement = con.getStatement();
      String sql = "SELECT entity, rank FROM " + 
                   DataAccessSQL.ENTITY_RANK + 
                   " WHERE entity IN (" + entitiesQuery + ")";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        int entity = rs.getInt("entity");
        double rank = rs.getDouble("rank");
        entitiesImportances.put(entity, rank);
      }
      rs.close();
      statement.commit();
      return entitiesImportances;
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return entitiesImportances;
  }

  @Override
  public double getEntityImportance(int entityId) {
    int[] entitiesIds = new int[1];
    entitiesIds[0] = entityId;
    TIntDoubleHashMap results = getEntitiesImportances(entitiesIds);
    return results.get(entityId);
  }
  
  @Override
  public double getKeyphraseSourceWeights(String source){
    return getKeyphraseSourceWeights().get(source);
  }
 
  @Override
  public Map<String, Double> getKeyphraseSourceWeights(){
    DBConnection sourceWeightCon = null;
    DBStatementInterface statement = null;
    Map<String, Double> querySourceWeights = new HashMap<String, Double>();
    try {
      sourceWeightCon = 
          AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "keyphrases sources weights");
      statement = sourceWeightCon.getStatement();
      String sql = "SELECT " + KEYPHRASE_SOURCE_WEIGHTS + ".source, " + 
                    KEYPHRASE_SOURCE_WEIGHTS + ".weight FROM " + KEYPHRASE_SOURCE_WEIGHTS;
      
      ResultSet r = statement.executeQuery(sql);
      while (r.next()) {
        String source = r.getString(1);
        double weight = r.getDouble(2);
        querySourceWeights.put(source, weight);
      }      
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
      e.printStackTrace();
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, sourceWeightCon);
    }
    return querySourceWeights;
  }

  @Override
  public String getConfigurationName() {
    String confName = "";
    DBConnection confNameConn = null;
    DBStatementInterface statement = null;
    try {
      confNameConn = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Reading Conf Name");
      statement = confNameConn.getStatement();
      String sql = "SELECT " + METADATA + ".value FROM " + METADATA +
          " WHERE " + METADATA + ".key = 'confName'";

      ResultSet r = statement.executeQuery(sql);
      if (r.next()) {
        confName = r.getString(1);
      }
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
      e.printStackTrace();
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, confNameConn);
    }
    return confName;
  }

  @Override
  public int[] getAllKeywordDocumentFrequencies() {
    DBConnection con = null;
    DBStatementInterface stmt = null;
    TIntIntHashMap keywordCounts = new TIntIntHashMap();
    int maxId = -1;
    try {
      logger.info("Reading keyword counts.");
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Reading keyword counts.");
      con.setAutoCommit(false);
      stmt = con.getStatement();
      stmt.setFetchSize(1000000);
      String sql = "SELECT keyword, count FROM " + KEYWORD_COUNTS;
      ResultSet rs = stmt.executeQuery(sql);
      int read = 0;
      while (rs.next()) {
        int keyword = rs.getInt("keyword");
        int count = rs.getInt("count");
        keywordCounts.put(keyword, count);
        if (keyword > maxId) {
          maxId = keyword;
        }

        if (++read % 1000000 == 0) {
         logger.debug("Read " + read + " keyword counts.");
        }
      }
      con.setAutoCommit(true);
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    
    // Transform hash to int array. This will contain a lot of zeroes as 
    // the keyphrase ids are not part of this (but need to be considered).
    int[] counts = new int[maxId + 1];
    for (TIntIntIterator itr = keywordCounts.iterator(); itr.hasNext(); ) {
      itr.advance();
      int keywordId = itr.key();
      // assert keywordId < counts.length && keywordId > 0 : "Failed for " + keywordId;  // Ids start at 1.
      // actually, keywords should not contain a 0 id, but they do. TODO(mamir,jhoffart).
      assert keywordId < counts.length : "Failed for " + keywordId;  // Ids start at 1.
      counts[keywordId] = itr.value();
    }
    return counts;
  }
}
