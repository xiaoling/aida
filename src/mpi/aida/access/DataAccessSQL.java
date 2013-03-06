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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpi.aida.AidaManager;
import mpi.aida.data.Entities;
import mpi.aida.data.Entity;
import mpi.aida.data.Keyphrases;
import mpi.aida.graph.similarity.PriorProbability;
import mpi.aida.util.YagoUtil;
import mpi.aida.util.YagoUtil.Gender;
import mpi.database.DBConnection;
import mpi.database.interfaces.DBStatementInterface;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import basics.Normalize;

public class DataAccessSQL implements DataAccessInterface {
  private static final Logger logger = 
      LoggerFactory.getLogger(DataAccessSQL.class);
  
  public static final String ENTITY_KEYPHRASES = "entity_keyphrases";
  public static final String ENTITY_IDS = "entity_ids";
  public static final String NAME_IDS = "named_ids";
  public static final String WORD_IDS = "word_ids";
  public static final String WORD_EXPANSION = "word_expansion";
  public static final String KEYPHRASE_COUNTS = "keyphrase_counts";
  public static final String KEYWORD_COUNTS = "keyword_counts";
  public static final String ENTITY_COUNTS = "entity_counts";
  public static final String ENTITY_KEYWORDS = "entity_keywords";
  public static final String ENTITY_LSH_SIGNATURES = "entity_lsh_signatures_2000";
  public static final String ENTITY_INLINKS = "entity_inlinks";
  public static final String ENTITY_RANK = "entity_rank";
  public static final String DICTIONARY = "dictionary";
  public static final String YAGO_FACTS = "facts";
  public static final String METADATA = "meta";

  
  @Override
  public DataAccess.type getAccessType() {
    return DataAccess.type.sql;
  }

  @Override
  public Entities getEntitiesForMention(String mention, double maxEntiyRank) {
    mention = PriorProbability.conflateMention(mention);
    Entities entities = new Entities();
    DBConnection mentionEntityCon = null;
    DBStatementInterface statement = null;
    TIntHashSet entityIds = new TIntHashSet();
    try {
      mentionEntityCon = 
          AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "mentions for entity");
      statement = mentionEntityCon.getStatement();
      String sql = null;
      String normMention = 
          YagoUtil.getPostgresEscapedString(Normalize.string(mention));
      if (maxEntiyRank < 1.0) {
        sql = "SELECT " + DICTIONARY + ".entity FROM " + DICTIONARY + 
            " JOIN " + ENTITY_RANK +
            " ON " + DICTIONARY + ".entity=" + ENTITY_RANK + ".entity" +
            " WHERE mention = E'" + normMention + "'" +
            " AND rank<" + maxEntiyRank; 
      } else {
        sql = "SELECT entity FROM " + DICTIONARY + 
              " WHERE mention = E'" + normMention + "'";
      }
      ResultSet r = statement.executeQuery(sql);
      while (r.next()) {
        int entity = r.getInt("entity");
        entityIds.add(entity);
      }
      int[] ids = entityIds.toArray();
      TIntObjectHashMap<String> yagoEntityIds = getYagoEntityIdsForIds(ids);
      for (int i = 0; i < ids.length; ++i) {
        entities.add(new Entity(yagoEntityIds.get(ids[i]), ids[i]));
      }
      r.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, mentionEntityCon);
    }
    
    return entities;
  }

  @Override
  public Keyphrases getEntityKeyphrases(
      Entities entities, String keyphraseSourceExclusion) {
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
    
    if (entities == null | entities.getUniqueNames().size() == 0) {            
      return keyphrases;
    }

    DBConnection con = null;
    DBStatementInterface statement = null;
    
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting keyphrases");
      String entityQueryString = StringUtils.join(entities.getUniqueIds(), ",");
      statement = con.getStatement();

      String sql = "SELECT entity, keyphrase, weight, " +
      		         "keyphrase_tokens" +
      		         ",keyphrase_token_weights" +
                   " FROM " + ENTITY_KEYPHRASES +
                   " WHERE entity IN (" + entityQueryString + ")";
      if (keyphraseSourceExclusion != null) {
        sql += " AND source<>'" + keyphraseSourceExclusion + "'";
      }
      ResultSet rs = statement.executeQuery(sql);
      TIntObjectHashMap<TIntHashSet> eKps = 
          new TIntObjectHashMap<TIntHashSet>();
      for (Entity e : entities) {
        eKps.put(e.getId(), new TIntHashSet());
      }
      while (rs.next()) {
        int entity = rs.getInt("entity");
        int keyphrase = rs.getInt("keyphrase");
        double keyphraseWeight = rs.getDouble("weight");
        
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
        Integer[] tokens = 
            (Integer[]) rs.getArray("keyphrase_tokens").getArray();
        Double[] tokenWeights = 
            (Double[]) rs.getArray("keyphrase_token_weights").getArray();
        TIntDoubleHashMap keyword2mi = entity2keyword2mi.get(entity);
        if (keyword2mi == null) {
          keyword2mi = new TIntDoubleHashMap();
          entity2keyword2mi.put(entity, keyword2mi);
        }
        if (!keyphraseTokens.containsKey(keyphrase)) {
          int[] tokenIds = new int[tokens.length];
          for (int i = 0; i < tokens.length; ++i) {
            tokenIds[i] = tokens[i];
            keyword2mi.put(tokenIds[i], tokenWeights[i]);
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
    
    return keyphrases;
  }

  @Override
  public void getEntityKeyphraseTokens(
      Entities entities, String keyphraseSourceExclusion, 
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
      if (keyphraseSourceExclusion != null) {
        sql += " AND source<>'" + keyphraseSourceExclusion + "'";
      }
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
      logger.error(e.getLocalizedMessage());
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

  public List<String> getTypes(String entity) {
    Set<String> entities = new HashSet<String>();
    entities.add(entity);
    Map<String, List<String>> results = getTypes(entities);
    return results.get(entity);
  }

  public Map<String, List<String>> getTypes(Set<String> entities) {
    DBConnection con = null;
    Map<String, List<String>> entityTypes = new HashMap<String, List<String>>();
    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      for (String entity : entities) {
        entityTypes.put(entity, new LinkedList<String>());
      }
      DBStatementInterface statement = con.getStatement();
      String entitiesQuery = YagoUtil.getPostgresEscapedConcatenatedQuery(entities);
      String sql = "SELECT arg1,arg2 FROM class_hierarchy_dc_20120110 " + 
                   "WHERE arg1 IN (" + entitiesQuery + ") " + "AND relation='type'";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        String entity = rs.getString("arg1");
        String type = rs.getString("arg2");
        List<String> types = entityTypes.get(entity);
        types.add(type);
      }
      rs.close();
      statement.commit();
    } catch (SQLException e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      if (con != null) {
        AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
      }
    }
    return entityTypes;
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
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "YN");
      DBStatementInterface statement = con.getStatement();
      String sql = "SELECT arg2 FROM class_hierarchy_dc_20120110 " + "WHERE arg1=E'" + YagoUtil.getPostgresEscapedString(queryType) + "' " + "AND relation='subclassOf'";
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
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
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
  public Map<String, List<String>> getEntityKeyphrases(Set<String> entities) {
    DBConnection con = null;
    DBStatementInterface statement = null;

    Map<String, List<String>> entityKeyphrases = new HashMap<String, List<String>>();

    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting entity-keyphrases");
      String entityQueryString = YagoUtil.getPostgresEscapedConcatenatedQuery(entities);
      statement = con.getStatement();

      String sql = "SELECT entity,keyphrase FROM entity_keyphrases WHERE entity IN (" + entityQueryString + ")";

      ResultSet rs = statement.executeQuery(sql);
      for (String e : entities) {
        entityKeyphrases.put(e, new LinkedList<String>());
      }
      while (rs.next()) {
        String entity = rs.getString("entity");
        String keyphrase = rs.getString("keyphrase");
        List<String> kps = entityKeyphrases.get(entity);
        kps.add(keyphrase);
      }
      rs.close();
      statement.commit();
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }

    return entityKeyphrases;
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
  public Map<Entity, int[]> getEntityLSHSignatures(Entities entities) {
    return getEntityLSHSignatures(entities, ENTITY_LSH_SIGNATURES);
  }

  @Override
  public Map<Entity, int[]> getEntityLSHSignatures(Entities entities, String table) {
    Map<Entity, int[]> entitySignatures = new HashMap<Entity, int[]>();
    Map<String, int[]> tmpEntitySignatures = new HashMap<String, int[]>();
//    TIntObjectHashMap<int[]> tmpEntitySignatures = 
//        new TIntObjectHashMap<int[]>();
    
    DBConnection con = null;
    DBStatementInterface statement = null;

    try {
      con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA, "Getting entity-keyphrases");
      String entityQueryString = YagoUtil.getPostgresEscapedConcatenatedQuery(entities.getUniqueNames());
//      String entityQueryString = StringUtils.join(entities.getUniqueIds(), ",");
      statement = con.getStatement();
      
      // TODO runtime of LSH will be really slow, batch create entities from IDs
      String sql = "SELECT entity, signature FROM " + table + " WHERE entity IN (" + entityQueryString + ")";
      ResultSet rs = statement.executeQuery(sql);
      while (rs.next()) {
        String entity = rs.getString("entity");
        int[] sig = org.apache.commons.lang.ArrayUtils.toPrimitive((Integer[]) rs.getArray("signature").getArray());
        tmpEntitySignatures.put(entity, sig);        
      }
      // Transform all entities at once.
      TObjectIntHashMap<String> name2id = DataAccess.getIdsForYagoEntityIds(tmpEntitySignatures.keySet());
      for (String name : tmpEntitySignatures.keySet()) {
        entitySignatures.put(new Entity(name, name2id.get(name)), tmpEntitySignatures.get(name));
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
      con.setAutoCommit(false);
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
          AidaManager.DB_AIDA, "Checking YAGO entity");
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
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
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
      logger.error("You probably have an outdated entity repository, please" +
      		" download the latest version from the AIDA website.");
      throw new IllegalStateException(
          "You probably have an outdated entity repository, please" +
          " download the latest version from the AIDA website.");
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }
    return collectionSize;
  }
}
