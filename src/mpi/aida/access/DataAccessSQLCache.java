package mpi.aida.access;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.sql.Array;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mpi.aida.AidaManager;
import mpi.aida.config.AidaConfig;
import mpi.aida.data.Entities;
import mpi.tools.database.DBConnection;
import mpi.tools.database.interfaces.DBStatementInterface;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class DataAccessSQLCache {
    
  private static long entityKeyphrasesCacheHits;
  private static long entityKeyphrasesCacheMisses;
      
  private Logger logger_ = LoggerFactory.getLogger(DataAccessSQLCache.class);
  
  private static class DataAccessSQLCacheHolder {
    public static DataAccessSQLCache cache = new DataAccessSQLCache();
  }
  
  public static DataAccessSQLCache singleton() {
    return DataAccessSQLCacheHolder.cache;
  }
  
  class EntityKeyphraseData {
    final int keyphrase;
    final double weight;
    final Integer[] keyphrase_tokens;
    final Double[] keyphrase_token_weights;
    final String source;
    
    public EntityKeyphraseData(int keyphrase, double weight,
        Integer[] keyphrase_tokens, Double[] keyphrase_token_weights,
        String source) {
      super();
      this.keyphrase = keyphrase;
      this.weight = weight;
      this.keyphrase_tokens = keyphrase_tokens;
      this.keyphrase_token_weights = keyphrase_token_weights;
      this.source = source;
    }
    
    public EntityKeyphraseData(EntityKeyphraseData ekd) {
      super();
      this.keyphrase = ekd.keyphrase;
      this.weight = ekd.weight;
      this.keyphrase_tokens = Arrays.copyOf(ekd.keyphrase_tokens, ekd.keyphrase_tokens.length);
      this.keyphrase_token_weights = Arrays.copyOf(ekd.keyphrase_token_weights, ekd.keyphrase_token_weights.length);
      this.source = ekd.source;    
    }
  }
  
  class CachingHashMap<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 4693725887215865592L;
        
    private int maxEntities_;
    
    public CachingHashMap(int maxEntities) {
      // Initialize with half the maximum capacity.
      super(maxEntities / 2);
      maxEntities_ = maxEntities;
      if (maxEntities > 0) {
        logger_.info("Caching up to " + maxEntities + 
                     " entities per query type");
      }
    }

    @Override
    protected boolean removeEldestEntry(Entry<K, V> eldest) {
      return size() > maxEntities_;
    }    
  }
  
  private Map<String, CachingHashMap<Integer, List<EntityKeyphraseData>>> entityKeyphrasesCaches =
      new HashMap<String, CachingHashMap<Integer, List<EntityKeyphraseData>>>();
  
  public KeyphrasesCache getEntityKeyphrasesCache(Entities entities,
      Map<String, Double> keyphraseSourceWeights, double minKeyphraseWeight,
      int maxEntityKeyphraseCount, boolean useSources) {
    String querySignature = createEntityKeyphrasesQuerySignature(keyphraseSourceWeights, 
        minKeyphraseWeight, maxEntityKeyphraseCount);
    KeyphrasesCache kpc = new KeyphrasesCache();
    CachingHashMap<Integer, List<EntityKeyphraseData>> queryCache =
        entityKeyphrasesCaches.get(querySignature);
    if (queryCache == null) {
      int maxEntities = AidaConfig.getAsInt(AidaConfig.ENTITIES_CACHE_SIZE);
      queryCache = new CachingHashMap<Integer, List<EntityKeyphraseData>>(maxEntities );
      entityKeyphrasesCaches.put(querySignature, queryCache);
    }
    Set<Integer> missingEntities = new HashSet<Integer>();
    for (int eId : entities.getUniqueIds()) {
      List<EntityKeyphraseData> ekds = queryCache.get(eId);
      if (ekds == null) {
        missingEntities.add(eId);
        ++entityKeyphrasesCacheMisses;
      } else {
        // Cache hit.
        for (EntityKeyphraseData ekd : ekds) {
          kpc.add(eId, ekd);
        }
        // Move element to the front so that CachingHashMap can keep track.        
        queryCache.put(eId, ekds);
        ++entityKeyphrasesCacheHits;
      }
    }
    
    logger_.debug("Cache hits/misses :" + entityKeyphrasesCacheHits + "/" + entityKeyphrasesCacheMisses);
    for (Entry<String, CachingHashMap<Integer, List<EntityKeyphraseData>>> c 
        : entityKeyphrasesCaches.entrySet()) {
      logger_.debug("Cache size (" + c.getKey() + "):" + c.getValue().size());
    }

    if (missingEntities.isEmpty()) {
      // All entities are cached.
      return kpc;
    }
    
    DBConnection con = null;
    DBStatementInterface statement = null;
    try {
      con = AidaManager.getConnectionForDatabase(
          AidaManager.DB_AIDA, "Getting keyphrases");
      String entityQueryString = StringUtils.join(missingEntities, ",");
      statement = con.getStatement();
  
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT entity, keyphrase, weight, keyphrase_tokens, " +
                   "keyphrase_token_weights");
      if (useSources) { 
        sql.append(", source");
      }
      sql.append(" FROM ");
      if (maxEntityKeyphraseCount > 0) {
        sql.append("(SELECT ROW_NUMBER() OVER")
           .append(" (PARTITION BY entity ORDER BY weight DESC) AS p,")
           .append(" ek.entity, ek.keyphrase, ek.weight, ek.keyphrase_tokens,")
           .append(" ek.keyphrase_token_weights FROM ")
           .append(DataAccessSQL.ENTITY_KEYPHRASES).append(" ek")
           .append(" WHERE entity IN (").append(entityQueryString).append(")");        
      } else {
        sql.append(DataAccessSQL.ENTITY_KEYPHRASES).append(" WHERE entity IN (")
           .append(entityQueryString).append(")");
      }
      if (useSources) {
        for (Entry<String, Double> sourceWeight : keyphraseSourceWeights.entrySet()) {
          if (sourceWeight.getValue() == 0.0) {
            sql.append(" AND source<>'").append(sourceWeight.getKey()).append("'");
          }
        }
      }
      if (minKeyphraseWeight > 0.0) {
        sql.append(" AND weight > ").append(minKeyphraseWeight);
      }
      // Close nested query when limiting number of keyphrases per entity.
      if (maxEntityKeyphraseCount > 0) {
        sql.append(" ) g WHERE g.p <= ").append(maxEntityKeyphraseCount);        
      }
      
      TIntObjectHashMap<List<EntityKeyphraseData>> entityKeyphrases =
          new TIntObjectHashMap<List<EntityKeyphraseData>>();
      ResultSet rs = statement.executeQuery(sql.toString());
      while (rs.next()) {
        int entityId = rs.getInt("entity");        
        int keyphrase = rs.getInt("keyphrase");
        double keyphraseWeight = rs.getDouble("weight");
        Integer[] tokens = 
            (Integer[]) rs.getArray("keyphrase_tokens").getArray();
        Array tokenWeightsArray = rs.getArray("keyphrase_token_weights");
        Double[] tokenWeights = new Double[0];
        if (tokenWeightsArray != null) {
          tokenWeights = 
            (Double[]) rs.getArray("keyphrase_token_weights").getArray();
        }
        String source = null; 
        if (useSources) {
          source = rs.getString("source");
        }        
              
        EntityKeyphraseData ekd = 
            new EntityKeyphraseData(keyphrase, keyphraseWeight, tokens, 
                tokenWeights, source);
        kpc.add(entityId, ekd);
        
        List<EntityKeyphraseData> entityCache = entityKeyphrases.get(entityId);
        if (entityCache == null) {
          entityCache = new ArrayList<EntityKeyphraseData>();
          entityKeyphrases.put(entityId, entityCache);
        }
        entityCache.add(ekd);        
      }
          
      rs.close();
      statement.commit();

      addToEntityKeyphrasesCache(querySignature, entityKeyphrases);
      
    } catch (Exception e) {
      logger_.error(e.getLocalizedMessage());
    } finally {
      AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
    }    
      
    return kpc;
  }

  private synchronized void addToEntityKeyphrasesCache(
      String querySignature, 
      TIntObjectHashMap<List<EntityKeyphraseData>> entityKeyphrases) {
    CachingHashMap<Integer, List<EntityKeyphraseData>> queryCache = 
        entityKeyphrasesCaches.get(querySignature);
    if (queryCache == null) {
      int maxEntities = AidaConfig.getAsInt(AidaConfig.ENTITIES_CACHE_SIZE);
      queryCache = new CachingHashMap<Integer, List<EntityKeyphraseData>>(maxEntities);
      entityKeyphrasesCaches.put(querySignature, queryCache);      
    }        
    
    for (TIntObjectIterator<List<EntityKeyphraseData>> itr = 
        entityKeyphrases.iterator(); 
        itr.hasNext(); ) {
      itr.advance();
      int entityId = itr.key();
      List<EntityKeyphraseData> keyphrases = itr.value();
      queryCache.put(entityId, keyphrases);
    }
  }

  private String createEntityKeyphrasesQuerySignature(
      Map<String, Double> keyphraseSourceWeights, double minKeyphraseWeight,
      int maxEntityKeyphraseCount) {
    StringBuilder sb = new StringBuilder();
    for (Entry<String, Double> e : keyphraseSourceWeights.entrySet()) {
      sb.append(e.getKey());
      sb.append(":");
      sb.append(e.getValue());
      sb.append("_");
    }
    sb.append(minKeyphraseWeight);
    sb.append("_");
    sb.append(maxEntityKeyphraseCount);
    return sb.toString();
  }
}
