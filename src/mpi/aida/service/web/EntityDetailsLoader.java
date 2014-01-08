package mpi.aida.service.web;

import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import mpi.aida.AidaManager;
import mpi.aida.access.DataAccess;
import mpi.aida.data.Type;
import mpi.aida.graph.similarity.measure.WeightComputation;
import mpi.tools.database.DBConnection;
import mpi.tools.database.interfaces.DBStatementInterface;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings("unchecked")
public class EntityDetailsLoader {

  public static JSONArray loadKeyphrases(String entityName) {
		Map<String, JSONObject> keyphrases = new LinkedHashMap<String, JSONObject>();
		String entity = entityName;//YagoUtil.getPostgresEscapedString(entityName);
		int totalCollectionSize = DataAccess.getCollectionSize();
		String sql = "select u.word keyphrase, u.source source, wi.word keyword, kc.count "
				+ "from ( "
				+ "select w.word word, kp.source source, unnest(kp.keyphrase_tokens) token_id, kp.weight weight "
				+ "from entity_keyphrases as kp, word_ids as w, entity_ids as e "
				+ "where e.entity = '"
				+ entity
				+ "' and e.id = kp.entity and w.id = kp.keyphrase order by source "
				+ ") as u, word_ids as wi, keyword_counts as kc where u.token_id = wi.id and u.token_id = kc.keyword";

		DBConnection con = null;
		DBStatementInterface statement = null;
		try {
			con = AidaManager.getConnectionForDatabase(AidaManager.DB_AIDA,
					"YN");
			statement = con.getStatement();
			ResultSet rs = statement.executeQuery(sql);
			while (rs.next()) {
				String keyphrase = rs.getString("keyphrase");
				String source = rs.getString("source");
				String keyword = rs.getString("keyword");
				int keywordCount = rs.getInt("count");
				JSONObject kp = keyphrases.get(keyphrase);
				if (kp == null) {
					kp = new JSONObject();
					kp.put("keyphrase", keyphrase);
					kp.put("source", source);
					kp.put("keywordsWeights", new JSONArray());
					keyphrases.put(keyphrase, kp);
				}
				double idf = 0.0;
				if (keywordCount > 0) {
					idf = WeightComputation.log2((double) totalCollectionSize
							/ (double) keywordCount);
				}
				JSONArray keywordWeights = (JSONArray) kp
						.get("keywordsWeights");
				JSONObject keywordJson = new JSONObject();
				keywordJson.put("keyword", keyword);
				keywordJson.put("weight", idf);
				keywordWeights.add(keywordJson);
			}
			rs.close();
			statement.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			AidaManager.releaseConnection(AidaManager.DB_AIDA, con);
		}

		JSONArray keyphrasesJSONArray = new JSONArray();
		for (String kpName : keyphrases.keySet()) {
			keyphrasesJSONArray.add(keyphrases.get(kpName));
		}
		return keyphrasesJSONArray;
	}
	
	public static JSONArray loadEntityTypes(String entity) {
	  Set<Type> entityTypes = DataAccess.getTypes(entity);
		
		JSONArray entityTypesJson = new JSONArray();
		for(Type type: entityTypes) {
			JSONObject entityType = new JSONObject();
			entityType.put("name", type.getName());
			entityType.put("knowledgebase", type.getKnowledgeBase());
			entityTypesJson.add(entityType);
		}
		
		return entityTypesJson;
	}
}
