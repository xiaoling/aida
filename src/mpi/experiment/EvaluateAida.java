package mpi.experiment;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import mpi.tools.javatools.datatypes.Pair;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class EvaluateAida {
	private static JSONParser parser = new JSONParser();
	public static Map<String, String> redirects = null;
	static {
		List<String> lines;
		try {
			redirects = new HashMap<String, String>();
			lines = FileUtils.readLines(new File("redirects.evaluation"));
			for (String line : lines) {
				String[] pair = line.split("\t");
				redirects.put(pair[0], pair[1]);
			}
			lines = FileUtils.readLines(new File("redirects.aida.evaluation"));
			for (String line : lines) {
				String[] pair = line.split("\t");
				redirects.put(pair[0], pair[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Map<String, String> getStats(String filename) {
		try {
			Map<String, String> stats = new HashMap<String, String>();
			List<String> lines = FileUtils.readLines(new File(filename));
			for (int i = 0; i < lines.size(); i++) {
				if (lines.get(i).startsWith("Saving the full")) {
					String key = lines.get(i).substring(
							lines.get(i).indexOf(" to ") + 4);
					key = key.substring(key.indexOf("//") + 2);
					StringBuilder sb = new StringBuilder(lines.get(i - 3)
							.replaceAll("\\p{Alpha}", ""));
					sb.append("/");
					sb.append(lines.get(i - 2).replaceAll("\\p{Alpha}", ""));
					sb.append("/");
					sb.append(lines.get(i - 1).replaceAll("\\p{Alpha}", ""));
					stats.put(key, sb.toString());
				}
			}
			return stats;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	static Map<String, String> stats = new HashMap<String, String>();

	public static void main(String[] args) {
		EvaluateWikifier.linkThresh = -0.00;
		EvaluateWikifier.rankThresh = -1.00;
//		Map<String, String> statsW = getStats("/projects/pardosa/data12/xiaoling/workspace/wikifier/output/AblationResults/FULL.xml/"
//				 + "Ace");
//		evaluateWikifier("ACE", true);
//		for (String key : statsW.keySet()) {
//			if (!statsW.get(key).equals(stats.get(key))) {
//				System.out.println("FILE = " + key);
//				System.out.println("WIKIFIER:" + statsW.get(key));
//				System.out.println("MINE:" + stats.get(key));
//			}
//		}
//		System.exit(0);

		StringBuilder sb = new StringBuilder();
		sb.append("W-A\t");
		sb.append(evaluateWikifier("ACE", true));
		sb.append("\t");
		sb.append(evaluateWikifier("MSNBC", true));
		sb.append("\t");
		sb.append(evaluateWikifier("AQUAINT", true));
		sb.append("\t");
		sb.append(evaluateWikifier("WIKI", true));
		sb.append("\n");

		sb.append("A-A\t");
		sb.append(evaluateAida("ACE", true));
		sb.append("\t");
		sb.append(evaluateAida("MSNBC", true));
		sb.append("\t");
		sb.append(evaluateAida("AQUAINT", true));
		sb.append("\t");
		sb.append(evaluateAida("WIKI", true));
		sb.append("\n");
//		try {
//			FileUtils.writeLines(new File("aida.pred.entities"), stats.keySet());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.exit(0);
		
		sb.append("W-N\t");
		sb.append(evaluateWikifier("ACE", false));
		sb.append("\t");
		sb.append(evaluateWikifier("MSNBC", false));
		sb.append("\t");
		sb.append(evaluateWikifier("AQUAINT", false));
		sb.append("\t");
		sb.append(evaluateWikifier("WIKI", false));
		sb.append("\n");

		sb.append("A-N\t");
		sb.append(evaluateAida("ACE", false));
		sb.append("\t");
		sb.append(evaluateAida("MSNBC", false));
		sb.append("\t");
		sb.append(evaluateAida("AQUAINT", false));
		sb.append("\t");
		sb.append(evaluateAida("WIKI", false));
		sb.append("\n");

		System.out.println("==========================");
		System.out.println(sb.toString());
	}

	public static double evaluateAida(String dataset,
			boolean useNonNamedEntities) {
		String dir = null, dir2 = null, repl = null;
		switch (dataset) {
		case "ACE":
			dir = "data/ace/";
			dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/ACE2004_Coref_Turking/Dev/ProblemsNoTranscripts/";
			repl = ".txt.json";
			break;
		case "AQUAINT":
			dir = "data/aquaint/";
			dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/AQUAINT/Problems/";
			repl = ".txt.json";
			break;
		case "MSNBC":
			dir = "data/msnbc/";
			dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/MSNBC/Problems/";
			repl = ".json";
			break;
		case "WIKI":
			dir = "data/wiki/";
			dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/WikipediaSample/ProblemsTest/";
			repl = ".txt.json";
			break;
		}
		String[] files = new File(dir).list();
		int tp = 0, fp = 0, fn = 0;
		int tp2 = 0, fp2 = 0, fn2 = 0;
		int empty = 0;
		for (String file : files) {
			if (file.endsWith(".json")) {
				System.out.println("=====" + file + "==========");
				Map<Pair<Integer, Integer>, String> result = readResultFromJson(dir
						+ file);
				for (Pair key : result.keySet()) {
					String val = result.get(key);
					if (redirects.containsKey(val)){
						result.put(key, redirects.get(val));
					}
				}
				
				Map<Pair<Integer, Integer>, String> gold = readGoldFromWikifier(
						dir2 + file.replace(repl, ""), useNonNamedEntities);
				if (dir.contains("msnbc")) {
					for (Pair key : gold.keySet()) {
						gold.put(key, gold.get(key).replace(" ", "_"));
					}
				}
				if (gold.isEmpty()) {
					empty++;
					continue;
				}
				System.out.println("GOLD:" + gold);
				System.out.println("PRED:" + result);
//				for (String r: result.values()) {
//					stats.put(r, "");
//				}
				// BOC precision
				HashSet<String> goldConcepts = new HashSet<String>(
						gold.values());
				HashSet<String> predConcepts = new HashSet<String>();
				System.out.println("GOLD2:" + goldConcepts);
				for (Pair<Integer, Integer> pair : result.keySet()) {
					if (gold.containsKey(pair)) {
						predConcepts.add(result.get(pair));
					}
				}
				System.out.println("PRED2:" + predConcepts);
				int oldTp = tp2, oldFn = fn2, oldFp = fp2;
				for (String concept : goldConcepts) {
					if (predConcepts.contains(concept)) {
						tp2++;
						predConcepts.remove(concept);
					} else {
						fn2++;
						System.out.println("[FN2]" + concept);
					}
				}
				for (String concept : predConcepts) {
					fp2++;
					System.out.println("[FP2]" + concept);
				}
				System.out.println(String.format("[INC]tp=%d, fp=%d, fn=%d",
						tp2 - oldTp, fp2 - oldFp, fn2 - oldFn));
				// AIDA precision
				for (Pair<Integer, Integer> pos : gold.keySet()) {
					boolean found = false;
					for (Pair<Integer, Integer> pos2 : result.keySet()) {
						if (pos.equals(pos2)
								&& result.get(pos2).equalsIgnoreCase(
										gold.get(pos))) {
							tp++;
							result.remove(pos2);
							found = true;
							break;
						}
					}
					if (!found) {
						fn++;
						System.out.println("[FN]" + pos + " => "
								+ gold.get(pos));
					}
				}

				fp += result.size();
				for (Pair<Integer, Integer> pos : result.keySet()) {
					System.out.println("[FP]" + pos + " => " + result.get(pos));
				}
			}
		}
		{
			System.out.println("======" + dir + "==empty:" + empty + "===");
			double prec = (double) tp / (tp + fp);
			double rec = (double) tp / (tp + fn);
			double f1 = 2 * prec * rec / (prec + rec);
			System.out.println(String.format("prec=%.3f\trec=%.3f\tf1=%.3f",
					prec, rec, f1));
			System.out.println(String.format("tp = %d, fp = %d, fn = %d", tp,
					fp, fn));
		}
		double res = 0;
		{
			System.out.println("=====BOC=====");
			double prec = (double) tp2 / (tp2 + fp2);
			double rec = (double) tp2 / (tp2 + fn2);
			double f1 = 2 * prec * rec / (prec + rec);
			res = f1;
			System.out.println(String.format("prec=%.3f\trec=%.3f\tf1=%.3f",
					prec, rec, f1));
			System.out.println(String.format("tp2 = %d, fp2 = %d, fn2 = %d",
					tp2, fp2, fn2));
		}
		return res;
	}

	public static double evaluateWikifier(String dataset,
			boolean useNonNamedEntities) {
		String dir = null, dir2 = null;
		switch (dataset) {
		case "ACE":
			dir = "/projects/pardosa/data12/xiaoling/workspace/wikifier/output/FULL/ACE/";
			dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/ACE2004_Coref_Turking/Dev/ProblemsNoTranscripts/";
			break;
		case "AQUAINT":
			dir = "/projects/pardosa/data12/xiaoling/workspace/wikifier/output/AQUAINT/";
			dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/AQUAINT/Problems/";
			break;
		case "MSNBC":
			dir = "/projects/pardosa/data12/xiaoling/workspace/wikifier/output/MSNBC/";
			dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/MSNBC/Problems/";
			break;
		case "WIKI":
			dir = "/projects/pardosa/data12/xiaoling/workspace/wikifier/output/Wikipedia/";
			dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/WikipediaSample/ProblemsTest/";
			break;
		}
		String repl = ".tagged.full.xml";

		String[] files = new File(dir).list();
		int tp = 0, fp = 0, fn = 0;
		int tp2 = 0, fp2 = 0, fn2 = 0;
		int empty = 0;
		for (String file : files) {
			// String file = "Pol16452612.txt.tagged.full.xml";
			if (!file.endsWith(repl)) {
				continue;
			}
			Map<Pair<Integer, Integer>, String> gold = readGoldFromWikifier(
					dir2 + file.replace(repl, ""), useNonNamedEntities);
			Map<Pair<Integer, Integer>, String> result0 = EvaluateWikifier
					.readWikifierOutput(dir + file);
			Map<Pair<Integer, Integer>, String> result = new HashMap<Pair<Integer, Integer>, String>();
			for (Pair<Integer, Integer> pair : result0.keySet()) {
				Pair<Integer, Integer> pair2 = new Pair<Integer, Integer>(
						pair.first + 1, pair.second + 1);
				result.put(pair2, result0.get(pair));
			}
			System.out.println("=====" + file + "==========");
			System.out.println("GOLD:" + gold);
			System.out.println("PRED:" + result);

			// BOC precision
			HashSet<String> goldConcepts = new HashSet<String>(gold.values());
			HashSet<String> predConcepts = new HashSet<String>();
			HashSet<String> visitedNullEntities = new HashSet<String>();
			int oldTp = tp2, oldFn = fn2, oldFp = fp2;
			for (Pair<Integer, Integer> pair : result.keySet()) {
				if (gold.containsKey(pair)) {
					String label = gold.get(pair);
					if (label.equals("*null*")) {
						if (!visitedNullEntities.contains(result.get(pair))) {
							fp2++;
							System.out.println("[FP2]" + result.get(pair)
									+ "(*null*)");
							visitedNullEntities.add(result.get(pair));
						} 
					} else {
						predConcepts.add(result.get(pair));
					}
				}
			}
			goldConcepts.remove("*null*");
			System.out.println("GOLD2:" + goldConcepts);
			System.out.println("PRED2:" + predConcepts);

			for (String concept : goldConcepts) {
				if (predConcepts.contains(concept)) {
					tp2++;
					predConcepts.remove(concept);
				} else {
					fn2++;
					System.out.println("[FN2]" + concept);
				}
			}
			for (String concept : predConcepts) {
				fp2++;
				System.out.println("[FP2]" + concept);
			}
			System.out.println(String.format("[INC]tp=%d, fp=%d, fn=%d", tp2
					- oldTp, fp2 - oldFp, fn2 - oldFn));
//			{
//				stats.put(
//						file,
//						String.format("%d/%d/%d", tp2 - oldTp, fp2 - oldFp, tp2
//								- oldTp + fn2 - oldFn));
//			}
			// AIDA precision
			for (Pair<Integer, Integer> pos : gold.keySet()) {
				boolean found = false;
				for (Pair<Integer, Integer> pos2 : result.keySet()) {
					if (pos.equals(pos2)
							&& result.get(pos2).equalsIgnoreCase(gold.get(pos))) {
						tp++;
						result.remove(pos2);
						found = true;
						break;
					}
				}
				if (!found) {
					fn++;
					// System.out.println("[FN]" + pos + " => " +
					// gold.get(pos));
				}
			}

			fp += result.size();
			for (Pair<Integer, Integer> pos : result.keySet()) {
				// System.out.println("[FP]" + pos + " => " + result.get(pos));
			}
		}
		{
			System.out.println("======" + dir + "==empty:" + empty + "===");
			double prec = (double) tp / (tp + fp);
			double rec = (double) tp / (tp + fn);
			double f1 = 2 * prec * rec / (prec + rec);
			System.out.println(String.format("prec=%.3f\trec=%.3f\tf1=%.3f",
					prec, rec, f1));
			System.out.println(String.format("tp = %d, fp = %d, fn = %d", tp,
					fp, fn));
		}
		double res = 0;
		{
			System.out.println("=====BOC=====");
			double prec = (double) tp2 / (tp2 + fp2);
			double rec = (double) tp2 / (tp2 + fn2);
			double f1 = 2 * prec * rec / (prec + rec);
			res = f1;
			System.out.println(String.format("prec=%.3f\trec=%.3f\tf1=%.3f",
					prec, rec, f1));
			System.out.println(String.format("tp2 = %d, fp2 = %d, fn2 = %d",
					tp2, fp2, fn2));
		}
		return res;
	}

	public static Map<Pair<Integer, Integer>, String> readGoldFromWikifier(
			String filename, boolean useNonNamedEntities) {
		List<String> lines = null;
		try {
			lines = FileUtils
					.readLines(new File(filename),
							(filename.contains("Wiki") || filename
									.contains("AQUAINT")) ? "utf-8"
									: "Windows-1252");
		} catch (IOException e) {
			e.printStackTrace();
		}
		int offset = -1, length = -1;
		String label = null;
		String surfaceForm = null;
		int state = 0;
		Map<Pair<Integer, Integer>, String> gold = new HashMap<Pair<Integer, Integer>, String>();
		for (String line : lines) {
			if (state > 0) {
				switch (state) {
				case 1:
					offset = Integer.parseInt(line.trim());
					break;
				case 2:
					length = Integer.parseInt(line.trim());
					break;
				case 3:
					label = line.trim();
					// if (label.startsWith("http")) {
					// label = label.replace("http://en.wikipedia.org/wiki/",
					// "");
					// }
					// if (label.endsWith("\"") &&
					// StringUtils.countMatches(label, "\"")==1)
					// label = label.substring(0, label.length()-1);
					if (redirects.containsKey(label)) {
						label = redirects.get(label);
					}
					break;
				case 4:
					surfaceForm = line.trim();
					break;
				default:
					break;
				}
				state = 0;
			}
			if (line.trim().equals("<Offset>")) {
				state = 1;
			} else if (line.trim().equals("<Length>")) {
				state = 2;
			} else if (line.trim().equals("<ChosenAnnotation>")) {
				state = 3;
			} else if (line.trim().equals("<SurfaceForm>")) {
				state = 4;
			} else if (line.trim().equals("</ReferenceInstance>")) {
				state = 0;
				if (!label.equals("none")
						&& !label.equals("---")
						// && !label.equals("*null*")
						&& (useNonNamedEntities || !EvaluateWikifier
								.containsNoUpperCase(surfaceForm))) {
					gold.put(new Pair(offset, offset + length), label);
				}
			}
		}
		return gold;
	}

	public static Map<Pair<Integer, Integer>, String> readResultFromJson(
			String filename) {
		try {
			JSONObject obj = (JSONObject) parser.parse(FileUtils
					.readFileToString(new File(filename)));
			JSONObject metadata = (JSONObject) obj.get("entityMetadata");
			HashMap<Long, String> map = new HashMap<Long, String>();
			for (Object key : metadata.keySet()) {
				map.put(Long.parseLong((String) key),
						(String) ((JSONObject) metadata.get(key)).get("url"));
			}
			JSONArray mentions = (JSONArray) obj.get("mentions");
			Map<Pair<Integer, Integer>, String> result = new HashMap<Pair<Integer, Integer>, String>();
			for (Object mention : mentions) {
				JSONObject m = ((JSONObject) mention);
				JSONObject pred = (JSONObject) m.get("bestEntity");
				if (pred == null)
					continue;
				long id = (Long) pred.get("id");
				long start = (Long) m.get("offset");
				long end = start + (Long) m.get("length");
				result.put(new Pair((int) start, (int) end), map.get(id)
						.substring("http://en.wikipedia.org/wiki/".length()));
			}
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
