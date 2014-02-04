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

	public static void main(String[] args) {
//		test();
//		System.exit(0);
//		String dir = "data/ace/";
//		String dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/ACE2004_Coref_Turking/Dev/ProblemsNoTranscripts/";
//		String repl = ".txt.json";

		 String dir = "data/msnbc/";
		 String dir2 =
		 "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/MSNBC/Problems/";
		 String repl = ".json";

//		 String dir = "data/wiki/";
//		 String dir2 =
//		 "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/WikipediaSample/ProblemsTest/";
//		 String repl = ".txt.json";

//		 String dir = "data/aquaint/";
//		 String dir2 =
//		 "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/AQUAINT/Problems/";
//		 String repl = ".txt.json";

		String[] files = new File(dir).list();
		int tp = 0, fp = 0, fn = 0;
		int tp2 = 0, fp2 = 0, fn2 = 0;
		int empty = 0;
		for (String file : files) {
			if (file.endsWith(".json")) {
				System.out.println("=====" + file + "==========");
				Map<Pair<Integer, Integer>, String> result = readResultFromJson(dir
						+ file);
				Map<Pair<Integer, Integer>, String> gold = readGoldFromWikifier(dir2
						+ file.replace(repl, ""));
				if (gold.isEmpty()) {
					empty++;
					continue;
				}
				System.out.println("GOLD:" + gold);
				System.out.println("PRED:" + result);

				// BOC precision
				HashSet<String> goldConcepts = new HashSet<String>(
						gold.values());
				HashSet<String> predConcepts = new HashSet<String>();
				System.out.println("GOLD2:" + goldConcepts);
				System.out.println("PRED2:" + predConcepts);
				for (Pair<Integer, Integer> pair : result.keySet()) {
					if (gold.containsKey(pair)) {
						predConcepts.add(result.get(pair));
					}
				}
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
		{
			System.out.println("=====BOC=====");
			double prec = (double) tp2 / (tp2 + fp2);
			double rec = (double) tp2 / (tp2 + fn2);
			double f1 = 2 * prec * rec / (prec + rec);
			System.out.println(String.format("prec=%.3f\trec=%.3f\tf1=%.3f",
					prec, rec, f1));
			System.out.println(String.format("tp2 = %d, fp2 = %d, fn2 = %d",
					tp2, fp2, fn2));
		}
	}

	private static void test() {
		String dir = "/projects/pardosa/data12/xiaoling/workspace/wikifier/output/FULL/ACE/";
		String dir2 = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/WikificationACL2011Data/ACE2004_Coref_Turking/Dev/ProblemsNoTranscripts/";
		String repl = ".tagged.full.xml";

		String[] files = new File(dir).list();
		int tp = 0, fp = 0, fn = 0;
		int tp2 = 0, fp2 = 0, fn2 = 0;
		int empty = 0;
		for (String file : files) {
			// String file = "20001115_AFP_ARB.0072.eng";
			if (!file.endsWith(repl)) {
				continue;
			}
			Map<Pair<Integer, Integer>, String> gold = readGoldFromWikifier(dir2
					+ file.replace(repl, ""));
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
			for (Pair<Integer, Integer> pair : result.keySet()) {
				if (gold.containsKey(pair)) {
					predConcepts.add(result.get(pair));
				}
			}
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
					System.out.println("[FN]" + pos + " => " + gold.get(pos));
				}
			}

			fp += result.size();
			for (Pair<Integer, Integer> pos : result.keySet()) {
				System.out.println("[FP]" + pos + " => " + result.get(pos));
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
		{
			System.out.println("=====BOC=====");
			double prec = (double) tp2 / (tp2 + fp2);
			double rec = (double) tp2 / (tp2 + fn2);
			double f1 = 2 * prec * rec / (prec + rec);
			System.out.println(String.format("prec=%.3f\trec=%.3f\tf1=%.3f",
					prec, rec, f1));
			System.out.println(String.format("tp2 = %d, fp2 = %d, fn2 = %d",
					tp2, fp2, fn2));
		}
	}

	public static Map<Pair<Integer, Integer>, String> readGoldFromWikifier(
			String filename) {
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
		int state = 0;
		Map<Pair<Integer, Integer>, String> gold = new HashMap<Pair<Integer, Integer>, String>();
		for (String line : lines) {
			if (state > 0) {
				switch (state) {
				case 1:
					offset = Integer.parseInt(line.trim());
				case 2:
					length = Integer.parseInt(line.trim());
				case 3:
					label = line.trim();
					if (label.startsWith("http")) {
						label = label.replace("http://en.wikipedia.org/wiki/",
								"");
					}
				default:
					state = 0;
				}
			}
			if (line.trim().equals("<Offset>")) {
				state = 1;
			}
			if (line.trim().equals("<Length>")) {
				state = 2;
			}
			if (line.trim().equals("<Annotation>")) {
				state = 3;
			}
			if (line.trim().equals("</ReferenceInstance>")) {
				state = 0;
				if (!label.equals("none") && !label.equals("---")) {
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
