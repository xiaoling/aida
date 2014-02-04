package mpi.experiment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import mpi.aida.data.Mention;
import mpi.aida.data.Mentions;
import mpi.aida.data.PreparedInput;
import mpi.experiment.reader.CoNLLReader;
import mpi.experiment.reader.CollectionReader.CollectionPart;
import mpi.experiment.reader.CollectionReaderSettings;
import mpi.tools.javatools.datatypes.Pair;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class EvaluateWikifier {
	public static double rankThresh = 0;
	public static double linkThresh = 0.05;

	public static void main(String[] args) {
		if (args.length == 2) {
			linkThresh = Double.parseDouble(args[0]);
			rankThresh = Double.parseDouble(args[1]);
		}
		// createComparePages();
		String aidaPath = "/homes/gws/xiaoling/dataset/nel/AIDA/aida-yago2-dataset/";
		// String aidaPath = "doc/";
		String wikifierPath = "/projects/pardosa/data12/xiaoling/workspace/wikifier/data/aida_output/";
		CollectionReaderSettings settings = new CollectionReaderSettings();
		settings.setIncludeOutOfDictionaryMentions(true);
		settings.setKeepSpaceBeforePunctuations(true);
		CoNLLReader reader = new CoNLLReader(aidaPath, CollectionPart.TEST,
				settings);

		int tp = 0, fp = 0, fn = 0;
		int tp2 = 0, fp2 = 0, fn2 = 0;
		for (PreparedInput inputDoc : reader) {
			String id = inputDoc.getDocId();
			Mentions mentions = inputDoc.getMentions();
			Map<Pair<Integer, Integer>, String> gold = new HashMap<Pair<Integer, Integer>, String>();
			for (Mention mention : mentions.getMentions()) {
				gold.put(
						new Pair<Integer, Integer>(mention.getCharOffset(),
								mention.getCharOffset()
										+ mention.getCharLength()),
						mention.getNer().substring(
								"http://en.wikipedia.org/wiki/".length()));
			}
			String id2 = id.substring(0, id.indexOf("testb"));
			createHtmlFromAnnotation(id2, gold);

			Map<Pair<Integer, Integer>, String> result = readWikifierOutput(wikifierPath
					+ id2 + ".wikification.tagged.full.xml");

			System.out.println("==========");
			System.out.println("doc " + id2);
			// BOC precision
			HashSet<String> goldConcepts = new HashSet<String>(gold.values());
			HashSet<String> predConcepts = new HashSet<String>(result.values());
			System.out.println("GOLD:" + goldConcepts);
			System.out.println("PRED:" + predConcepts);
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
			if (fn2 > fn) {
				System.out.println(id + "," + fn2 + "," + fn);
			}
			
			// AIDA precision
			for (Pair<Integer, Integer> pos : gold.keySet()) {
				if (result.containsKey(pos)
						&& result.get(pos).equalsIgnoreCase(gold.get(pos))) {
					tp++;
					result.remove(pos);
				} else {
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
			System.out.println("====" + linkThresh + "===" + rankThresh
					+ "====");
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

	private static void createHtmlFromAnnotation(String id2,
			Map<Pair<Integer, Integer>, String> gold) {
		try {
			List<Pair<Integer, Integer>> posList = new ArrayList<Pair<Integer, Integer>>(
					gold.keySet());
			Collections.sort(posList, new Comparator<Pair<Integer, Integer>>() {

				@Override
				public int compare(Pair<Integer, Integer> o1,
						Pair<Integer, Integer> o2) {
					return o2.first - o1.first;
				}

			});
			StringBuilder sb = new StringBuilder(FileUtils.readFileToString(
					new File(
							"/homes/gws/xiaoling/dataset/nel/AIDA/aida-yago2-dataset/rawText/"
									+ id2), "UTF-8"));
			for (int i = 0; i < posList.size(); i++) {
				int first = posList.get(i).first + 1;
				int end = posList.get(i).second + 1;
				sb.insert(end, "</a>");
				sb.insert(
						first,
						"<a href=http://en.wikipedia.org/wiki/"
								+ gold.get(posList.get(i)) + ">");
			}

			FileUtils.writeStringToFile(new File(
					"/homes/gws/xiaoling/dataset/nel/AIDA/aida-yago2-dataset/html/"
							+ id2 + ".html"),
					sb.toString().replace("\n", "<br/>\n"), "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void createComparePages() {
		String[] files = new File(
				"/homes/gws/xiaoling/dataset/nel/AIDA/aida-yago2-dataset/rawText/")
				.list();
		for (String file : files) {
			String id = file;
			String html = "<!DOCTYPE html>\n"
					+ "<html>\n"
					+ "<body>\n"
					+ "<iframe src=\"http://homes.cs.washington.edu/~xiaoling/aida/gold/"
					+ id
					+ ".html\" width=49% height=600>\n"
					+ "  <p>Your browser does not support iframes.</p>\n"
					+ "</iframe>\n"
					+ "<iframe src=\"http://homes.cs.washington.edu/~xiaoling/aida/wikifier/"
					+ id
					+ ".wikification.tagged.flat.html\" width=49% height=600>\n"
					+ " <p>Your browser does not support iframes.</p>\n"
					+ "</iframe>\n" + "</body>\n" + "</html>\n";
			try {
				FileUtils.writeStringToFile(new File(
						"/homes/gws/xiaoling/public_html/aida/comp/" + id
								+ ".html"), html);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static Map<Pair<Integer, Integer>, String> readWikifierOutput(
			String filename) {
		try {
			File fXmlFile = new File(filename);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("Entity");
			Map<Pair<Integer, Integer>, String> result = new HashMap<Pair<Integer, Integer>, String>();
			Map<Integer, Pair<Integer, Integer>> costarts = new HashMap<Integer, Pair<Integer, Integer>>();
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					String surfaceForm = eElement
							.getElementsByTagName("EntitySurfaceForm").item(0)
							.getTextContent();
					int start = Integer.parseInt(eElement
							.getElementsByTagName("EntityTextStart").item(0)
							.getTextContent());
					int end = Integer.parseInt(eElement
							.getElementsByTagName("EntityTextEnd").item(0)
							.getTextContent());
					double linkerScore = Double.parseDouble(eElement
							.getElementsByTagName("LinkerScore").item(0)
							.getTextContent());
					if (linkerScore < linkThresh
							|| containsNoUpperCase(surfaceForm)) {
						continue;
					}
					Pair<Integer, Integer> pos = new Pair<Integer, Integer>(
							start - 1, end - 1);
					if (costarts.containsKey(start - 1)) {
						int end2 = costarts.get(start - 1).second;
						if (end > end2) {
							// use the longer span
							result.remove(costarts.get(start - 1));
							costarts.put(start - 1, pos);
						} else {
							continue;
						}
					} else {
						costarts.put(start - 1, pos);
					}
					double score = Double.parseDouble(eElement
							.getElementsByTagName("TopDisambiguation").item(0)
							.getChildNodes().item(5).getTextContent());
					String entity = eElement
							.getElementsByTagName("TopDisambiguation").item(0)
							.getChildNodes().item(1).getTextContent();
					if (score > rankThresh)
						result.put(pos, entity);
				}
			}
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static boolean containsNoUpperCase(String str) {
		for (char c : str.toCharArray()) {
			if (Character.isUpperCase(c)) {
				return false;
			}
		}
		return true;
	}
}
