package mpi.aida.util;

import java.io.Reader;

import mpi.tools.basics.Normalize;
import mpi.tools.javatools.filehandlers.FileLines;
import mpi.tools.javatools.parsers.Char;
import mpi.tools.javatools.util.FileUtils;

/**
 * Extracts all article ids from a Wikipedia pages-articles dump.
 * Output format is:
 * article_title<TAB>id
 * 
 *
 */
public class WikipediaDumpArticleIdExtractor {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      printUsage();
      System.exit(1);
    }
    
    final Reader reader = FileUtils.getBufferedUTF8Reader(args[0]);
    String page = FileLines.readBetween(reader, "<page>", "</page>");
    
    int pagesDone = 0;
    
    while (page != null) {
      if (++pagesDone % 100000 == 0) {
        System.err.println(pagesDone + " pages done.");
      }
      
      page = Char.decodeAmpersand(page.replace("&amp;", "&"));
      String title = FileLines.readBetween(page, "<title>", "</title>");
      String id = FileLines.readBetween(page, "<id>", "</id>");
      System.out.println(Normalize.entity(title) + "\t" + id);
      
      page = FileLines.readBetween(reader, "<page>", "</page>");
    }
  }

  public static void printUsage() {
    System.out.println("Usage:");
    System.out.println("\tWikipediaDumpArticleIdExtractor <wikipedia-pages-articles.xml>");
  }
}