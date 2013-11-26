package mpi.tools.basics;

import mpi.tools.javatools.parsers.Char;

/**
 * Class Normalize
 * 
 * Transforms strings to valid YAGO entities
 * 
 * @author Fabian M. Suchanek
 */
public class Normalize {
	/** Normalizes an entity */
	public static String entity(String s) {
		return (Char.encodeBackslashToAlphanumeric(s.trim().replace(' ', '_')));
	}

	/** Returns the readable string for an entity */
	public static String unEntity(String s) {
		return (Char.decodeBackslash(s).replace('_', ' '));
	}

	/** Normalizes a string */
	public static String string(String s) {
		return ('"' + Char.encodeBackslashToASCII(s.trim()).replace("\"", "\\u0022") + '"');
	}

	/** Un-Normalizes a string */
	public static String unString(String s) {
		if (!s.startsWith("\""))
			return (null);
		return (Char.decodeBackslash(stripQuotes(s)));
	}
	
  public static String stripQuotes(String s) {
    if (s.startsWith("\"")) s = s.substring(1);
    if (s.endsWith("\"")) s = s.substring(0, s.length() - 1);
    return (s);
  }

	/** Normalizes a fact id */
	public static String factId(long myFactId) {
		return ("#" + myFactId);
	}
  
  /** Un-Normalizes a fact id  
   * @return numeric value of the fact id if the given string is a normalized fact id, null otherwise
   */public static Long unFactId(String normalizedFactId){
     if(normalizedFactId.matches("#\\d++"))
       return new Long(normalizedFactId.substring(1));
     else return null;
   }

	/** Normalizes a wordnet entity */
	public static String wordNetEntity(String word, String id) {
		return ("wordnet_" + entity(word) + "_" + id);
	}

	/** Normalizes a wordnet entity */
	public static String wikiCategory(String wikiCat) {
		return ("wikicategory_" + entity(wikiCat));
	}

	/** Returns the name of the wordnet Entity (or null) */
	public static String unWordNetEntity(String wn) {
		if (!wn.startsWith("wordnet_"))
			return (null);
		wn = wn.substring("wordnet_".length());
		wn = wn.substring(0, wn.lastIndexOf('_'));
		return (unEntity(wn));
	}

	/** Returns the name of the wiki category (or null) */
	public static String unWikiCategory(String wn) {
		if (!wn.startsWith("wikicategory_"))
			return (null);
		wn = wn.substring("wikicategory_".length());
		return (unEntity(wn));
	}

	/** Returns the readable name of an entity */
	public static String unNormalize(String s) {
		if (s == null)
			return ("NULL");
		
		// check all formats, make sure that the surface form
		// of each potential normalizer is different!
		
		String clean = unWikiCategory(s);
		if (clean != null)
			return (clean);
		
		clean = unString(s);
		if (clean != null)
			return (clean);
		
		clean = unWordNetEntity(s);
		if (clean != null)
			return (clean);
		
		clean = unGeonamesEntity(s);
		if (clean != null)
			return (clean);
		
		clean = unGeonamesClass(s);
		if (clean != null) 
		  return (clean);
		
		return (unEntity(s));
	}

	/** Returns the readable name of an entity */
  public static String fullUnNormalize(String s) {
    if (s == null)
      return ("NULL");
    String clean = unWikiCategory(s);
    if (clean != null) {
      s = clean;
    }
    clean = unString(s);
    if (clean != null) {
      s = clean;
    }
    clean = unWordNetEntity(s);
    if (clean != null) {
      s = clean;
    }
    clean = unGeonamesEntity(s);
    if (clean != null) {
      s = clean;
    }
    clean = unGeonamesClass(s);
    if (clean != null) {
      s = clean;
    }
    return (unEntity(s));
  }

	/** Normalizes a geonames entitiy */
	public static String geonamesEntity(String word, String id) {
		return ("geoent_" + entity(word) + "_" + id);
	}

	/** Returns the name of a geonames entity (or null) */
	public static String unGeonamesEntity(String entity) {
		if (!entity.startsWith("geoent_"))
			return (null);
		entity = entity.substring("geoent_".length());
		entity = entity.substring(0, entity.lastIndexOf('_'));
		return (unEntity(entity));
	}

	/** Normalizes a geonames class */
	public static String geonamesClass(String name) {
		return ("geoclass_" + entity(name));
	}

	/** Returns the name of a geonames class (or null) */
	public static String unGeonamesClass(String entity) {
		if (!entity.startsWith("geoclass_"))
			return (null);
		entity = entity.substring("geoclass_".length());
		return (unEntity(entity));
	}

	/** Returns the wikipedia URL for an entity */
	public static String urlForEntity(String e) {
		if (e.startsWith("wiki_") || e.startsWith("wordnet_"))
			return (null);
		if (e.startsWith("wikicategory_")) {
			return ("http://en.wikipedia.org/wiki/Category:" + Char.encodeURIPathComponent(e.substring(13)));
		}
		return ("http://en.wikipedia.org/wiki/" + Char.encodeURIPathComponent(e));
	}
}
