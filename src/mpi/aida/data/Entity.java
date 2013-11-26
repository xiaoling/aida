package mpi.aida.data;

import java.io.Serializable;
import java.util.List;

import mpi.tools.javatools.parsers.Char;

public class Entity implements Serializable, Comparable<Entity>, Cloneable {

  private static final long serialVersionUID = 131444964369556633L;

  private String name;
  
  private List<String> surroundingMentionNames;

  private int id = -1;

  
  public static final String OOKBE = "--NME--";

  /**
   * Use this field to represent the mention-entity similarity computed with 
   * some method (not the score stored in the DB). This field will not be set 
   * in the constructor. We set it later on, when we compute the similarity
   */
  private double mentionEntitySimilarity;

  public Entity(String name, int id) {
    this.name = name;
    this.mentionEntitySimilarity = -1.0;
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return name + " (" + id + ")";
  }

  public String tohtmlString() {
    return "<td></td><td></td><td>" + Char.toHTML(name) + "</td><td></td><td></td><td></td>";
  }

  public int getId() {
    return id;
  }

  public double getMentionEntitySimilarity() {
    return this.mentionEntitySimilarity;
  }

  public void setMentionEntitySimilarity(double mes) {
    this.mentionEntitySimilarity = mes;
  }

  public int compareTo(Entity e) {
    return name.compareTo(e.getName());
  }
  
  public boolean equals(Object o) {
    if (o instanceof Entity) {
      Entity e = (Entity) o;
      return name.equals(e.getName());
    } else {
      return false;
    }
  }
  
  public int hashCode() {
    return name.hashCode();
  }

  public boolean isOOKBentity() {
    return Entities.isOokbeName(name);
  }

  public String getNMEnormalizedName() {
    String normName = name.replace("-"+OOKBE, "").replace(' ', '_');
    return normName;
  }

  public List<String> getSurroundingMentionNames() {
    return surroundingMentionNames;
  }

  public void setSurroundingMentionNames(List<String> surroundingMentionNames) {
    this.surroundingMentionNames = surroundingMentionNames;
  }
}
