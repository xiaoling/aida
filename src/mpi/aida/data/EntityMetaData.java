package mpi.aida.data;

import java.io.Serializable;

public class EntityMetaData implements Serializable{

  private static final long serialVersionUID = -5254220574529910760L;

  private int id;

  private String humanReadableRepresentation;

  private String url;

  public EntityMetaData(int id, String humanReadableRepresentation, String url) {
    super();
    this.id = id;
    this.humanReadableRepresentation = humanReadableRepresentation;
    this.url = url;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getHumanReadableRepresentation() {
    return humanReadableRepresentation;
  }

  public void setHumanReadableRepresentation(String humanReadableRepresentation) {
    this.humanReadableRepresentation = humanReadableRepresentation;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

}
