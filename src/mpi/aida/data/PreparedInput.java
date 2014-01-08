package mpi.aida.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mpi.aida.access.DataAccess;
import mpi.aida.util.filereading.FileEntries;
import mpi.tokenizer.data.Token;
import mpi.tokenizer.data.Tokens;
import mpi.tools.javatools.datatypes.Pair;
import mpi.tools.javatools.util.FileUtils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PreparedInput implements Iterable<PreparedInputChunk> {
  
  private Logger logger_ = LoggerFactory.getLogger(PreparedInput.class);
  
  private String docId_;
  
  private List<PreparedInputChunk> chunks_;

  private static Pattern p = Pattern.compile("-DOCSTART- \\((.*?)\\)");
    
  /**
   * Timestamp (at midnight) of when this document was published. May be empty.
   */
  private long timestamp_;
  
  private Set<String> punctuations_ = getPuncuations();
  
  public PreparedInput(String docId, List<PreparedInputChunk> chunks) {
    docId_ = docId;
    chunks_ = chunks;
  }

  public String getDocId() {
    return docId_;
  }

  public int getMentionSize() {
    int mentionSize = 0;
    for (PreparedInputChunk c : this) {
      mentionSize += c.getMentions().getMentions().size();
    }
    return mentionSize;
  }
  
  public int getChunksCount() {
    return chunks_.size();
  }
  
  public Tokens getTokens() {
    Tokens allTokens = new Tokens();
    for (PreparedInputChunk c : this) {
      for (Token t : c.getTokens()) {
        allTokens.addToken(t);     
      }
    }
    return allTokens;
  }
  
  public Mentions getMentions() {
    Mentions allMentions = new Mentions();
    for (PreparedInputChunk c : this) {
      for (Mention m : c.getMentions().getMentions()) {
        allMentions.addMention(m);        
      }
    }
    return allMentions;
  }

  @Override
  public Iterator<PreparedInputChunk> iterator() {
    return chunks_.iterator();
  }

  public void setMentionEntitiesTypes(Set<Type> filteringTypes) {
    for (PreparedInputChunk c : this) {
      c.getMentions().setEntitiesTypes(filteringTypes);
    }
  }
  
  /**
   * Loads the necessary information from a file in AIDA-collection-format.
   * 
   * @param file  File in AIDA collection format.
   */
  public PreparedInput(File file) {
    this(file, 0, true);
  }
    
  /**
   * Loads the necessary information from a file in AIDA-collection-format, 
   * discarding mentions with less than the given minimum occrurence count.
   * 
   * @param file  File in AIDA collection format.
   * @param mentionMinOccurrences Minimum number of occurrences a mention must have to be included
   *                              (must be present in data)
   * @param inludeOODMentions Set to false to drop all mentions that are not in the dictionary.                             
   */
  public PreparedInput(File file, int mentionMinOccurrences, boolean inludeOODMentions) {
    Pair<PreparedInputChunk, Long> loaded = 
        loadFrom(file, mentionMinOccurrences, inludeOODMentions);
    docId_ = loaded.first.getChunkId();
    chunks_ = new ArrayList<PreparedInputChunk>(1);
    chunks_.add(loaded.first);
    timestamp_ = loaded.second;
  }
  
  private Pair<PreparedInputChunk, Long> loadFrom(File f, int mentionMinOccurrences, boolean includeOutOfDictionaryMentions) {
    String docId = null;
    Tokens tokens = null;
    Mentions mentions = null;
    long timestamp = 0;
    // Helpers.
    boolean first = true;
    int sentence = 0;
    int position = -1;
    int index = 0;
    for (String line : new FileEntries(f)) {
      if (first) {
        // Read metadata.
        if (!line.startsWith("-DOCSTART-")) {
          logger_.error("Invalid input format, first line has to start with " +
              "-DOCSTART-");
        } else {
          // Parse metadata.
          String[] data = line.split("\t");
          Matcher m = p.matcher(data[0]);
          if (m.find()) {
            // Initialize datastructures.
            docId = m.group(1);          
            tokens = new Tokens();
            mentions = new Mentions();
            // Read time if it exists.
            if (data.length > 1) {
              String[] dateParts = data[1].split("-");
              timestamp = new DateTime(
                  Integer.parseInt(dateParts[0]), 
                  Integer.parseInt(dateParts[1]), 
                  Integer.parseInt(dateParts[2]), 
                  0, 0, DateTimeZone.UTC).getMillis();
            }
          } else {
            logger_.error("Could not find docid in " + line);
          }
        }
        first = false;
      } else {
        // Read document line by line
        if (line.length() == 0) {
          sentence++;          
          continue;
        }
        String[] data = line.split("\t");
        position++;       
        boolean mentionStart = false;
        String word = null;
        String textMention = null;
        String entity = null;
        String ner = null;
        int mentionOccurrenceCount = 0;
        if (data.length == 0) {
          logger_.warn("Line length 0 for doc id " + docId);
        } 
        // Simple token.
        if (data.length >= 1) {
          word = data[0];
        }
        // Mention.
        if (data.length >= 4) {
          mentionStart = "B".equals(data[1]);
          textMention = data[2];
          entity = data[3];
        }
        // Mention with Stanford ner label.
        if (data.length >= 5) {
          word = data[0];
          mentionStart = "B".equals(data[1]);
          textMention = data[2];
          entity = data[3];
          ner = data[4];
        } 
        if (data.length >= 6) {
          mentionOccurrenceCount = Integer.parseInt(data[5]);
        }
        if ((data.length >= 2 && data.length <= 3) ||
            data.length >= 7) {
          logger_.warn("Line has wrong format: '" + line + "' for docId " + docId);
        }

        if (punctuations_.contains(word) && tokens.size() > 0) {
          Token at = tokens.getToken(tokens.size() - 1);
          at.setOriginalEnd("");
          index = index - 1;
        }
        int endIndex = index + word.length();
        Token at = new Token(position, word, " ", index, endIndex, sentence, 0, null, ner);
        tokens.addToken(at);
        if (textMention != null && mentionStart && 
            (mentionOccurrenceCount >= mentionMinOccurrences)) {
          Mention mention = new Mention();
          mention.setCharOffset(index);
          mention.setCharLength(textMention.length());
          mention.setMention(textMention);
          mention.setGroundTruthResult(entity);
          mention.setOccurrenceCount(mentionOccurrenceCount);
          mentions.addMention(mention);
        } 
        index = endIndex + 1;
      }
    }
    // TODO(jhoffart): what about OOKBE?
    if (!includeOutOfDictionaryMentions) {
      Map<String, Entities> candidates = 
          DataAccess.getEntitiesForMentions(mentions.getMentionNames(), 1.0);
      Mentions mentionsToInclude = new Mentions();
      for (Mention m : mentions.getMentions()) {
        Entities cands = candidates.get(m.getMention());
        if (!cands.isEmpty()) {
          mentionsToInclude.addMention(m);
        }
      }
      mentions = mentionsToInclude;
    }
    if (tokens != null) {
      List<String> content = new LinkedList<String>();
      for (int p = 0; p < tokens.size(); p++) {
        Token token = tokens.getToken(p);
        content.add(token.getOriginal());
      }
      setTokensPositions(mentions, tokens);
    }
    PreparedInputChunk prepInput = new PreparedInputChunk(docId, tokens, mentions); 
    return new Pair<PreparedInputChunk, Long>(prepInput, timestamp);
  }
  
  /**
   * Assumes a PreparedInput with only a single chunk. Multi-Chunk documents
   * should never be stored.
   * 
   * @param writer
   * @throws IOException
   */
  public void writeTo(BufferedWriter writer) throws IOException {
    writer.write("-DOCSTART- (");
    writer.write(docId_);
    writer.write(")");
    if (timestamp_ != 0) {
      DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
      String timeString = fmt.print(timestamp_);
      writer.write("\t" + timeString);
    }
    writer.newLine();
    int currentToken = 0;
    if (chunks_.size() > 1 || chunks_.size() == 0) {
      throw new IllegalStateException("AIDA disk formats do not support "
          + "chunked documents. This document contains " + chunks_.size() + 
          "cunks.");
    }
    PreparedInputChunk chunk = chunks_.get(0);
    for (Mention mention : chunk.getMentions().getMentions()) {
      // Write up to mention.
      writeTokens(chunk.getTokens(), currentToken, mention.getStartToken(), writer);
      currentToken = mention.getEndToken() + 1;
      // Add mention.
      writeTokensMention(chunk.getTokens(), mention, writer);
    }
    writeTokens(chunk.getTokens(), currentToken, chunk.getTokens().size(), writer);
  }
  
  public void writeTo(File file) throws IOException {
    BufferedWriter writer = FileUtils.getBufferedUTF8Writer(file);
    writeTo(writer);
    writer.flush();
    writer.close();
  }
  
  private void writeTokens(Tokens tokens, int from, int to, BufferedWriter writer) throws IOException {
    for (int i = from; i < to; i++) {
      if (i > 0 && tokens.getToken(i - 1).getSentence() != tokens.getToken(i).getSentence()) {
        writer.newLine();
      }
      writer.write(tokens.getToken(i).getOriginal());
      writer.newLine();
    }
  }

  private void writeTokensMention(Tokens tokens, Mention mention, BufferedWriter writer) throws IOException {
    String start = "B";
    for (int i = mention.getStartToken(); i <= mention.getEndToken(); i++) {
      if (i > 0 && tokens.getToken(i - 1).getSentence() != tokens.getToken(i).getSentence()) {
        writer.newLine();
      }
      if (mention.getGroundTruthResult() == null) {
        mention.setGroundTruthResult("--UNKNOWN--");
      }
      String NE = (tokens.getToken(i).getNE() != null) ? tokens.getToken(i).getNE() : "NULL";     
      String line = tokens.getToken(i).getOriginal() + "\t" + start + 
                    "\t" + mention.getMention() + 
                    "\t" + mention.getGroundTruthResult() +
                    "\t" + NE;
      if (mention.getOccurrenceCount() > 0) {
        line += "\t" + mention.getOccurrenceCount();
      }
      writer.write(line);
      writer.newLine();
      start = "I";
    }
  }
  
  private void setTokensPositions(Mentions mentions, Tokens tokens) {
    int startToken = -1;
    int endToken = -1;
    int t = 0;
    int i = 0;
    Mention mention = null;
    Token token = null;
    while (t < tokens.size() && i < mentions.getMentions().size()) {
      mention = mentions.getMentions().get(i);
      token = tokens.getToken(t);
      if (startToken >= 0) {
        if (token.getEndIndex() > mention.getCharOffset() + mention.getCharLength()) {
          mention.setStartToken(startToken);
          mention.setId(startToken);
          mention.setEndToken(endToken);
          if (mention.getMention() == null) {
            mention.setMention(tokens.toText(startToken, endToken));
          }
          startToken = -1;
          endToken = -1;
          i++;
        } else {
          endToken = token.getId();
          t++;
        }
      } else {
        if (token.getBeginIndex() >= mention.getCharOffset() && mention.getCharOffset() <= token.getEndIndex()) {
          startToken = token.getId();
          endToken = token.getId();
        } else {
          t++;
        }
      }
    }
    if (startToken >= 0) {
      if (token.getEndIndex() >= mention.getCharOffset() + mention.getCharLength()) {
        mention.setStartToken(startToken);
        mention.setId(startToken);
        mention.setEndToken(endToken);
      }
    }
  }
  
  private Set<String> getPuncuations() {
    HashSet<String> punctuations = null;
    punctuations = new HashSet<String>();
    punctuations.add(".");
    punctuations.add(":");
    punctuations.add(",");
    punctuations.add(";");
    punctuations.add("!");
    punctuations.add("?");
    punctuations.add("'s");
    return punctuations;
  }
  

  public long getTimestamp() {
    return timestamp_;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp_ = timestamp;
  }  
}
