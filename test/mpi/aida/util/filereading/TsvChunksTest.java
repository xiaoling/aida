package mpi.aida.util.filereading;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import javatools.datatypes.Pair;

import org.junit.Test;


public class TsvChunksTest {
  
  @Test
  public void testRegular() {
    int i = 0;
    for (Pair<String[], List<String[]>> entry : 
          new TsvChunks(
              new File("testdata/fileentriestest/datafile.tsv"), 
              new int[] { 0, 0}, false, 100)) {
      switch (i) {
        case 0:
          assertArrayEquals(new String[] { "A" }, entry.first);
          assertEquals(3, entry.second.size());
          assertArrayEquals(new String[] { "1", "2" }, entry.second.get(0));
          assertArrayEquals(new String[] { "1", "3" }, entry.second.get(1));
          assertArrayEquals(new String[] { "2", "1" }, entry.second.get(2));
          break;
        case 1:
          assertArrayEquals(new String[] { "B" }, entry.first);
          assertEquals(2, entry.second.size());
          assertArrayEquals(new String[] { "1", "1" }, entry.second.get(0));
          assertArrayEquals(new String[] { "2", "1" }, entry.second.get(1));
          break;
        case 2:
          assertArrayEquals(new String[] { "C" }, entry.first);
          assertEquals(1, entry.second.size());
          assertArrayEquals(new String[] { "1", "1" }, entry.second.get(0));
          break;
        case 3:
          assertArrayEquals(new String[] { "D" }, entry.first);
          assertEquals(2, entry.second.size());
          assertArrayEquals(new String[] { "3", "1" }, entry.second.get(0));
          assertArrayEquals(new String[] { "4", "2" }, entry.second.get(1));
          break;
        case 4:
          assertArrayEquals(new String[] { "E" }, entry.first);
          assertEquals(1, entry.second.size());
          assertArrayEquals(new String[] { "1", "1" }, entry.second.get(0));
          break;
        default:
          break;
      }
      ++i;
    }
    assertEquals(5,i);
  }

  @Test
  public void testBackwards() {
    int i = 0;
    for (Pair<String[], List<String[]>> entry : 
          new TsvChunks(
              new File("testdata/fileentriestest/datafile.tsv"), 
              new int[] { -2, -2}, false, 100)) {
      switch (i) {
        case 0:
          assertArrayEquals(new String[] { "1" }, entry.first);
          assertEquals(2, entry.second.size());
          assertArrayEquals(new String[] { "A", "2" }, entry.second.get(0));
          assertArrayEquals(new String[] { "A", "3" }, entry.second.get(1));
          break;
        case 1:
          assertArrayEquals(new String[] { "2" }, entry.first);
          assertEquals(1, entry.second.size());
          assertArrayEquals(new String[] { "A", "1" }, entry.second.get(0));
          break;
        case 7:
          assertArrayEquals(new String[] { "1" }, entry.first);
          assertEquals(1, entry.second.size());
          assertArrayEquals(new String[] { "E", "1" }, entry.second.get(0));
          break;
        default:
          break;
      }
      ++i;
    }
    assertEquals(8,i);
  }

  @Test
  public void testRange() {
    int i = 0;
    for (Pair<String[], List<String[]>> entry : 
          new TsvChunks(
              new File("testdata/fileentriestest/datafile.tsv"), 
              new int[] { 0, 1}, false, 100)) {
      switch (i) {
        case 0:
          assertArrayEquals(new String[] { "A", "1" }, entry.first);
          assertEquals(2, entry.second.size());
          assertArrayEquals(new String[] { "2" }, entry.second.get(0));
          assertArrayEquals(new String[] { "3" }, entry.second.get(1));
          break;
        case 1:
          assertArrayEquals(new String[] { "A", "2" }, entry.first);
          assertEquals(1, entry.second.size());
          assertArrayEquals(new String[] { "1" }, entry.second.get(0));
          break;
        case 7:
          assertArrayEquals(new String[] { "E", "1" }, entry.first);
          assertEquals(1, entry.second.size());
          assertArrayEquals(new String[] { "1" }, entry.second.get(0));
          break;
        default:
          break;
      }
      ++i;
    }
    assertEquals(8,i);
  }
  
  @Test
  public void testInner() {
    int i = 0;
    for (Pair<String[], List<String[]>> entry : 
          new TsvChunks(
              new File("testdata/fileentriestest/datafile.tsv"), 
              new int[] { 1, 1}, false, 100)) {
      switch (i) {
        case 0:
          assertArrayEquals(new String[] { "1" }, entry.first);
          assertEquals(2, entry.second.size());
          assertArrayEquals(new String[] { "A", "2" }, entry.second.get(0));
          assertArrayEquals(new String[] { "A", "3" }, entry.second.get(1));
          break;
        case 1:
          assertArrayEquals(new String[] { "2" }, entry.first);
          assertEquals(1, entry.second.size());
          assertArrayEquals(new String[] { "A", "1" }, entry.second.get(0));
          break;
        case 7:
          assertArrayEquals(new String[] { "1" }, entry.first);
          assertEquals(1, entry.second.size());
          assertArrayEquals(new String[] { "E", "1" }, entry.second.get(0));
          break;
        default:
          break;
      }
      ++i;
    }
    assertEquals(8,i);
  }
}