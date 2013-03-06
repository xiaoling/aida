package mpi.aida.util.filereading;

import static org.junit.Assert.*;

import org.junit.Test;


public class TsvEntriesTest {

  @Test
  public void testRegular() {
    int i = 0;
    for (String[] line : new TsvEntries("testdata/fileentriestest/file.tsv")) {
      switch (i) {
        case 0:
          assertArrayEquals(new String[] {"A", "B", "C" }, line);
          break;
        case 1:
          assertArrayEquals(new String[] { "D" }, line);
          break;
        case 2:
          assertArrayEquals(new String[] { "E" }, line);
          break;
        case 3:
          assertArrayEquals(new String[] { "F", "G" }, line);
          break;
        case 4:
          assertArrayEquals(new String[] { "H", "IJK" , "L" }, line);
          break;
        case 5:
          assertArrayEquals(new String[] { "MNO" }, line);
          break;
        default:
          break;
      }
      ++i;
    }
    assertEquals(6,i);
  }
  
  @Test
  public void testOneliner() {
    int i = 0;
    for (String line[] : new TsvEntries("testdata/fileentriestest/oneliner.tsv")) {
      assertArrayEquals(new String[] { "A", "B", "C" }, line);
      ++i;
    }
    assertEquals(1, i);
  }
}