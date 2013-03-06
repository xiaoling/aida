package mpi.aida.util.filereading;

import static org.junit.Assert.*;

import org.junit.Test;


public class FileEntriesTest {

  @Test
  public void testRegular() {
    int i = 0;
    for (String line : new FileEntries("testdata/fileentriestest/file.tsv")) {
      switch (i) {
        case 0:
          assertEquals("A\tB\tC", line);
          break;
        case 1:
          assertEquals("D", line);
          break;
        case 2:
          assertEquals("E", line);
          break;
        case 3:
          assertEquals("F\tG", line);
          break;
        case 4:
          assertEquals("H\tIJK\tL", line);
          break;
        case 5:
          assertEquals("MNO", line);
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
    for (String line : new FileEntries("testdata/fileentriestest/oneliner.tsv")) {
      assertEquals("A\tB\tC", line);
      ++i;
    }
    assertEquals(1, i);
  }
}