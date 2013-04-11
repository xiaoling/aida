package mpi.aida.data;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import javatools.util.FileUtils;

import mpi.aida.config.AidaConfig;

import org.junit.Test;


public class PreparedInputTest {

  public PreparedInputTest() {
    AidaConfig.set("dataAccess", "testing");
  }
  
  @Test
  public void testLoadWrite() throws IOException {
    File orig = new File("testdata/preparedinput/preparedinputtest.tsv");
    String origContent = FileUtils.getFileContent(orig);    
    PreparedInput prep = new PreparedInput(orig);
    File tmpFile = File.createTempFile("test", "tmp");
    tmpFile.deleteOnExit();
    prep.writeTo(tmpFile);
    String tmpContent = FileUtils.getFileContent(tmpFile);
    assertEquals(origContent, tmpContent);
  } 
}
