package mpi.aida.util;
// filename: ExternalSort.java
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
* Goal: offer a generic external-memory sorting program in Java.
* 
* It must be : 
*  - hackable (easy to adapt)
*  - scalable to large files
*  - sensibly efficient.
*
* This software is in the public domain.
*
* Usage: 
*  java com/google/code/externalsorting/ExternalSort somefile.txt out.txt
* 
* You can change the default maximal number of temporary files with the -t flag:
*  java com/google/code/externalsorting/ExternalSort somefile.txt out.txt -t 3
*
* For very large files, you might want to use an appropriate flag to allocate
* more memory to the Java VM: 
*  java -Xms2G com/google/code/externalsorting/ExternalSort somefile.txt out.txt
*
* By (in alphabetical order) 
*   Philippe Beaudoin,  Jon Elsas,  Christan Grant, Daniel Haran, Daniel Lemire, 
*  April 2010
* originally posted at 
*  http://www.daniel-lemire.com/blog/archives/2010/04/01/external-memory-sorting-in-java/
*/
public class ExternalSort {
	
	static int DEFAULTMAXTEMPFILES = 1024;
	 
	// we divide the file into small blocks. If the blocks
	// are too small, we shall create too many temporary files. 
	// If they are too big, we shall be using too much memory. 
	public static long estimateBestSizeOfBlocks(File filetobesorted, int maxtmpfiles) {
		long sizeoffile = filetobesorted.length() * 2;
		/**
		* We multiply by two because later on someone insisted on counting the memory
		* usage as 2 bytes per character. By this model, loading a file with 1 character
		* will use 2 bytes.
		*/ 
		// we don't want to open up much more than maxtmpfiles temporary files, better run
		// out of memory first.
		long blocksize = sizeoffile / maxtmpfiles + (sizeoffile % maxtmpfiles == 0 ? 0 : 1) ;
		
		// on the other hand, we don't want to create many temporary files
		// for naught. If blocksize is smaller than half the free memory, grow it.
		long freemem = Runtime.getRuntime().freeMemory();
		if( blocksize < freemem/2) {
		    blocksize = freemem/2;
		} 
		return blocksize;
	}

	/**
	 * This will simply load the file by blocks of x rows, then
	 * sort them in-memory, and write the result to  
	 * temporary files that have to be merged later.
	 * 
	 * @param file some flat  file
	 * @param cmp string comparator 
	 * @return a list of temporary flat files
	 */
	public static List<File> sortInBatch(File file, Comparator<String> cmp) throws IOException {		return sortInBatch(file, cmp,DEFAULTMAXTEMPFILES,Charset.defaultCharset(),null);	}
	
	

	/**
	 * This will simply load the file by blocks of x rows, then
	 * sort them in-memory, and write the result to 
	 * temporary files that have to be merged later. You can
	 * specify a bound on the number of temporary files that
	 * will be created.
	 * 
	 * @param file some flat  file
	 * @param cmp string comparator 
	 * @param maxtmpfiles maximal number of temporary files
	 * @param Charset character set to use  (can use Charset.defaultCharset()) 
   * @param tmpdirectory location of the temporary files (set to null for default location)
	 * @return a list of temporary flat files
	 */
	public static List<File> sortInBatch(File file, Comparator<String> cmp, int maxtmpfiles, Charset cs, File tmpdirectory) throws IOException {
		List<File> files = new ArrayList<File>();
		BufferedReader fbr = new BufferedReader(new InputStreamReader(new FileInputStream(file),cs));
		long blocksize = estimateBestSizeOfBlocks(file,maxtmpfiles);// in bytes

		try{
			List<String> tmplist =  new ArrayList<String>();
			String line = "";
			try {
				while(line != null) {
					long currentblocksize = 0;// in bytes
					while((currentblocksize < blocksize) 
					&&(   (line = fbr.readLine()) != null) ){ // as long as you have enough memory
						tmplist.add(line);
						currentblocksize += line.length() * 2; // java uses 16 bits per character?
					}
					files.add(sortAndSave(tmplist,cmp,cs,tmpdirectory));
					tmplist.clear();
				}
			} catch(EOFException oef) {
				if(tmplist.size()>0) {
					files.add(sortAndSave(tmplist,cmp,cs,tmpdirectory));
					tmplist.clear();
				}
			}
		} finally {
			fbr.close();
		}
		return files;
	}

     /**
     * Sort a list and save it to a temporary file 
     *
     * @return the file containing the sorted data
     * @param tmplist data to be sorted
     * @param cmp string comparator
     * @param cs charset to use for output (can use Charset.defaultCharset())
     * @param tmpdirectory location of the temporary files (set to null for default location)
     */ 
	public static File sortAndSave(List<String> tmplist, Comparator<String> cmp, Charset cs, File tmpdirectory) throws IOException  {
		Collections.sort(tmplist,cmp);  
		File newtmpfile = File.createTempFile("sortInBatch", "flatfile", tmpdirectory);
		newtmpfile.deleteOnExit();
		BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newtmpfile),cs));
		try {
			for(String r : tmplist) {
				fbw.write(r);
				fbw.newLine();
			}
		} finally {
			fbw.close();
		}
		return newtmpfile;
	}
	
	/**
	 * This merges a bunch of temporary flat files 
	 * @param files
	 * @param output file
     * @return The number of lines sorted. (P. Beaudoin)
	 */
	public static int mergeSortedFiles(List<File> files, File outputfile, final Comparator<String> cmp) throws IOException {
		return mergeSortedFiles(files, outputfile, cmp,Charset.defaultCharset()); 
	}

	/**
	 * This merges a bunch of temporary flat files 
	 * @param files
	 * @param output file
	 * @param Charset character set to use to load the strings
     * @return The number of lines sorted. (P. Beaudoin)
	 */
	public static int mergeSortedFiles(List<File> files, File outputfile, final Comparator<String> cmp, Charset cs) throws IOException {
		PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(11, 
            new Comparator<BinaryFileBuffer>() {
              public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
                return cmp.compare(i.peek(), j.peek());
              }
            }
        );
		for (File f : files) {
			BinaryFileBuffer bfb = new BinaryFileBuffer(f,cs);
			pq.add(bfb);
		}
		BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfile),cs));
		int rowcounter = 0;
		try {
			while(pq.size()>0) {
				BinaryFileBuffer bfb = pq.poll();
				String r = bfb.pop();
				fbw.write(r);
				fbw.newLine();
				++rowcounter;
				if(bfb.empty()) {
					bfb.fbr.close();
					bfb.originalfile.delete();// we don't need you anymore
				} else {
					pq.add(bfb); // add it back
				}
			}
		} finally { 
			fbw.close();
			for(BinaryFileBuffer bfb : pq ) bfb.close();
		}
		return rowcounter;
	}

	public static void main(String[] args) throws IOException {
		
		boolean verbose = false;
		int maxtmpfiles = DEFAULTMAXTEMPFILES;
		Charset cs = Charset.defaultCharset();
		String inputfile=null, outputfile=null;
		for(int param = 0; param<args.length; ++param) {
			if(args[param].equals("-v") ||  args[param].equals("--verbose"))
			  verbose = true;
			else if ((args[param].equals("-t") ||  args[param].equals("--maxtmpfiles")) && args.length>param+1) {
				param++;
			    maxtmpfiles = Integer.parseInt(args[param]);  
			} else if ((args[param].equals("-c") || args[param].equals("--charset")) && args.length>param+1) {
				param++;
				cs = Charset.forName(args[param]);
			} else {
				if(inputfile == null) 
				  inputfile = args[param];
				else if (outputfile == null)
				  outputfile = args[param];
				else System.out.println("Unparsed: "+args[param]); 
			}
		}
		if(outputfile == null) {
			System.out.println("please provide input and output file names");
			return;
		}
		Comparator<String> comparator = new Comparator<String>() {
			public int compare(String r1, String r2){
				return r1.compareTo(r2);}};
		List<File> l = sortInBatch(new File(inputfile), comparator, maxtmpfiles,cs,null) ;
		if(verbose) System.out.println("created "+l.size()+" tmp files");
		mergeSortedFiles(l, new File(outputfile), comparator,cs);
	}
}


class BinaryFileBuffer  {
	public static int BUFFERSIZE = 2048;
	public BufferedReader fbr;
	public File originalfile;
	private String cache;
	private boolean empty;
	
	public BinaryFileBuffer(File f, Charset cs) throws IOException {
		originalfile = f;
		fbr = new BufferedReader(new InputStreamReader(new FileInputStream(f),cs), BUFFERSIZE);
		reload();
	}
	
	public boolean empty() {
		return empty;
	}
	
	private void reload() throws IOException {
		try {
          if((this.cache = fbr.readLine()) == null){
            empty = true;
            cache = null;
          }
          else{
            empty = false;
          }
      } catch(EOFException oef) {
        empty = true;
        cache = null;
      }
	}
	
	public void close() throws IOException {
		fbr.close();
	}
	
	
	public String peek() {
		if(empty()) return null;
		return cache.toString();
	}
	public String pop() throws IOException {
	  String answer = peek();
		reload();
	  return answer;
	}
	
	
}