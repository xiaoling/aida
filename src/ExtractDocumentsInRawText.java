import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import mpi.aida.data.Mention;
import mpi.aida.data.Mentions;
import mpi.aida.data.PreparedInput;
import mpi.experiment.reader.CoNLLReader;
import mpi.experiment.reader.CollectionReaderSettings;
import mpi.experiment.reader.CollectionReader.CollectionPart;
import mpi.tokenizer.data.Token;

import org.apache.commons.io.FileUtils;


/**
 * read the AIDA data set and output in plain text 
 * @author xiaoling
 * 
 */
public class ExtractDocumentsInRawText {
	public static void main(String[] args) {
        System.out.println("args[0]: path to the files. args[1]: part, e.g., dev or test");
		CollectionReaderSettings settings = new CollectionReaderSettings();
		settings.setIncludeOutOfDictionaryMentions(true);
		settings.setKeepSpaceBeforePunctuations(true);
//        settings.setIncludeNMEMentions(true);
        CollectionPart part = null;
        if (args[1].equals("dev")) {
            part = CollectionPart.DEV;
        } else if (args[1].equals("test")) {
            part = CollectionPart.TEST;
        }
		CoNLLReader reader = new CoNLLReader(
				args[0],
				part, settings);
		for (PreparedInput inputDoc : reader) {
			String id = inputDoc.getDocId();
			id = id.substring(0, id.indexOf("test"));
			int s = 0;
			StringBuilder sb = new StringBuilder();
			for (Token token : inputDoc.getTokens()) {
				if (token.getSentence() != s) {
					sb.append("\n");
					s = token.getSentence();
				} else {
					sb.append(" ");
				}
				sb.append(token.getOriginal());
			}
			Mentions mentions = inputDoc.getMentions();
			Collections.sort(mentions.getMentions(), new Comparator<Mention>(){

				@Override
				public int compare(Mention o1, Mention o2) {
					return o2.getCharOffset() - o1.getCharOffset();
				}
			});
			/*for (Mention mention: mentions.getMentions()) {
				int beg = mention.getCharOffset()+1, end = beg+mention.getCharLength();
				System.out.println("mention = "+sb.toString().substring(beg,end)+","+beg+","+end);
				sb.insert(end, "]]");
				sb.insert(beg, "[[");
			}*/
			try {
				FileUtils.writeStringToFile(new File("exp/aidaRawText/" + id
						+ ".txt"), sb.toString(), "UTF-8");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
