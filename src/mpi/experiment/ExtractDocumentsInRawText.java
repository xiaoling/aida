package mpi.experiment;

import java.io.File;
import java.io.IOException;

import mpi.aida.data.PreparedInput;
import mpi.experiment.reader.CoNLLReader;
import mpi.experiment.reader.CollectionReader.CollectionPart;
import mpi.tokenizer.data.Token;

import org.apache.commons.io.FileUtils;

public class ExtractDocumentsInRawText {
	public static void main(String[] args) {
		CoNLLReader reader = new CoNLLReader("/homes/gws/xiaoling/dataset/nel/AIDA/aida-yago2-dataset/", CollectionPart.TEST);
		for (PreparedInput inputDoc : reader) {
			String id = inputDoc.getDocId();
			id = id.substring(0, id.indexOf("testb"));
			int s = 0;
			StringBuilder sb = new StringBuilder();
			for (Token token:inputDoc.getTokens()) {
				if (token.getSentence() !=s) {
					sb.append("\n");
					s = token.getSentence();
				}else {
					sb.append(" ");
				}
				sb.append(token.getOriginal());
			}
			try {
				FileUtils.writeStringToFile(new File("/homes/gws/xiaoling/dataset/nel/AIDA/aida-yago2-dataset/rawText/"+id), sb.toString(), "UTF-8");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

