import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Reducer_2 extends Reducer<Text, Text, Text, Text> {

	private final static String flag = new String("$");
	private List<String> pairList = new LinkedList<String>();
	private Text first = new Text();
	private Text second = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// System.out.println("----------------------------------REDUCER2----------------------------------------");

		int countBC = 0;

		for (Text value : values) {
			/*
			 * Scorre i valori per trovare il valore count(BC), ossia il secondo
			 * item della coppia dove il primo item è rappresentato da '$'.
			 * Quindi salva i rimanenti item (contenenti le coppie (BC) e
			 * (A,count(ABC)) nella lista ausiliaria.
			 */
			String[] v = value.toString().split("\\s+");

			if (v[0].equals(flag)) {
				countBC = Integer.parseInt(v[1].trim());
			} else {
				pairList.add(value.toString());
			}
		}
		//System.out.println();
		for (String pair : pairList) {
			/*
			 * Scorre la lista dei valori dove la chiave relativa ad essi è (BC)
			 * e i valori sono (A,count(ABC). Per ogni valore prende l'item
			 * count(ABC), e calcola la probabilità condizionata
			 * count(ABC)/count(BC) per ogni tripla (ABC) ed emette in output
			 * (ABC), count(ABC), Pr(A|BC)
			 */
			String[] item = pair.toString().split("\\s+");
			float countABC = (float) Integer.parseInt(item[1].trim());
			first.set(item[0] + " " + key);
			second.set(item[1] + " " + countABC / countBC);
			context.write(first, second);
		}
		pairList.clear();
	}
}