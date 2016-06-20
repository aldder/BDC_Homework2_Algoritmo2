import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_2 extends Mapper<LongWritable, Text, Text, Text> {

	private final static String flag = new String("$");
	private Text first = new Text();
	private Text second = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("----------------------------------MAPPER2----------------------------------------");

		String[] tokens = value.toString().split("\\s+");

		if (tokens.length == 3) {
			/*
			 * Input da intermediate_output del primo algoritmo. Contiene
			 * <(BC),count(BC)>
			 */
			if (!tokens[0].equals(flag)) {
				/*
				 * Saltiamo se incontriamo un '*', non serve nel conteggio di
				 * count(BC). Prende in input una coppia del tipo
				 * <(BC),count(BC)> ed emette una coppia del tipo
				 * <(BC);($,count(BC))>
				 */
				first.set(tokens[0] + " " + tokens[1]);
				second.set("$" + " " + tokens[2]);
				context.write(first, second);
			}
		} else if (tokens.length == 4) {
			/*
			 * Input da intermediate_output del secondo algoritmo. Prende in
			 * input una coppia del tipo <(ABC),count(ABC)> ed emette una coppia
			 * del tipo <(BC);(A,count(ABC)>
			 */
			first.set(tokens[1] + " " + tokens[2]);
			second.set(tokens[0] + " " + tokens[3]);
			context.write(first, second);
		}
	}
}
