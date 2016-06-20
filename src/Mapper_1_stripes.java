import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_1_stripes extends Mapper<LongWritable, Text, Text, MapWritable> {

	/* Stripe contenete i valori delle coppie (A,B),count(AB) */
	private MapWritable stripe = new MapWritable();

	private Text word = new Text();

	//private final static Text flag = new Text("*");
	private final static IntWritable ONE = new IntWritable(1);

	/*
	 * Lista contenente i valori visitati, se si incontra un valore visitato
	 * precedentemente si salta affinchè il conteggio non venga ripetuto più di
	 * una volta
	 */
	private ArrayList<String> sameValues = new ArrayList<String>();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("----------------------------------MAPPER1---------------------------------------");

		String[] tokens = value.toString().split("\\s+");
		if (tokens.length > 2) {

			for (int i = 0; i < tokens.length; i++) {

				if (sameValues.contains(tokens[i]))
					continue;
				else
					sameValues.add(tokens[i]);

				word.set(tokens[i]);

				//stripe.put(flag, ONE);

				for (int j = 0; j < tokens.length; j++) {

					if (tokens[j].equals(tokens[i]))
						continue;

					for (int k = 0; k < tokens.length; k++) {
						if (tokens[k].equals(tokens[j]))
							continue;
						if (tokens[k].equals(tokens[i]))
							continue;

						/*
						 * Inseriamo nella stripe l'elemento neighbor dandogli
						 * come valore 1
						 */
						stripe.put(new Text(tokens[j] + " " + tokens[k]), ONE);
					}
				}
				context.write(word, stripe);

				stripe.clear();
			}
			sameValues.clear();
		}
	}
}
