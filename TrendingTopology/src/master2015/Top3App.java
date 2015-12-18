package master2015;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Top3App: Storm topology builder.
 * 
 * Creates and submits the storm topology.
 * 
 * @author Claudiu Barzu (claudiu.barzu@alumnos.upm.es)
 * @author Javier Villar Gil (javier.villar.gil@alumnos.upm.es)
 *
 */
public class Top3App {
	/** ID for the Storm Topology */
	public static String TOPOLOGY_ID;

	/** ID for the Storm Spout */
	public static final String SPOUT_ID = "TwitterSpout";

	/** ID for the Storm Bolt */
	public static final String BOLT_ID = "TwitterBolt";

	/** ID for the Storm Out Stream */
	public static final String TWITTER_OUTSTREAM = "TwitterOutStream";

	/**
	 * Main method
	 * 
	 * @param args[0]
	 *            langList. String with the list of languages (“lang” values) we
	 *            are interested in. The list is in CSV format, example:
	 *            en,pl,ar,es.
	 * @param args[1]
	 *            Zookeeper URL. String IP:port of the Zookeeper node.
	 * @param args[2]
	 *            winParams. String with the window parameters size and advance
	 *            using the format: size,advance. The time units are seconds.
	 * @param args[3]
	 *            topologyName. String identifying the topology in the Storm
	 *            Cluster.
	 * @param args[4]
	 *            Folder path. to the folder used to store the output files (the
	 *            path is relative to the filesystem of the node that will be
	 *            used to run the Storm Supervisor).
	 */
	public static void main(String[] args) {
		/** Reads and verifies arguments **/
		if (args.length != 5) {
			System.err.println("****** ERROR ****** Wrong arguments. Usage:"
					+ "\n\t[1] langList: String with the list of languages (“lang” values) we are interested in. The list is in CSV format, example: en,pl,ar,es"
					+ "\n\t[2] Zookeeper URL: String IP:port of the Zookeeper node."
					+ "\n\t[3] winParams: String with the window parameters size and advance using the format: size,advance. The time units are seconds."
					+ "\n\t[4] topologyName: String identifying the topology in the Storm Cluster."
					+ "\n\t[5] Folder: path to the folder used to store the output files (the path is relative to the filesystem of the node that will be used to run the Storm Supervisor)");
			System.exit(-1);
		}

		System.out.println(" ----------------------------------------");
		System.out.println("             STARTING Top3App            ");
		System.out.println(" ----------------------------------------");
		System.out.println("\nGiven arguments:");
		System.out.println("\t[1] langList: " + args[0]);
		System.out.println("\t[2] Zookeeper URL: " + args[1]);
		System.out.println("\t[3] winParams: " + args[2]);
		System.out.println("\t[4] topologyName: " + args[3]);
		System.out.println("\t[5] Folder: " + args[4]);

		/** Arguments reading **/
		// Setting topology ID
		Top3App.TOPOLOGY_ID = args[3];

		// Creating a language list
		List<String> langList = new ArrayList<String>();
		String[] argLang = args[0].split(",");

		for (String lang : argLang) {
			langList.add("\"" + lang + "\""); // Lang within " " characters for
												// the json format
		}

		// Window parameters
		String[] windowParams = args[2].split(",");
		if (windowParams.length != 2) {
			System.err.println("****** ERROR ****** Wrong window parameters. Expected:"
					+ "[size,advance] in seconds, got " + args[2]);
			System.exit(-3);
		}

		// From seconds to mseconds
		long windowSize = Long.parseLong(windowParams[0]) * 1000;
		long windowAdvance = Long.parseLong(windowParams[1]) * 1000;

		/** Setting up Storm Topology **/
		System.out.println("\n ---> Creating topology [" + Top3App.TOPOLOGY_ID + "] ........");
		TopologyBuilder builder = new TopologyBuilder();

		//Create one spout
		System.out.println("\n ---> Creating Storm Spout [" + Top3App.SPOUT_ID + "] ........");
		builder.setSpout(Top3App.SPOUT_ID, new TwitterHashtagsSpout(args[1], langList));

		//Create as many bolts as different languages we have to analyze (fieldsGrouping by lang)
		System.out.println("\n ---> Creating Storm Bolts ("+langList.size()+") [" + Top3App.BOLT_ID + "] ........");
		builder.setBolt(Top3App.BOLT_ID, new TwitterHashtagsBolt(Top3App.BOLT_ID, args[4], windowSize, windowAdvance),langList.size())
				.fieldsGrouping(Top3App.SPOUT_ID, Top3App.TWITTER_OUTSTREAM, new Fields(TwitterHashtagsSpout.LANG_FIELD));


		/** Submits the topology (or local mode) **/
		if (args[1].toLowerCase().contains("localhost")) {
			System.out
					.println("\n ---> Executing topology [" + Top3App.TOPOLOGY_ID + "] in local cluster ........\n\n");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(Top3App.TOPOLOGY_ID, new Config(), builder.createTopology());

			Utils.sleep(1000000);
			cluster.killTopology(Top3App.TOPOLOGY_ID);
			cluster.shutdown();
		} else {
			System.out.println("\n ---> Submiting topology [" + Top3App.TOPOLOGY_ID + "] to cluster [" + args[1]
					+ "]........\n\n");
			try {
				StormSubmitter.submitTopology(Top3App.TOPOLOGY_ID, new Config(), builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}

	}

}
