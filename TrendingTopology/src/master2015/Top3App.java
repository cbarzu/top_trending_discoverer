package master2015;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * TODO
 * 1. Configurar fichero server.properties
 * 2. Donde colocar el kafka
 * 3. Windows
 * 4. Reparto a los bolts (por lan??)
 *
 */

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
		
		//Window parameters
		String[] windowParams = args[2].split(",");
		if(windowParams.length != 2){
			System.err.println("****** ERROR ****** Wrong window parameters. Expected:"
					+ "[size,advance] in seconds, got "+args[2]);
			System.exit(-3);		
		}
		
		long windowSize = Long.parseLong(windowParams[0]);
		long windowAdvance = Long.parseLong(windowParams[1]);

		/** Setting up Storm Topology **/

		System.out.println("\n ---> Creating topology [" + Top3App.TOPOLOGY_ID + "] ........");
		TopologyBuilder builder = new TopologyBuilder();

		System.out.println("\n ---> Creating Storm Spouts [" + Top3App.SPOUT_ID + "] ........");
		builder.setSpout(Top3App.SPOUT_ID, new TwitterHashtagsSpout(args[1], langList));
		

		/** TODO Create as many bolts as languages */
		/** TODO Change localOrShuffleGrouping to fieldsGrouping */

		int i=0;
		//for (int i = 0; i < langList.size(); i++) {
			//builder.setBolt(Top3App.BOLT_ID +"_"+i, new TwitterHashtagsBolt(Top3App.BOLT_ID +"_"+i, args[4])).fieldsGrouping(Top3App.SPOUT_ID,
			//		new Fields(TwitterHashtagsSpout.LANG_FIELD));

			System.out.println("\n ---> Creating Storm Bolts [" + Top3App.BOLT_ID +"_"+i + "] ........");
			builder.setBolt(Top3App.BOLT_ID, new TwitterHashtagsBolt(Top3App.BOLT_ID +"_"+i, args[4], windowSize, windowAdvance))
					.localOrShuffleGrouping(Top3App.SPOUT_ID, Top3App.TWITTER_OUTSTREAM);
		//}

		/** TODO Change local cluster for StormSubmitter */

		System.out.println("\n ---> Submiting topology [" + Top3App.TOPOLOGY_ID + "] to cluster ........");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(Top3App.TOPOLOGY_ID, new Config(), builder.createTopology());

		Utils.sleep(100000);
		cluster.killTopology(Top3App.TOPOLOGY_ID);
		cluster.shutdown();

		// try {
		// StormSubmitter.submitTopology(Top3App.TOPOLOGY_ID, new Config(),
		// builder.createTopology());
		// } catch (AlreadyAliveException e) {
		// e.printStackTrace();
		// } catch (InvalidTopologyException e) {
		// e.printStackTrace();
		// }

	}

}
