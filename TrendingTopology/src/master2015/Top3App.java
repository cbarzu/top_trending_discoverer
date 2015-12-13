package master2015;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * TODO
 * 1. Configurar fichero server.properties
 * 2. Donde colocar el kafka
 * 3. Windows
 * 4. Reparto a los bolts (por lan??)
 * 
 * @author javiervillargil
 *
 */

public class Top3App {
	public static String TOPOLOGY_ID;
	public static final String SPOUT_ID = "TwitterSpout";
	public static final String BOLT_ID = "TwitterSpout";
	public static final String TWITTER_OUTSTREAM = "TwitterOutStream";
	
	public static void main(String[] args) {
		/** Reads and verifies arguments **/
		if(args.length != 5){
			System.err.println("ERROR: Bad arguments. Usage:"
					+ "langList: String with the list of languages (“lang” values) we are interested in. The list is in CSV format, example: en,pl,ar,es"
					+ "Zookeeper URL: String IP:port of the Zookeeper node."
					+ "winParams: String with the window parameters size and advance using the format: size,advance. The time units are seconds."
					+ "topologyName: String identifying the topology in the Storm Cluster."
					+ "Folder: path to the folder used to store the output files (the path is relative to the filesystem of the node that will be used to run the Storm Supervisor)");
			System.exit(-1);
		}
				
		System.out.println("langList: "+args[0]);
		System.out.println("Zookeeper URL: "+args[1]);
		System.out.println("winParams: "+args[2]);
		System.out.println("topologyName: "+args[3]);
		System.out.println("Folder: "+args[4]);
		
		Top3App.TOPOLOGY_ID = args[3];
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(Top3App.SPOUT_ID, new TwitterHashtagsSpout(args[1]));
		
		//Indicar el tipo de stream grouping deseado (shuffle, fields, all, custom, direct, global)
		builder.setBolt(Top3App.BOLT_ID, new TwitterHashtagsBolt()).localOrShuffleGrouping(Top3App.SPOUT_ID, Top3App.TWITTER_OUTSTREAM);

		
		//LocalCluster cluster = new LocalCluster();
		//cluster.submitTopology(Top3App.TOPOLOGY_ID, new Config(), builder.createTopology());
		try {
			StormSubmitter.submitTopology(Top3App.TOPOLOGY_ID, new Config(), builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}
