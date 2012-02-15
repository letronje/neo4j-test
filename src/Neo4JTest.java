import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

enum RelTypes implements RelationshipType
{
    NEAR
}

public class Neo4JTest {
	private static GraphDatabaseService graphDb = null;
	private static Index<Node> placesIndex = null;
	private static Index<Relationship> neighboursIndex = null;
	
	private static Set<String> getCityNames(String fileName) throws FileNotFoundException, IOException{
		return new HashSet<String>(IOUtils.readLines(new FileReader(fileName)));
	}
	
	private static void deleteDb(String dbPath){
		try {
			File dbDir = new File(dbPath);
			if(dbDir.exists()){
				System.out.println("Deleting existing db @ " + dbPath);
				FileUtils.forceDelete(dbDir);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void createCities(Set<String> cityNames){
		Transaction tx = graphDb.beginTx();
		
		try
		{
			long t1 = System.nanoTime();
			
			for(String cityName: cityNames){
				Node node = graphDb.createNode();
				node.setProperty("name", cityName);
				//node.setProperty("misc", "[" + cityName + "]");
				placesIndex.add(node, "name", cityName);
			}
			
			long t2 = System.nanoTime();
			System.out.println(cityNames.size() + " cities added in " + (t2-t1)/1e9 + " secs");
			tx.success();
		}
		finally{
		   tx.finish();
		}
		
	}
	
	private static Node getCityByName(String cityName){
		return placesIndex.get("name", cityName).getSingle();
	}
	
	private static void linkCities(Set<String> cityNamesSet, int numNeighbours){
		Transaction tx = graphDb.beginTx();
		Random rand = new Random();
		ArrayList<String> cityNames = new ArrayList<String>(cityNamesSet);
		List<String> cityNamesDup = (ArrayList<String>) cityNames.clone();
		
		try
		{
			long t1 = System.nanoTime();
			int numLinks = 0;
			
			for(String cityName: cityNames){
				//System.out.print(cityName +  " : ");
				
				Node city = getCityByName(cityName);
				
				Collections.shuffle(cityNamesDup);
				List<String> neighbourNames = new ArrayList<String>(cityNamesDup).subList(0, numNeighbours);
				neighbourNames.remove(cityName);
				
				//System.out.println(neighbourNames);
				
				for(String neighbourName: neighbourNames){
					Node neighbour = getCityByName(neighbourName);
					
					
					if(neighboursIndex.get("name", cityName + "_" + neighbourName).getSingle() == null){
						Relationship rel1 = city.createRelationshipTo(neighbour, RelTypes.NEAR);
						neighboursIndex.add(rel1, "name", cityName + "_" + neighbourName);
					};
					
					if(neighboursIndex.get("name", neighbourName + "_" + cityName).getSingle() == null){
						Relationship rel2 = neighbour.createRelationshipTo(city, RelTypes.NEAR);
						neighboursIndex.add(rel2, "name", neighbourName + "_" + cityName);
					}
					
					numLinks ++;
				}
			}
			
			long t2 = System.nanoTime();
			System.out.println(numLinks + " edges added in " + (t2-t1)/1e9 + " secs");
			tx.success();
		}
		finally{
		   tx.finish();
		}
		
	}
	
	private static void printNeighbours(Set<String> cityNames){
		Transaction tx = graphDb.beginTx();
				
		try{
			DescriptiveStatistics stats = new DescriptiveStatistics();

			for(String cityName: cityNames){
				long t1 = System.nanoTime();
				
				Node city = getCityByName(cityName);
				
				Iterable<Relationship> relationships = city.getRelationships(Direction.OUTGOING);
				
				List<String> neighbours = new ArrayList<String>();
				for(Relationship relationship : relationships){
					neighbours.add((String)relationship.getEndNode().getProperty("name"));
				}
				
				long t2 = System.nanoTime();
				long time = t2-t1;
				stats.addValue(time);
			}
			System.out.print("min : " + stats.getMin()/1e6);
			System.out.print(", avg : " + stats.getMean()/1e6);
			System.out.print(", max : " + stats.getMax()/1e6);
			System.out.println(", median : " + stats.getPercentile(50)/1e6);
						
			tx.success();
		}
		finally{
		   tx.finish();
		}
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		String dbPath = args[0];
		
		deleteDb(dbPath);
		
		graphDb = new EmbeddedGraphDatabase(dbPath);
		placesIndex = graphDb.index().forNodes("places");
		neighboursIndex = graphDb.index().forRelationships("neighbours");
		
		int numNeighbours = 10;
		int numCities = 10000;
		
		int numUniqueCities = 1000;
		List<String> uniqueCityNames = new ArrayList<String>(getCityNames(args[1]));
		System.out.println("Found " + uniqueCityNames.size() + " cities in " + args[1]);
		int times = numCities/numUniqueCities;
		
		List<String> allCityNames = new ArrayList<String>(); 
		while(times-- > 0){
			for(String c: uniqueCityNames){
				allCityNames.add(c + "_" + times);
			}
		}
		
		Collections.shuffle(allCityNames);
		Set<String> cityNames = new HashSet<String>(allCityNames.subList(0, numCities));
				
		createCities(cityNames);
		linkCities(cityNames, numNeighbours);
		
		int numTimesPrint = 10;
		
		System.out.println();
		System.out.println("Fetching adjacent cities for all cities : ");
		
		for(int i=1;i<=numTimesPrint;i++){
			System.out.print("Run #" + i + " ");
			printNeighbours(cityNames);
		}
		
		graphDb.shutdown();
	}
}
