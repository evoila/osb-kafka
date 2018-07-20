/**
 * 
 */
package de.evoila.cf.broker.custom.mongodb;

import com.mongodb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Johannes Hiemer
 *
 */
public class MongoDbService {

	private MongoClient mongoClient;

	public boolean isConnected() {
		return mongoClient != null && mongoClient.getUsedDatabases() != null;
	}

    public MongoClient mongoClient() {
        return mongoClient;
    }

	public void createConnection(String username, String password, String database, List<de.evoila.cf.broker.model.ServerAddress> hosts) {
		if(database == null)
			database = "admin";
		
		List<ServerAddress> serverAddresses = new ArrayList<>();
		for (de.evoila.cf.broker.model.ServerAddress host : hosts) {
			serverAddresses.add(new ServerAddress(host.getIp(), host.getPort()));
		}

		MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(username, database, password.toCharArray());
		MongoClientOptions mongoClientOptions = MongoClientOptions
                .builder()
                .writeConcern(WriteConcern.JOURNALED).build();
		mongoClient = new MongoClient(serverAddresses, Arrays.asList(mongoCredential), mongoClientOptions);
	}

}
