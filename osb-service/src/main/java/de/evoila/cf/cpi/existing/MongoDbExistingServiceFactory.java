/**
 * 
 */
package de.evoila.cf.cpi.existing;

import com.mongodb.*;
import de.evoila.cf.broker.bean.ExistingEndpointBean;
import de.evoila.cf.broker.custom.mongodb.MongoDBCustomImplementation;
import de.evoila.cf.broker.custom.mongodb.MongoDbService;
import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.model.Plan;
import de.evoila.cf.broker.model.Platform;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.util.RandomString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author René Schollmeyer
 *
 */

@Service
@ConditionalOnBean(ExistingEndpointBean.class)
public class MongoDbExistingServiceFactory extends ExistingServiceFactory {

    RandomString usernameRandomString = new RandomString(10);
    RandomString passwordRandomString = new RandomString(15);

    @Autowired
    private ExistingEndpointBean existingEndpointBean;

    @Autowired
	private MongoDBCustomImplementation mongodb;

	public void createDatabase(MongoDbService connection, String database) throws PlatformException {
		try {
			MongoClient mongo = connection.mongoClient();
			mongo.setWriteConcern(WriteConcern.JOURNAL_SAFE);
			DB db = mongo.getDB(database);
			DBCollection collection = db.getCollection("_auth");
			collection.save(new BasicDBObject("auth", "auth"));
			collection.drop();
		} catch(MongoException e) {
			throw new PlatformException("Could not add to database", e);
		}
	}

	public void deleteDatabase(MongoDbService connection, String database) throws PlatformException {
		try {
		    connection.mongoClient().dropDatabase(database);
		} catch (MongoException e) {
			throw new PlatformException("Could not remove from database", e);
		}
	}

    @Override
    public void deleteInstance(ServiceInstance serviceInstance, Plan plan) throws PlatformException {
        MongoDbService mongoDbService = this.connection(serviceInstance, plan);

        deleteDatabase(mongoDbService, serviceInstance.getId());
    }

    @Override
    public ServiceInstance createInstance(ServiceInstance serviceInstance, Plan plan, Map<String, Object> parameters) throws PlatformException {

        String username = usernameRandomString.nextString();
        String password = passwordRandomString.nextString();

        serviceInstance.setUsername(username);
        serviceInstance.setPassword(password);

        MongoDbService mongoDbService = this.connection(serviceInstance, plan);

        createDatabase(mongoDbService, serviceInstance.getId());

        return serviceInstance;
    }

    private MongoDbService connection(ServiceInstance serviceInstance, Plan plan) {
        MongoDbService jdbcService = new MongoDbService();

        if (plan.getPlatform() == Platform.EXISTING_SERVICE)
            jdbcService.createConnection(existingEndpointBean.getUsername(), existingEndpointBean.getPassword(),
                    existingEndpointBean.getDatabase(), existingEndpointBean.getHosts());

        return jdbcService;
    }
}
