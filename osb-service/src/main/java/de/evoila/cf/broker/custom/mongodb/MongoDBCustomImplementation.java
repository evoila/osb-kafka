/**
 *
 */
package de.evoila.cf.broker.custom.mongodb;

import com.mongodb.BasicDBObject;
import de.evoila.cf.broker.bean.ExistingEndpointBean;
import de.evoila.cf.broker.model.Plan;
import de.evoila.cf.broker.model.Platform;
import de.evoila.cf.broker.model.ServerAddress;
import de.evoila.cf.broker.model.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author René
 */
@Service
public class MongoDBCustomImplementation {

    private Logger log = LoggerFactory.getLogger(MongoDBCustomImplementation.class);

    @Autowired(required = false)
    private ExistingEndpointBean existingEndpointBean;

    public static void createUserForDatabase(MongoDbService mongoDbService, String database, String username,
                                             String password) {
        createUserForDatabaseWithRoles(mongoDbService, database, username, password, "readWrite");
    }

    public static void createUserForDatabaseWithRoles(MongoDbService mongoDbService, String database, String username,
                                                      String password, String... roles) {
        Map<String, Object> commandArguments = new BasicDBObject();
        commandArguments.put("createUser", username);
        commandArguments.put("pwd", password);
        commandArguments.put("roles", roles);
        BasicDBObject command = new BasicDBObject(commandArguments);

        mongoDbService.mongoClient().getDatabase(database).runCommand(command);
    }

    public MongoDbService connection(ServiceInstance serviceInstance, Plan plan) {
        MongoDbService mongoDbService = new MongoDbService();
        ServerAddress host = serviceInstance.getHosts().get(0);
        log.info("Opening connection to " + host.getIp() + ":" + host.getPort());

        if(plan.getPlatform() == Platform.BOSH)
            mongoDbService.createConnection(serviceInstance.getUsername(), serviceInstance.getPassword(),
                    "admin", serviceInstance.getHosts());
        else if (plan.getPlatform() == Platform.EXISTING_SERVICE)
            mongoDbService.createConnection(existingEndpointBean.getUsername(), existingEndpointBean.getPassword(),
                    existingEndpointBean.getDatabase(), existingEndpointBean.getHosts());

        return mongoDbService;
    }
}
