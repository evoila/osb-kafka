package de.evoila.cf.broker.backup;

import com.mongodb.MongoException;
import com.mongodb.client.ListDatabasesIterable;
import de.evoila.cf.broker.bean.BackupTypeConfiguration;
import de.evoila.cf.broker.custom.mongodb.MongoDBCustomImplementation;
import de.evoila.cf.broker.custom.mongodb.MongoDbService;
import de.evoila.cf.broker.exception.ServiceBrokerException;
import de.evoila.cf.broker.exception.ServiceDefinitionDoesNotExistException;
import de.evoila.cf.broker.exception.ServiceInstanceDoesNotExistException;
import de.evoila.cf.broker.model.Plan;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.repository.ServiceDefinitionRepository;
import de.evoila.cf.broker.repository.ServiceInstanceRepository;
import de.evoila.cf.broker.service.BackupCustomService;
import de.evoila.cf.model.EndpointCredential;
import org.bson.Document;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@ConditionalOnBean(BackupTypeConfiguration.class)
public class BackupCustomServiceImpl implements BackupCustomService {

    BackupTypeConfiguration backupTypeConfiguration;

    ServiceInstanceRepository serviceInstanceRepository;

    MongoDBCustomImplementation mongoDBCustomImplementation;

    ServiceDefinitionRepository serviceDefinitionRepository;

    public BackupCustomServiceImpl(BackupTypeConfiguration backupTypeConfiguration,
                                   ServiceInstanceRepository serviceInstanceRepository,
                                   MongoDBCustomImplementation mongoDBCustomImplementation,
                                   ServiceDefinitionRepository serviceDefinitionRepository) {
        this.backupTypeConfiguration = backupTypeConfiguration;
        this.serviceInstanceRepository = serviceInstanceRepository;
        this.mongoDBCustomImplementation = mongoDBCustomImplementation;
        this.serviceDefinitionRepository = serviceDefinitionRepository;
    }

    @Override
    public Map<String, String> getItems(String serviceInstanceId) throws ServiceInstanceDoesNotExistException,
            ServiceDefinitionDoesNotExistException {
        ServiceInstance instance = serviceInstanceRepository.getServiceInstance(serviceInstanceId);

        if(instance == null || instance.getHosts().size() <= 0) {
            throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
        }

        Plan plan = serviceDefinitionRepository.getPlan(instance.getPlanId());

        MongoDbService mongoDbService = mongoDBCustomImplementation.connection(instance, plan);

        Map<String, String> result = new HashMap<>();
        try {
            ListDatabasesIterable<Document> databases = mongoDbService.mongoClient().listDatabases();

            for(Document database : databases)
                result.put(database.getString("name"), database.getString("name"));
        } catch(MongoException ex) {
            new ServiceBrokerException("Could not load databases", ex);
        }

        return result;
    }

    public EndpointCredential getCredentials(String serviceInstanceId) throws ServiceInstanceDoesNotExistException {
        ServiceInstance instance = serviceInstanceRepository.getServiceInstance(serviceInstanceId);

        if(instance == null || instance.getHosts().size() <= 0) {
            throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
        }

        EndpointCredential credential = new EndpointCredential();
        credential.setServiceInstanceId(instance.getId());
        credential.setUsername(instance.getUsername());
        credential.setPassword(instance.getPassword());
        credential.setHostname(instance.getHosts().get(0).getIp());
        credential.setPort(instance.getHosts().get(0).getPort());
        credential.setType(backupTypeConfiguration.getType());

        return credential;
    }

}
