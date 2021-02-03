/**
 *
 */
package de.evoila.cf.broker.custom.kafka;

import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.exception.ServiceBrokerException;
import de.evoila.cf.broker.model.*;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.repository.*;
import de.evoila.cf.broker.service.AsyncBindingService;
import de.evoila.cf.broker.service.impl.BindingServiceImpl;
import de.evoila.cf.cpi.bosh.KafkaBoshPlatformService;
import de.evoila.cf.cpi.bosh.deployment.manifest.Manifest;
import de.evoila.cf.security.credentials.credhub.CredhubClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * @author Johannes Hiemer.
 */
@Service
public class KafkaBindingService extends BindingServiceImpl {

    private Logger log = LoggerFactory.getLogger(KafkaBindingService.class);

    private static String KAFKA_BROKERS = "brokers";
    private static String KAFKA_BOOTSTRAP_SERVERS = "bootstrap-servers";
    private static String KAFKA_BOOTSTRAP_SERVERS_BAK = "bootstrap_servers";
    private static String ZOOKEEPER_BROKERS = "zkNodes";
    private static String DEFAULT_BROKER_PORT = "defaultBrokerPort";
    private static String DEFAULT_ZK_PORT = "defaultZkPort";
    private static String USERNAME = "user";
    private static String PASSWORD = "password";

    private CredhubClient credhubClient;

    private KafkaBoshPlatformService kafkaBoshPlatformService;

    @EventListener(ContextRefreshedEvent.class)
    public void changeSystemProrties() {
        System.setProperties(new ThreadLocalProperties(System.getProperties()));
    }

    public KafkaBindingService(BindingRepository bindingRepository, ServiceDefinitionRepository serviceDefinitionRepository, ServiceInstanceRepository serviceInstanceRepository,
                               RouteBindingRepository routeBindingRepository, JobRepository jobRepository, AsyncBindingService asyncBindingService, PlatformRepository platformRepository,
                               CredhubClient credhubClient, KafkaBoshPlatformService kafkaBoshPlatformService) {
        super(bindingRepository, serviceDefinitionRepository, serviceInstanceRepository, routeBindingRepository, jobRepository, asyncBindingService, platformRepository);
        this.credhubClient = credhubClient;
        this.kafkaBoshPlatformService = kafkaBoshPlatformService;

    }

    @Override
    protected Map<String, Object> createCredentials(String bindingId, ServiceInstanceBindingRequest serviceInstanceBindingRequest,
                                                    ServiceInstance serviceInstance, Plan plan, ServerAddress host) throws PlatformException, ServiceBrokerException {

        Map<String, Object> credentials = new HashMap<>();

        List<String> brokers = new LinkedList<>();
        List<String> zookeepers = new LinkedList<>();


        Map<String,Object> permissions= (Map<String, Object>) serviceInstanceBindingRequest.getParameters().get("acl");

        if (permissions != null) {
            permissions = (Map<String, Object>) permissions.get("permissons");
        }
        if (permissions == null) {
            permissions = new HashMap<String, Object>();
            List<Object> topics = new ArrayList<Object>(1);
            Map <String,Object> allTopics = new HashMap<String, Object>();
            List<String> rights = new ArrayList<String>(1);
            allTopics.put("name","*");
            allTopics.put("rights",rights);
            rights.add("ALL");
            topics.add(allTopics);
            permissions.put("topics",topics);
        }
        credhubClient.createUser(serviceInstance, bindingId);

        String username = credhubClient.getUser(serviceInstance, bindingId).getUsername();
        String password = credhubClient.getUser(serviceInstance, bindingId).getPassword();

        serviceInstance.getHosts().forEach(instance -> {
            if (instance.getPort() == KafkaBoshPlatformService.KAFKA_PORT_SSL || instance.getPort() == KafkaBoshPlatformService.KAFKA_PORT) {
                brokers.add(instance.getIp() + ":" + instance.getPort());
            } else if (instance.getPort() == KafkaBoshPlatformService.ZOOKEEPER_PORT) {
                zookeepers.add(instance.getIp() + ":" + instance.getPort());
            }
        });

        try {
            kafkaBoshPlatformService.createKafkaUser(serviceInstance, plan, username, password,"");
        } catch (Exception e) {
            e.printStackTrace();
            throw new PlatformException("Can not create user");
        }


        KafkaAdminTools adminTools = null;
        try {
            adminTools = new KafkaAdminTools(serviceInstance.getHosts(), true, "admin", credhubClient.getPassword(serviceInstance, "admin_password"));
            //adminTools.createUser(username, password);
            adminTools.createAcl(username,permissions);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ServiceBrokerException("Problem with create User");
        }finally {
            if (adminTools!=null) {
                adminTools.clean();
            }
        }

        Manifest manifest = null;
        try {
            manifest = kafkaBoshPlatformService.getDeployedManifest(serviceInstance);
        } catch (IOException e) {
            e.printStackTrace();
        }

        credentials.put(KAFKA_BROKERS, brokers);
        credentials.put(KAFKA_BOOTSTRAP_SERVERS, brokers);
        credentials.put(KAFKA_BOOTSTRAP_SERVERS_BAK, brokers);

        if(manifest != null && kafkaBoshPlatformService.isKafkaSecure(manifest)) {
            credentials.put(DEFAULT_BROKER_PORT, KafkaBoshPlatformService.KAFKA_PORT_SSL);
        } else {
            credentials.put(DEFAULT_BROKER_PORT, kafkaBoshPlatformService.KAFKA_PORT);
        }

        credentials.put(ZOOKEEPER_BROKERS, zookeepers);
        credentials.put(DEFAULT_ZK_PORT, KafkaBoshPlatformService.ZOOKEEPER_PORT);
        credentials.put(USERNAME, username);
        credentials.put(PASSWORD, password);

        return credentials;
    }

    @Override
    protected void unbindService(ServiceInstanceBinding binding, ServiceInstance serviceInstance, Plan plan) throws PlatformException, ServiceBrokerException {
        ArrayList<Map<String,Object>> topicMap = (ArrayList<Map<String, Object>>) binding.getParameters().get("topics");


        KafkaAdminTools adminTools = null;
        try {
            adminTools = new KafkaAdminTools(serviceInstance.getHosts(),true,"admin",credhubClient.getPassword(serviceInstance,"admin_password"));
            adminTools.deleteAcl(binding.getId());
            //adminTools.deleteUser(binding.getId());
        } catch (IOException e) {
            e.printStackTrace();
            throw new ServiceBrokerException("Problem with delete User");
        }finally {
            adminTools.clean();
        }

        try {
            kafkaBoshPlatformService.deleteKafkaUser(serviceInstance, plan, credhubClient.getUser(serviceInstance, binding.getId()).getUsername(),"");
        } catch (Exception e) {
            e.printStackTrace();
            throw new PlatformException("Can not delete user");
        }
        
        credhubClient.deleteCredentials(serviceInstance.getId(), binding.getId());
    }

    @Override
    protected RouteBinding bindRoute(ServiceInstance serviceInstance, String route) {
        throw new UnsupportedOperationException();
    }
}
