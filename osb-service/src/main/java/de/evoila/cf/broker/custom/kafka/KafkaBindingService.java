/**
 *
 */
package de.evoila.cf.broker.custom.kafka;


import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.model.RouteBinding;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.ServiceInstanceBinding;
import de.evoila.cf.broker.model.ServiceInstanceBindingRequest;
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
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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

    public KafkaBindingService(BindingRepository bindingRepository, ServiceDefinitionRepository serviceDefinitionRepository, ServiceInstanceRepository serviceInstanceRepository,
                               RouteBindingRepository routeBindingRepository, JobRepository jobRepository, AsyncBindingService asyncBindingService, PlatformRepository platformRepository,
                               CredhubClient credhubClient, KafkaBoshPlatformService kafkaBoshPlatformService) {
        super(bindingRepository, serviceDefinitionRepository, serviceInstanceRepository, routeBindingRepository, jobRepository, asyncBindingService, platformRepository);
        this.credhubClient = credhubClient;
        this.kafkaBoshPlatformService = kafkaBoshPlatformService;
    }

    @Override
    protected Map<String, Object> createCredentials(String bindingId, ServiceInstanceBindingRequest serviceInstanceBindingRequest,
                                                    ServiceInstance serviceInstance, Plan plan, ServerAddress host)  throws PlatformException {

        Map<String, Object> credentials = new HashMap<>();

        /*String topics="*:ALL";
        String groups="*:ALL";
        String cluster="";*/
        List<String> brokers = new LinkedList<>();
        List<String> zookeepers = new LinkedList<>();

        serviceInstance.getHosts().forEach(instance -> {
            if (instance.getPort() == KafkaBoshPlatformService.KAFKA_PORT_SSL || instance.getPort() == KafkaBoshPlatformService.KAFKA_PORT) {
                brokers.add(instance.getIp() + ":" + instance.getPort());
            } else if (instance.getPort() == KafkaBoshPlatformService.ZOOKEEPER_PORT) {
                zookeepers.add(instance.getIp() + ":" + instance.getPort());
            }
        });

        /*ArrayList<Map<String,Object>> topicMap = (ArrayList<Map<String, Object>>) serviceInstanceBindingRequest.getParameters().get("topics");

        if(topicMap!=null) {
            topics = String.join(";", topicMap.stream().map(topic -> {
                return topic.get("name") + ":" + String.join(",", (ArrayList<String>) topic.get("rights"));
            }).collect(Collectors.toList()));
        }

        ArrayList<Map<String,Object>> groupMap = (ArrayList<Map<String, Object>>) serviceInstanceBindingRequest.getParameters().get("groups");

        if (groupMap != null) {
            groups = String.join(";", topicMap.stream().map(group -> {
                return group.get("name") + ":" + String.join(",", (ArrayList<String>) group.get("rights"));
            }).collect(Collectors.toList()));
        }
        credhubClient.createUser(serviceInstance, bindingId);


        ArrayList<String> clusterArray = (ArrayList<String>) serviceInstanceBindingRequest.getParameters().get("custer");

        if(clusterArray!=null) {
            cluster = String.join(",", clusterArray);
        }*/

        credhubClient.createUser(serviceInstance, bindingId);

        String username = credhubClient.getUser(serviceInstance, bindingId).getUsername();
        String password = credhubClient.getUser(serviceInstance, bindingId).getPassword();

        try {
            //kafkaBoshPlatformService.createKafkaUser(serviceInstance, plan, username, password, topics, groups, cluster);
            kafkaBoshPlatformService.createKafkaUser(serviceInstance, plan, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Manifest manifest = null;
        try {
            manifest = kafkaBoshPlatformService.getDeployedManifest(serviceInstance);
        } catch (IOException e) {
            e.printStackTrace();
            throw new PlatformException("Can not create user");
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
    protected void unbindService(ServiceInstanceBinding binding, ServiceInstance serviceInstance, Plan plan) throws PlatformException {

        /*String topics="*:ALL";
        String groups="*:AALL";
        String cluster="";

        ArrayList<Map<String,Object>> topicMap = (ArrayList<Map<String, Object>>) binding.getParameters().get("topics");

        if(topicMap!=null) {
            topics = String.join(";", topicMap.stream().map(topic -> {
                return topic.get("name") + ":" + String.join(",", (ArrayList<String>) topic.get("rights"));
            }).collect(Collectors.toList()));
        }

        ArrayList<Map<String,Object>> groupMap = (ArrayList<Map<String, Object>>) binding.getParameters().get("groups");

        if (groupMap!=null) {
            groups = String.join(";", topicMap.stream().map(group -> {
                return group.get("name") + ":" + String.join(",", (ArrayList<String>) group.get("rights"));
            }).collect(Collectors.toList()));
        }

        ArrayList<String> clusterArray = (ArrayList<String>) binding.getParameters().get("custer");

        if(clusterArray!=null) {
                cluster = String.join(",", clusterArray);
        }*/
        
        try {
            //kafkaBoshPlatformService.deleteKafkaUser(serviceInstance, plan, credhubClient.getUser(serviceInstance, binding.getId()).getUsername(), topics, groups, cluster);
            kafkaBoshPlatformService.deleteKafkaUser(serviceInstance, plan, credhubClient.getUser(serviceInstance, binding.getId()).getUsername());
        } catch (Exception e) {
            e.printStackTrace();
            throw new PlatformException("Can not delete User");
        }
        credhubClient.deleteCredentials(serviceInstance.getId(), binding.getId());
    }

    @Override
    protected RouteBinding bindRoute(ServiceInstance serviceInstance, String route) {
        throw new UnsupportedOperationException();
    }
}
