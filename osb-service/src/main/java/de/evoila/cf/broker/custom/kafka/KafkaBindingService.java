/**
 *
 */
package de.evoila.cf.broker.custom.kafka;

import de.evoila.cf.broker.model.RouteBinding;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.ServiceInstanceBinding;
import de.evoila.cf.broker.model.ServiceInstanceBindingRequest;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.repository.*;
import de.evoila.cf.broker.service.AsyncBindingService;
import de.evoila.cf.broker.service.HAProxyService;
import de.evoila.cf.broker.service.impl.BindingServiceImpl;
import de.evoila.cf.cpi.bosh.KafkaBoshPlatformService;
import de.evoila.cf.security.credentials.credhub.CredhubClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Johannes Hiemer.
 */
@Service
public class KafkaBindingService extends BindingServiceImpl {

    private Logger log = LoggerFactory.getLogger(KafkaBindingService.class);

    private static String KAFKA_BROKERS = "brokers";
    private static String ZOOKEEPER_BROKERS = "zkNodes";
    private static String DEFAULT_BROKER_PORT = "defaultBrokerPort";
    private static String DEFAULT_ZK_PORT = "defaultZkPort";
    private static String USERNAME = "user";
    private static String PASSWORD = "password";

    private CredhubClient credhubClient;

    private KafkaBoshPlatformService kafkaBoshPlatformService;

    public KafkaBindingService(BindingRepository bindingRepository, ServiceDefinitionRepository serviceDefinitionRepository,
                               ServiceInstanceRepository serviceInstanceRepository, RouteBindingRepository routeBindingRepository,
                               HAProxyService haProxyService, JobRepository jobRepository, AsyncBindingService asyncBindingService,
                               PlatformRepository platformRepository, CredhubClient credhubClient, KafkaBoshPlatformService kafkaBoshPlatformService) {
        super(bindingRepository, serviceDefinitionRepository, serviceInstanceRepository, routeBindingRepository, haProxyService, jobRepository, asyncBindingService, platformRepository);
        this.credhubClient = credhubClient;
        this.kafkaBoshPlatformService = kafkaBoshPlatformService;
    }

    @Override
    protected Map<String, Object> createCredentials(String bindingId, ServiceInstanceBindingRequest serviceInstanceBindingRequest,
                                                    ServiceInstance serviceInstance, Plan plan, ServerAddress host) {

        Map<String, Object> credentials = new HashMap<>();

        List<String> brokers = new LinkedList<>();
        List<String> zookeepers = new LinkedList<>();

        serviceInstance.getHosts().forEach(instance -> {
            if (instance.getPort() == KafkaBoshPlatformService.KAFKA_PORT_SSL) {
                brokers.add(instance.getIp() + ":" + instance.getPort());
            } else if (instance.getPort() == KafkaBoshPlatformService.ZOOKEEPER_PORT) {
                zookeepers.add(instance.getIp() + ":" + instance.getPort());
            }
        });

        credhubClient.createUser(serviceInstance, bindingId);

        String username = credhubClient.getUser(serviceInstance, bindingId).getUsername();
        String password = credhubClient.getUser(serviceInstance, bindingId).getPassword();

        try {
            kafkaBoshPlatformService.createKafkaUser(serviceInstance, plan, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        credentials.put(KAFKA_BROKERS, brokers);
        credentials.put(DEFAULT_BROKER_PORT, KafkaBoshPlatformService.KAFKA_PORT_SSL);
        credentials.put(ZOOKEEPER_BROKERS, zookeepers);
        credentials.put(DEFAULT_ZK_PORT, KafkaBoshPlatformService.ZOOKEEPER_PORT);
        credentials.put(USERNAME, username);
        credentials.put(PASSWORD, password);

        return credentials;
    }

    @Override
    protected void unbindService(ServiceInstanceBinding binding, ServiceInstance serviceInstance, Plan plan) {
        try {
            kafkaBoshPlatformService.deleteKafkaUser(serviceInstance, plan, credhubClient.getUser(serviceInstance, binding.getId()).getUsername());
        } catch (Exception e) {
            e.printStackTrace();
        }
        credhubClient.deleteCredentials(serviceInstance.getId(), binding.getId());
    }


    @Override
    protected ServiceInstanceBinding bindServiceKey(String bindingId, ServiceInstanceBindingRequest serviceInstanceBindingRequest,
                                                    ServiceInstance serviceInstance, Plan plan,
                                                    List<ServerAddress> externalAddresses) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected RouteBinding bindRoute(ServiceInstance serviceInstance, String route) {
        throw new UnsupportedOperationException();
    }
}
