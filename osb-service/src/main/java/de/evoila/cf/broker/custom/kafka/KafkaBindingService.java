/**
 *
 */
package de.evoila.cf.broker.custom.kafka;

import de.evoila.cf.broker.model.*;
import de.evoila.cf.broker.repository.BindingRepository;
import de.evoila.cf.broker.repository.RouteBindingRepository;
import de.evoila.cf.broker.repository.ServiceDefinitionRepository;
import de.evoila.cf.broker.repository.ServiceInstanceRepository;
import de.evoila.cf.broker.service.HAProxyService;
import de.evoila.cf.broker.service.impl.BindingServiceImpl;
import de.evoila.cf.cpi.bosh.KafkaBoshPlatformService;
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

    public KafkaBindingService(BindingRepository bindingRepository, ServiceDefinitionRepository serviceDefinitionRepository, ServiceInstanceRepository serviceInstanceRepository, RouteBindingRepository routeBindingRepository, HAProxyService haProxyService) {
        super(bindingRepository, serviceDefinitionRepository, serviceInstanceRepository, routeBindingRepository, haProxyService);
    }

    @Override
    protected void unbindService(ServiceInstanceBinding binding, ServiceInstance serviceInstance, Plan plan) {

    }

    @Override
    public ServiceInstanceBinding getServiceInstanceBinding(String id) {
        throw new UnsupportedOperationException();
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


    @Override
    protected Map<String, Object> createCredentials(String bindingId, ServiceInstanceBindingRequest serviceInstanceBindingRequest,
                                                    ServiceInstance serviceInstance, Plan plan, ServerAddress host) {

        Map<String, Object> credentials = new HashMap<>();

        List<String> brokers = new LinkedList<>();
        List<String> zookeepers = new LinkedList<>();

        serviceInstance.getHosts().forEach(instance -> {
            if (instance.getPort() == KafkaBoshPlatformService.KAFKA_PORT) {
                brokers.add(instance.getIp() + ":" + instance.getPort());
            } else if (instance.getPort() == KafkaBoshPlatformService.ZOOKEEPER_PORT) {
                zookeepers.add(instance.getIp() + ":" + instance.getPort());
            }
        });

        credentials.put(KAFKA_BROKERS, brokers);
        credentials.put(DEFAULT_BROKER_PORT, KafkaBoshPlatformService.KAFKA_PORT);
        credentials.put(ZOOKEEPER_BROKERS, zookeepers);
        credentials.put(DEFAULT_ZK_PORT, KafkaBoshPlatformService.ZOOKEEPER_PORT);

        return credentials;
    }

}
