package de.evoila.cf.cpi.bosh;

import de.evoila.cf.broker.bean.BoshProperties;
import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.model.DashboardClient;
import de.evoila.cf.broker.model.Plan;
import de.evoila.cf.broker.model.ServerAddress;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.repository.PlatformRepository;
import de.evoila.cf.broker.service.CatalogService;
import de.evoila.cf.broker.service.availability.ServicePortAvailabilityVerifier;
import io.bosh.client.deployments.Deployment;
import io.bosh.client.errands.ErrandSummary;
import io.bosh.client.tasks.Task;
import io.bosh.client.vms.Vm;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
@ConditionalOnBean(BoshProperties.class)
public class KafkaBoshPlatformService extends BoshPlatformService {

    public static final int KAFKA_PORT = 9092;
    public static final int KAFKA_PORT_SSL = 9093;
    public static final int ZOOKEEPER_PORT = 2181;
    private static final String KAFKA_JOB_NAME = "kafka";


    KafkaBoshPlatformService(PlatformRepository repository, CatalogService catalogService, ServicePortAvailabilityVerifier availabilityVerifier, BoshProperties boshProperties, Optional<DashboardClient> dashboardClient) {
        super(repository, catalogService, availabilityVerifier, boshProperties, dashboardClient, new KafkaDeploymentManager(boshProperties));
    }

    public void runCreateErrands(ServiceInstance instance, Plan plan, Deployment deployment, Observable<List<ErrandSummary>> errands) throws PlatformException {
        Task task = super.connection.connection().errands().runErrand(deployment.getName(), "kafka-smoke-tests").toBlocking().first();
        super.waitForTaskCompletion(task);

    }

    @Override
    protected void updateHosts(ServiceInstance in, Plan plan, Deployment deployment) {

        List<Vm> vms = connection.connection().vms().listDetails(BoshPlatformService.DEPLOYMENT_NAME_PREFIX + in.getId()).toBlocking().first();
        if (in.getHosts() == null)
            in.setHosts(new ArrayList<>());

        in.getHosts().clear();

        vms.forEach(vm -> {
            ServerAddress serverAddress;

            if (vm.getJobName().equals(KAFKA_JOB_NAME)) {
                serverAddress = new ServerAddress("Kafka-" + vm.getIndex(), vm.getIps().get(0), KAFKA_PORT);
            } else {
                serverAddress = new ServerAddress("Zookeeper-" + vm.getIndex(), vm.getIps().get(0), ZOOKEEPER_PORT);
            }

            in.getHosts().add(serverAddress);
        });
    }

    @Override
    public void postDeleteInstance(ServiceInstance serviceInstance) throws PlatformException {
    }
}
