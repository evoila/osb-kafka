package de.evoila.cf.cpi.bosh;

import de.evoila.cf.broker.bean.BoshProperties;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.model.credential.CertificateCredential;
import de.evoila.cf.broker.model.credential.PasswordCredential;
import de.evoila.cf.cpi.bosh.deployment.DeploymentManager;
import de.evoila.cf.cpi.bosh.deployment.manifest.Manifest;
import de.evoila.cf.security.credentials.credhub.CredhubClient;
import org.springframework.core.env.Environment;
import org.springframework.credhub.support.certificate.CertificateParameters;
import org.springframework.credhub.support.certificate.ExtendedKeyUsage;

import java.util.*;

public class KafkaDeploymentManager extends DeploymentManager {

    private static final String KAFKA_INSTANCE_GROUP = "kafka";
    private static final String ZOOKEEPER_INSTANCE_GROUP = "zookeeper";
    private static final String SECURE_CLIENT = "setup_secure_client_connection";
    private static final String ADMIN_PASSWORD = "admin_password";
    private static final String SSL_CA = "certificate_authorities";
    private static final String SSL_CERT = "certificate";
    private static final String SSL_KEY = "key";
    private static final String ORGANIZATION = "evoila";

    private CredhubClient credhubClient;

    KafkaDeploymentManager(BoshProperties boshProperties, Environment environment, CredhubClient credhubClient){
        super(boshProperties, environment);
        this.credhubClient = credhubClient;
    }

    @Override
    protected void replaceParameters(ServiceInstance serviceInstance, Manifest manifest, Plan plan, Map<String, Object> customParameters, boolean isUpdate) {
        HashMap<String, Object> properties = new HashMap<>();
        if(customParameters != null && !customParameters.isEmpty())
            properties.putAll(customParameters);

        Map<String, Object> zookeeperProperties = manifest.getInstanceGroups()
                .stream()
                .filter(i -> i.getName().equals(ZOOKEEPER_INSTANCE_GROUP))
                .findAny().get().getJobs()
                .stream()
                .filter(j -> j.getName().equals(ZOOKEEPER_INSTANCE_GROUP))
                .findAny().get().getProperties();

        HashMap<String, Object> zookeeperSecurity = (HashMap<String, Object>) zookeeperProperties.get("security");

        Map<String, Object> kafkaProperties = manifest.getInstanceGroups()
                .stream()
                .filter(i -> i.getName().equals(KAFKA_INSTANCE_GROUP))
                .findAny().get().getJobs()
                .stream()
                .filter(j -> j.getName().equals(KAFKA_INSTANCE_GROUP))
                .findAny().get().getProperties();

        HashMap<String, Object> kafkaSecurity = (HashMap<String, Object>) kafkaProperties.get("security");

        kafkaSecurity.put(SECURE_CLIENT, true);

        zookeeperSecurity.put(SECURE_CLIENT, true);

        PasswordCredential passwordCredential = credhubClient.createPassword(serviceInstance.getId(), "admin_password");

        kafkaSecurity.put(ADMIN_PASSWORD, passwordCredential.getPassword());

        zookeeperSecurity.put(ADMIN_PASSWORD, passwordCredential.getPassword());

        HashMap<String, Object> ssl = (HashMap<String, Object>) kafkaSecurity.get("ssl");

        CertificateCredential certificateCredential = credhubClient.createCertificate(serviceInstance.getId(), "transport_ssl",
                CertificateParameters.builder()
                        .organization(ORGANIZATION)
                        .selfSign(true)
                        .certificateAuthority(true)
                        .extendedKeyUsage(ExtendedKeyUsage.CLIENT_AUTH, ExtendedKeyUsage.SERVER_AUTH)
                        .build());

        ssl.put(SSL_CA, certificateCredential.getCertificateAuthority());
        ssl.put(SSL_CERT, certificateCredential.getCertificate());
        ssl.put(SSL_KEY, certificateCredential.getPrivateKey());

        this.updateInstanceGroupConfiguration(manifest, plan);
    }

}
