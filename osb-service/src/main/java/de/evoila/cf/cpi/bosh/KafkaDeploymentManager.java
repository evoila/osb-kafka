package de.evoila.cf.cpi.bosh;

import de.evoila.cf.broker.bean.BoshProperties;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.model.credential.CertificateCredential;
import de.evoila.cf.broker.model.credential.PasswordCredential;
import de.evoila.cf.cpi.CredentialConstants;
import de.evoila.cf.cpi.bosh.deployment.DeploymentManager;
import de.evoila.cf.cpi.bosh.deployment.manifest.Manifest;
import de.evoila.cf.security.credentials.CredentialStore;
import org.springframework.core.env.Environment;
import org.springframework.credhub.support.certificate.CertificateParameters;
import org.springframework.credhub.support.certificate.ExtendedKeyUsage;

import java.util.HashMap;
import java.util.Map;

public class KafkaDeploymentManager extends DeploymentManager {

    private static final String KAFKA_INSTANCE_GROUP = "kafka";
    private static final String KAFKA_SMOKE_TEST_INSTANCE = "kafka-smoke-test";
    private static final String ZOOKEEPER_INSTANCE_GROUP = "zookeeper";
    private static final String SECURE_CLIENT = "setup_secure_client_connection";
    private static final String ADMIN_PASSWORD = "admin_password";
    private static final String SSL_CA = "certificate_authorities";
    private static final String SSL_CERT = "certificate";
    private static final String SSL_KEY = "key";
    private static final String ORGANIZATION = "evoila";

    private CredentialStore credentialStore;

    KafkaDeploymentManager(BoshProperties boshProperties, Environment environment, CredentialStore credentialStore){
        super(boshProperties, environment);
        this.credentialStore = credentialStore;
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

        Map<String, Object> smokeTestProperties = manifest.getInstanceGroups()
                .stream()
                .filter(i -> i.getName().equals(KAFKA_SMOKE_TEST_INSTANCE))
                .findAny().get().getJobs()
                .stream()
                .filter(j -> j.getName().equals(KAFKA_SMOKE_TEST_INSTANCE))
                .findAny().get().getProperties();

        HashMap<String, Object> smokeTestSecurity = (HashMap<String, Object>) smokeTestProperties.get("security");

        kafkaSecurity.put(SECURE_CLIENT, true);
        zookeeperSecurity.put(SECURE_CLIENT, true);
        smokeTestSecurity.put(SECURE_CLIENT, true);

        PasswordCredential passwordCredential = credentialStore.createPassword(serviceInstance.getId(), CredentialConstants.ADMIN_PASSWORD);

        kafkaSecurity.put(ADMIN_PASSWORD, passwordCredential.getPassword());
        zookeeperSecurity.put(ADMIN_PASSWORD, passwordCredential.getPassword());
        smokeTestSecurity.put(ADMIN_PASSWORD, passwordCredential.getPassword());

        HashMap<String, Object> kafkaSsl = (HashMap<String, Object>) kafkaSecurity.get("ssl");
        HashMap<String, Object> smokeTestSsl = (HashMap<String, Object>) smokeTestSecurity.get("ssl");
        CertificateCredential certificateCredential = credentialStore.createCertificate(serviceInstance.getId(), CredentialConstants.TRANSPORT_SSL,
                CertificateParameters.builder()
                        .organization(ORGANIZATION)
                        .selfSign(true)
                        .certificateAuthority(true)
                        .extendedKeyUsage(ExtendedKeyUsage.CLIENT_AUTH, ExtendedKeyUsage.SERVER_AUTH)
                        .build());

        kafkaSsl.put(SSL_CA, certificateCredential.getCertificateAuthority());
        kafkaSsl.put(SSL_CERT, certificateCredential.getCertificate());
        kafkaSsl.put(SSL_KEY, certificateCredential.getPrivateKey());

        smokeTestSsl.put(SSL_CA, certificateCredential.getCertificateAuthority());
        smokeTestSsl.put(SSL_CERT, certificateCredential.getCertificate());
        smokeTestSsl.put(SSL_KEY, certificateCredential.getPrivateKey());

        this.updateInstanceGroupConfiguration(manifest, plan);
    }

}
