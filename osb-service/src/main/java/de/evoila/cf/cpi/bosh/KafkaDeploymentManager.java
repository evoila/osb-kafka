package de.evoila.cf.cpi.bosh;

import de.evoila.cf.broker.bean.BoshProperties;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.model.credential.CertificateCredential;
import de.evoila.cf.broker.model.credential.PasswordCredential;
import de.evoila.cf.broker.util.MapUtils;
import de.evoila.cf.cpi.CredentialConstants;
import de.evoila.cf.cpi.bosh.deployment.DeploymentManager;
import de.evoila.cf.cpi.bosh.deployment.manifest.InstanceGroup;
import de.evoila.cf.cpi.bosh.deployment.manifest.Manifest;
import de.evoila.cf.security.credentials.CredentialStore;
import org.springframework.core.env.Environment;
import org.springframework.credhub.support.certificate.CertificateParameters;
import org.springframework.credhub.support.certificate.ExtendedKeyUsage;

import javax.swing.text.html.Option;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KafkaDeploymentManager extends DeploymentManager {

    private static final String KAFKA_INSTANCE_GROUP = "kafka";
    private static final String SECURE_CLIENT = "setup_secure_client_connection";
    private static final String ADMIN_PASSWORD = "admin_password";
    private static final String SSL_CA = "certificate_authorities";
    private static final String SSL_CERT = "certificate";
    private static final String SSL_KEY = "key";
    private static final String ORGANIZATION = "evoila";

    private CredentialStore credentialStore;

    KafkaDeploymentManager(BoshProperties boshProperties, Environment environment, CredentialStore credentialStore) {
        super(boshProperties, environment);
        this.credentialStore = credentialStore;
    }

    @Override
    protected void replaceParameters(ServiceInstance serviceInstance, Manifest manifest, Plan plan, Map<String, Object> customParameters, boolean isUpdate) {
        HashMap<String, Object> properties = new HashMap<>();
        if(customParameters != null && !customParameters.isEmpty())
            properties.putAll(customParameters);

        if(customParameters != null && !customParameters.isEmpty()) {

            Map<String, Object> kafkaProperties = (Map<String, Object>) manifestProperties(KAFKA_INSTANCE_GROUP, manifest).get("kafka");
            Map<String, Object> kafkaSecurity = (Map<String, Object>) kafkaProperties.get("security");

            kafkaSecurity.put(SECURE_CLIENT, true);

            PasswordCredential passwordCredential = credentialStore.createPassword(serviceInstance.getId(), CredentialConstants.ADMIN_PASSWORD);

            kafkaSecurity.put(ADMIN_PASSWORD, passwordCredential.getPassword());

            HashMap<String, Object> kafkaSsl = (HashMap<String, Object>) kafkaSecurity.get("ssl");


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
            
            if (customParameters != null) {
                for (Map.Entry parameter : customParameters.entrySet()) {
                    Map<String, Object> manifestProperties = manifestProperties(parameter.getKey().toString(), manifest);

                    if (manifestProperties != null)
                        MapUtils.deepMerge(manifestProperties, customParameters);
                }
            }
        }

        this.updateInstanceGroupConfiguration(manifest, plan);
    }

    private Map<String, Object> manifestProperties(String instanceGroup, Manifest manifest) {
        Optional<InstanceGroup> optionalInstanceGroup = manifest
                .getInstanceGroups()
                .stream()
                .filter(i -> {
                    if (i.getName().equals(instanceGroup))
                        return true;
                    return false;
                }).findFirst();
        if (optionalInstanceGroup.isPresent()){
                return optionalInstanceGroup.get().getProperties();
        } else {
            return null;
        }
    }
}
