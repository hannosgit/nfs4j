package testutil;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class NfsServerTestcontainer<SELF extends NfsServerTestcontainer<SELF>> extends GenericContainer<SELF> {

    private final String exportPath;

    public NfsServerTestcontainer(String exportPath) {
        super(new ImageFromDockerfile("nfs-server:latest",false)
                .withFileFromClasspath("Dockerfile", "Dockerfile")
                .withFileFromClasspath("start.sh", "start.sh")
        );
        this.withExposedPorts(2049)
                .withEnv("GANESHA_EXPORT_ID", "1")
                .withEnv("GANESHA_EXPORT", exportPath)
                .withEnv("GANESHA_PSEUDO_PATH", exportPath)
                .withEnv("GANESHA_ACCESS", "*")
                .withEnv("GANESHA_ROOT_ACCESS", "*");
        this.exportPath = exportPath;
    }

    public String getExportPath() {
        return exportPath;
    }
}
