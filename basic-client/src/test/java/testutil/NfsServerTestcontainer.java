package testutil;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.wait.strategy.*;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class NfsServerTestcontainer extends GenericContainer<NfsServerTestcontainer> {

    public NfsServerTestcontainer() {
        super(new ImageFromDockerfile("nfsserver-testcontainer", false)
                .withFileFromClasspath("Dockerfile", "testcontainer/Dockerfile")
                .withFileFromClasspath("exports", "testcontainer/exports")
                .withFileFromClasspath("basic-server.jar", "testcontainer/basic-server.jar")
                .withFileFromString("folder/someFile.txt", "hello")
        );
        this.waitingFor(Wait.forLogMessage(".*NfsServerV3.*\\n",1));
        this.withExposedPorts(9051);
    }

    public String getExport(){
        return "/data";
    }

}
