FROM eclipse-temurin:17.0.12_7-jre-focal

COPY basic-server.jar /opt/nfs-server/basic-server.jar

WORKDIR /opt/nfs-server

RUN chmod 777 basic-server.jar

RUN mkdir shared

RUN chmod 777 shared

COPY exports /opt/nfs-server/exports

RUN chmod 777 exports

EXPOSE 9051/tcp

CMD java -jar basic-server.jar -root shared -exports exports -port 9051 -nfsvers 3 -with-portmap
