docker run -it -p 6650:6650 -p 8080:8080 --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:2.5.0 bin/pulsar standalone
echo pulsar://localhost:6650
echo http://localhost:8080
