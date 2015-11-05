from rcm-img-docker01.build.eng.bos.redhat.com:5001/openshift3/ose-docker-registry
run mkdir /distribution
volume ["dist-build"]
cmd ["/bin/sh", "-c", "REGISTRY_URL=${DOCKER_REGISTRY_SERVICE_HOST}:${DOCKER_REGISTRY_SERVICE_PORT} /distribution/dockerregistry ${REGISTRY_CONFIGURATION_PATH}"]
