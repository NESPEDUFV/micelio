include .env
export

MAKEFLAGS += --no-print-directory
NS3_VERSION := $(shell cat ${NS3_HOME}/VERSION)
CONFIGURE_TARGET := ${NS3_HOME}/cmake-cache/Makefile
BUILD_TARGET := ${NS3_HOME}/build/scratch/${SIM_NAME}/ns$(NS3_VERSION)-${SIM_NAME}-${SIM_PROFILE}
SIM_SRC_FILES := $(shell for f in $$(find simulation); do test -f $$f && echo $$f; done)
ONTO_FILES := $(shell for f in $$(find ontology); do test -f $$f && echo $$f; done)
LIB_SRC_FILES := $(shell for f in $$(find micelio micelio-rdf micelio-derive micelio-ns3 nsrs); do test -f $$f && echo $$f; done)

.PHONY: run configure

all: $(BUILD_TARGET)

run: $(BUILD_TARGET)
	rsync -a --delete data ${NS3_HOME}/
	cd ${NS3_HOME} && LIBTORCH=${LIBTORCH} LD_LIBRARY_PATH=${LIBTORCH}/lib:${LD_LIBRARY_PATH} PYTORCH_ALLOC_CONF=${PYTORCH_ALLOC_CONF} ./ns3 run ${SIM_NAME}

configure: 
	cd ${NS3_HOME} && LIBTORCH=${LIBTORCH} LD_LIBRARY_PATH=${LIBTORCH}/lib:${LD_LIBRARY_PATH} ./ns3 configure --build-profile=${SIM_PROFILE} --with-brite=${BRITE_HOME} --enable-examples

$(BUILD_TARGET): $(SIM_SRC_FILES) ${NS3_HOME}/build/lib/libmicelio.a ${JENA_FUSEKI_HOME}/data/.ontology ${NS3_HOME}/src/brite/helper/brite-topology-helper.h ${NS3_HOME}/src/brite/helper/brite-topology-helper.cc
	mkdir -p ${NS3_HOME}/scratch/${SIM_NAME}
	rsync -a --delete simulation/ "${NS3_HOME}/scratch/${SIM_NAME}/"
	cd ${NS3_HOME} && LIBTORCH=${LIBTORCH} LD_LIBRARY_PATH=${LIBTORCH}/lib:${LD_LIBRARY_PATH} ./ns3 build ${SIM_NAME}
	touch $(BUILD_TARGET)

${NS3_HOME}/src/brite/helper/brite-topology-helper.%: simulation/helper/brite-topology-helper.%
	cp $< $@

${JENA_FUSEKI_HOME}/data/.ontology: $(ONTO_FILES)
	mkdir -p "${JENA_FUSEKI_HOME}/data"
	rsync -a --delete --exclude 'catalog-*.xml' ontology/ "${JENA_FUSEKI_HOME}/data/"
	touch "${JENA_FUSEKI_HOME}/data/.ontology"

${NS3_HOME}/build/lib/libmicelio.a: target/${BUILD_PROFILE}/libmicelio.a
	[ -f ${NS3_HOME}/build/lib/libmicelio.a ] || ln -s $$(realpath target/${BUILD_PROFILE}/libmicelio.a) ${NS3_HOME}/build/lib/libmicelio.a
	[ -f ${NS3_HOME}/build/lib/libnsrs.a ] || ln -s $$(realpath target/${BUILD_PROFILE}/libnsrs.a) ${NS3_HOME}/build/lib/libnsrs.a
	touch ${NS3_HOME}/build/lib/libmicelio.a

target/${BUILD_PROFILE}/libmicelio.a: $(LIB_SRC_FILES)
	touch simulation/simulation_brite.cc
	rsync -aL --delete target/cxxbridge/rust ${NS3_HOME}/build/include/
	rsync -aL --delete target/cxxbridge/nsrs ${NS3_HOME}/build/include/
	rsync -aL --delete nsrs/include ${NS3_HOME}/build/include/nsrs
	rsync -aL --delete target/cxxbridge/micelio-ns3 ${NS3_HOME}/build/include/
	NS3_HOME=${NS3_HOME} LIBTORCH=${LIBTORCH} RUSTFLAGS=-Zhigher-ranked-assumptions cargo +nightly build ${BUILD_FLAGS}
