#!/bin/bash

# Execute the suite of tests.  It is assumed the following environment variables will
# have been set up beforehand:
# -  DATASTORE_TYPE + other calico datastore envs
# -  LOGPATH
execute_test_suite() {
    # This is needed for use in the keys of our templates, and the sed commands
    # in utils.sh use them to create the toml files.
    export NODENAME="kube-master"
    
    # Make sure the log and rendered templates paths are created and old test run data is
    # deleted.
    mkdir -p $LOGPATH
    mkdir -p $LOGPATH/rendered
    rm $LOGPATH/log* || true
    rm $LOGPATH/rendered/*.cfg || true

    # Download the templates from calico and build the tomls based on $NODENAME
    download_templates_from_calico
    build_tomls_for_node
    
    # Run confd in one-shot mode a few times and then start it as a background process.
    echo "Running confd to initialise templates"
    confd -confdir=/etc/calico/confd -onetime -log-level=debug >$LOGPATH/log1 2>&1 || true
    confd -confdir=/etc/calico/confd -onetime -log-level=debug >$LOGPATH/log3 2>&1 || true
    echo "Running confd as background process"
    confd -confdir=/etc/calico/confd -log-level=debug >$LOGPATH/log4 2>&1 &
    CONFD_PID=$!
    echo "Running with PID " $CONFD_PID

    # Note that changes to the node to node mesh config option will result in a restart of
    # confd, so order the tests accordingly.  Since the default nodeToNodeMeshEnabled setting
    # is true, perform the mesh tests first.  Then run the explicit peering tests - we should
    # see confd terminate when we turn of the mesh.
    run_individual_test 'mesh/ipip-always'
    run_individual_test 'mesh/ipip-cross-subnet'
    run_individual_test 'mesh/ipip-off'
    
    # Turn the node-mesh off.  This will cause confd to terminate through one of it's own
    # template updates.  Check that it does.
    turn_mesh_off
    check_pid_exits $CONFD_PID

    # Run as single shot first a few times to make sure templates are updated, and then re-start a background
    # confd.
    echo "Restarting confd"
    confd -confdir=/etc/calico/confd -onetime -log-level=debug >$LOGPATH/log5 2>&1 || true
    confd -confdir=/etc/calico/confd -onetime -log-level=debug >$LOGPATH/log7 2>&1 || true
    confd -confdir=/etc/calico/confd -log-level=debug >$LOGPATH/log8 2>&1 &
    CONFD_PID=$!

    # Run the explicit peering tests.
    run_individual_test 'explicit_peering/global'
    run_individual_test 'explicit_peering/specific_node'

    # Turn the node-mesh back on.  This will cause confd to terminate through one of it's own
    # template updates.  Check that it does.
    turn_mesh_on
    check_pid_exits $CONFD_PID
}

# Turn the node-to-node mesh off.
turn_mesh_off() {
    calicoctl apply -f - <<EOF
kind: BGPConfiguration
apiVersion: projectcalico.org/v2
metadata:
  name: default
spec:
  nodeToNodeMeshEnabled: false
EOF
}

# Turn the node-to-node mesh on.
turn_mesh_on() {
    calicoctl apply -f - <<EOF
kind: BGPConfiguration
apiVersion: projectcalico.org/v2
metadata:
  name: default
spec:
  nodeToNodeMeshEnabled: true
EOF
}

run_individual_test() {
    testdir=$1

    # Populate Calico using calicoctl to load the input.yaml test data.
    echo "Populating calico with test data using calicoctl: " $testdir
    calicoctl apply -f /tests/mock_data/calicoctl/${testdir}/input.yaml

    # Check the confd templates are updated.
    test_confd_templates $testdir

    # Remove any resource that does not need to be persisted due to test environment
    # limitations.
    echo "Preparing Calico data for next test"
    calicoctl delete -f /tests/mock_data/calicoctl/${testdir}/delete.yaml
}

# get_templates attempts to grab the latest templates from the calico repo
download_templates_from_calico() {
    repo_dir="/node-repo"
    if [ ! -d ${repo_dir} ]; then
        echo "Getting latest confd templates from calico repo"
        git clone https://github.com/projectcalico/calico.git ${repo_dir}
        ln -s ${repo_dir}/calico_node/filesystem/etc/calico/ /etc/calico
    fi
}

build_tomls_for_node() {
    echo "Building initial toml files"
    # This is pulled from the calico_node rc.local script, it generates these three
    # toml files populated with the $NODENAME var set in calling script.
    sed "s/NODENAME/$NODENAME/" /etc/calico/confd/templates/bird6_aggr.toml.template > /etc/calico/confd/conf.d/bird6_aggr.toml
    sed "s/NODENAME/$NODENAME/" /etc/calico/confd/templates/bird_aggr.toml.template > /etc/calico/confd/conf.d/bird_aggr.toml
    sed "s/NODENAME/$NODENAME/" /etc/calico/confd/templates/bird_ipam.toml.template > /etc/calico/confd/conf.d/bird_ipam.toml

    # Need to pause as running confd immediately after might result in files not being present.
    sync
}

# Check that the process with the specified pid exits.
# $pid is process ID to monitor.
check_pid_exits() {
    # Compare the templates until they match (for a max of 10s).
    pid=$1
    echo "Checking confd exits (PID ${pid})"

    for i in $(seq 1 10); do ps -o pid | grep -w $pid 1>/dev/null 2>&1 && sleep 1 || break; done

    if ps -o pid | grep -w $pid 1>/dev/null 2>&1; then
      echo "Failed: Expecting confd to have exited, but has not."
      ps
      exit 1
    fi
}

# Tests that confd generates the required set of templates for the test.
# $1 would be the tests you want to run e.g. mesh/global
test_confd_templates() {
    # Compare the templates until they match (for a max of 10s).
    testdir=$1
    for i in $(seq 1 10); do echo "comparing templates attempt $i" && compare_templates $testdir 0 && break || sleep 1; done
    compare_templates $testdir 1
}

# Compares the generated templates against the known good templates
# $1 would be the tests you want to run e.g. mesh/global
# $2 is whether or not we should output the diff results (0=no)
compare_templates() {
    # Check the generated templates against known compiled templates.
    testdir=$1
    output=$2
    rc=0
    for f in `ls /tests/compiled_templates/${testdir}`; do
        if ! diff -q /tests/compiled_templates/${testdir}/${f} /etc/calico/confd/config/${f} 1>/dev/null 2>&1; then
            if [ $output -ne 0 ]; then
                echo "Failed: $f templates do not match, showing diff of expected vs received"
                set +e
                diff /tests/compiled_templates/${testdir}/${f} /etc/calico/confd/config/${f}
                echo "Copying confd rendered output to ${LOGPATH}/rendered/${f}"
                cp /etc/calico/confd/config/${f} ${LOGPATH}/rendered/${f}
                ps
                set -e
            fi
            rc=1
        fi
    done
    return $rc
}
