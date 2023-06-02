#!/usr/bin/env bash
#
# cluster.sh [ARGS]
#
# This script will create a new local kind cluster in an idempotent way, i.e.,
# it is safe to call this script multiple times with the same arguments.

source "${BASH_SOURCE[0]%/*}/lib/common.sh"

set -euo pipefail

export COLOR=on

# usage yields the usage string of this script
usage() {
    cat <<EOF

Usage:
    cluster.sh [ARGS]

Arguments:
    -c        turn off colored output (e.g., in CICD)
    -h        print help message and exit
    -f FILE   path to your kind yaml configuration file, default is \$PWD/kind.yaml

EOF
}

# (0) process pre-conditions

kind_file="${PWD}/kind.yaml"

while getopts ":chf:" opt; do
    case "$opt" in
        c)
            unset color;;
        h)
            usage; exit 0;;
        f)
            kind_file=$OPTARG;;
        *)
            common::err "invalid option: ${OPTARG}" "$(usage)"; exit 1;;
    esac
done
shift $(( OPTIND - 1 ))

[[ ! -e "${kind_file}" ]] && { common::err "kind file \"${kind_file}\" does not exist" "$(usage)"; exit 1; }


dependencies=(
    'kind'
    'kubectl'
)
common::check_dependencies "${dependencies[@]}"

# (1) idempotency check
cluster_name="$(awk '/name:/ { print $2; }' kind.yaml)"; readonly cluster_name;

if kind get clusters | grep "${cluster_name}" &> /dev/null; then
  echo "kind cluster ${cluster_name} already exists, exiting"
  exit 0
fi

# (2) create cluster
echo "creating kind cluster ${cluster_name}"

args=(
  'create'
  'cluster'
  "--config=${kind_file}"
)

kind "${args[@]}"

# (3) install nginx ingress support (see https://kind.sigs.k8s.io/docs/user/ingress/)

printf '  node ready\r'
kubectl wait --for=condition=Ready nodes --all --timeout=90s --context="kind-${cluster_name}" &> /dev/null \
  || { printf "  node not ready, aborting install\n"; exit 1; }
printf '✔  node ready\n'

printf '  nginx ingress\r'

kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml --context="kind-${cluster_name}" &> /dev/null \
  || { printf "  nginx ingress installation failed, aborting\n"; exit 1; }

args=(
  'rollout'
  'status'
  '--namespace=ingress-nginx'
  '--timeout=90s'
  "--context=kind-${cluster_name}"
  'deployment/ingress-nginx-controller'
)
kubectl "${args[@]}" &> /dev/null \
  || { printf "  nginx ingress not ready, aborting\n"; exit 1; }

printf '✔  nginx ingress\n'
