#!/usr/bin/env bash

set -o errexit -o nounset

for stack_name
do
    while true
    do
        stack_info="$(aws cloudformation describe-stacks --query "Stacks[?contains(@.StackName,'${stack_name}')].[StackName]" --output text)"
        if [[ -z "$stack_info" ]]
        then
            break
        fi

        echo "Waiting for ${stack_name} teardown…" >&2
        sleep 3
    done
done

echo "All stacks are gone"
