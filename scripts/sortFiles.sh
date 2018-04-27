#!/bin/bash
(head -n 1 $1 && tail -q -n +2 $@ | sort -t "|" -k2n | awk 'NF { print $0 }')
