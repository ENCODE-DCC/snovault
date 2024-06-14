#!/bin/bash
bin/dev-servers development.ini --app-name app --clear --init --load &
sleep 10
bin/pserve development.ini
