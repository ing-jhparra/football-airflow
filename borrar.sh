#!/bin/bash
docker system prune
astro dev stop
astro airflow stop
astro dev kill
