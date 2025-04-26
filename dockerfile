sudo docker run -d \
  -p 1521:1521 \
  -p 81:81 \
  -v ~/h2-data:/opt/h2-data \
  -e H2_OPTIONS="-ifNotExists -tcp -tcpAllowOthers -web -webAllowOthers" \
  --name h2-database \
  oscarfonts/h2

  