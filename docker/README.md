To run snovault in a docker container:

```
docker build -t insitro-snovault:latest .
docker run -p 6543:6543 -dit insitro-snovault:latest
```
(wait ~20 seconds for the snovault server to start up before viewing the snovault portal on localhost:6453 in your browser)
