# Flight Reservation System - Application that uses distributed database for managing data.

Used:
- Python 3.10.16
- Docker version 27.5.1
- Cassandra 4.1 

### Commands in terminal:
- starting (detached container): <br>
  `docker-compose up -d`
- closing: database connections  <br>
  `docker-compose down`
- sctipt for simple preview:
  creating example users, flights, reservations, updating reservation, running 3 stress tests <br>
  `python3 containers.py`
- run interactive menu <br>
  `python3 menu.py` <br>

  ![screenshot of view after running this command](menu.png "menu") <br>
  Airport Worker View means being able to see all information in the system <br>


### Clean up  after use:
- remove all data from containers: <br>
  `docker volume prune`
- get rid of inactive containers: <br>
  `docker container prune`

 
