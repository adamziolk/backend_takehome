This project is for a small distributed data processing pipeline in the apache beam framework.

- `src/main.py` contains the pipeline code. This file has incomplete sections marked with `TODO` that you need to fill in.
- `src/main__test.py` contains test code for the pipeline. Running these tests will allow you to debug your code.
- `src/schema.sql` contains the postgres sql used to define a schema and insert some data.

Go through these steps to get started:

- Install `docker`: https://docs.docker.com/get-docker/
- Install `docker-compose`: https://docs.docker.com/compose/install/
- Run `docker-compose up` in this folder. The first time you run this command, a docker image defined by `Dockerfile` will be built (and it will take a while!), which provides a python environment to run the pipeline. A postgres database will also be started that will be initialized with `src/schema.sql`.
- In a separate shell, run `./test.sh`. This script runs the `pytest` command inside the python docker container currently running through `docker-compose`. Putting `-k name_of_test_function` at the end of the `pytest` call in `./test.sh` will allow you to only run one test at a time.

If you stop the `docker-compose up` command, and then run `docker-compose down`, it will destroy the containers, resetting your database to the initial state.

You will likely find these resources useful:

- Documentation for python's `date` type: https://docs.python.org/3/library/datetime.html#date-objects
- Beam transform reference: https://beam.apache.org/documentation/transforms/python/overview/
- Beam programming guide (don't read this entire thing, just enough to understand what's going on, such as the "Overview", "PCollections", and "Transforms" sections): https://beam.apache.org/documentation/programming-guide/

---

You can open a database client tool in which you can test sql queries with: `docker exec -it pipeline bash -c 'PGPASSWORD=password psql -U user -h db database'`

For example:

```bash
docker exec -it pipeline bash -c 'PGPASSWORD=password psql -U user -h db database'

psql (13.3 (Debian 13.3-1), server 13.4)
Type "help" for help.

database=# select * from product;
 id | display_name | price
----+--------------+-------
  1 | Apple        |  4.50
  2 | Banana       |  3.00
  3 | Orange       |  2.50
  4 | Pear         |  3.50
  5 | Plum         |  2.00
  6 | Lemon        |  1.00
  7 | Lime         |  1.00
  8 | Cherry       |  0.50
  9 | Raspberry    |  5.00
 10 | Grapefruit   |  6.00
 11 | Kiwi         |  3.25
 12 | Mango        |  5.50
(12 rows)
```

Type `\q` or `ctrl+d` to exit the tool.
