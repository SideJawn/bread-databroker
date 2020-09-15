import sys
from sys import stdout
from flask import Flask, g, request
import json, traceback
## doc: https://neo4j.com/docs/api/python-driver/current/
import neo4j
from neo4j import GraphDatabase
import logging
app = Flask(__name__)
app.logger #  initialize logger

## Initialize neo4j driver and connect to neo4 database
try:
    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4jpass"))
    print('Successfullly initialized neo4j driver')
except:
    print('Error initializing neo4j driver')
    traceback.print_exc()
    sys.exit(1)

## Retrieve a db session
def get_db_session():
    if not hasattr(g, 'neo4j_db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db

## Close the db connection when app shuts down.
@app.teardown_appcontext
def close_db_session(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()

@app.route('/project', methods=['GET'])
def get_projects():
    category = request.args.get('category')
    num_recs = request.args.get('num_recs')
    if num_recs is not None:
        if category is not None:
            query = "MATCH (project:Project)-[:IS_CAT]->(category:Category {display_name: '" + category + "'}), (user:User)-[:MANAGE]->(project) RETURN project.display_name, project.status, project.created_ts, project.description, user.f_name, user.l_name LIMIT " + num_recs
        else:
            query = "MATCH (project:Project), (user:User)-[:MANAGE]->(project) RETURN project.display_name, project.status, project.created_ts, project.description, user.f_name, user.l_name LIMIT " + num_recs
        r = exe_query(query)
    else:
        r = { 'status_code': 'Number of records are missing' }

    return r

def exe_query(query):
    r = {}
    try:
        db = get_db_session()
        with db as session:
            with session.begin_transaction() as tx:
                results = tx.run(query)
                ## if a record is there (by peeking), process results
                if results.peek() is not None:
                    sorted_results = [record for record in results.data()]
                    r = {
                        'status_code': 'OK',
                        'results': sorted_results
                    }
                else:
                    r = { 'status_code': 'ERR_NOT_FOUND' }
    except neo4j.exceptions.ServiceUnavailable:
        #close_db_session(err)
        r = { 'status_code': 'ERR_DB_SRVC_UNAVAIL' }
    except neo4j.exceptions.SessionExpired:
        #close_db_session(err)
        r = { 'status_code': 'ERR_DB_SESSION_UNAVAIL' }
    # except:
    #     if CONFIG['neo4j']['debug_enabled']['default'] == True:
    #         traceback.print_exc()
    #     r = { 'status_code': 'ERR_DB_UNKNOWN'}

    return r