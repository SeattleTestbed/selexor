"""
<Program Name>
  selexordatabase.py

<Started>
  July 7, 2012

<Author>
  leon.wlaw@gmail.com
  Leonard Law

<Purpose>
  Probes various services to acquire the latest information about all vessels.

  Nodes Table:
  [nodelocation][nodekey]
  nodelocation is IP:Port
  nodekey is the public key that uniquely identifies each node.

  vessels Table:
  [nodelocation][vesselname]

  userkey Table
  [nodelocation][vesselname][Userkey]

  location Table:
  [nodelocation][country_code][city][longitude][latitude]

  vesselports Table:
  [nodelocation][vesselname][port]

<Dependencies>
  MySQLdb Python Module
  MySQL server
  MySQL client
  MySQL mod python
"""

import selexorhelper
import repyhelper
# Keep the mobules in their own namespace
repyhelper.translate_and_import("geoip_client.repy")
repyhelper.translate_and_import('nmclient.repy')
repyhelper.translate_and_import('advertise.repy')
repyhelper.translate_and_import('rsa.repy')
import settings
import sys
import threading
import time
import os
import traceback
import MySQLdb





# The default value for a vessel's ip_change_count.
# It is incremented everytime the node's IP address changes.
# Therefore, when a node's IP is first registered, the ip_change_count is incremented to 0.
INIT_IP_CHANGE_COUNT = -1


def autoretry_mysql_command(cursor, command):
  """
  <Purpose>
    Inserts the specified command into the command queue, and returns its
    result when the command is executed.  This is used to prevent deadlocks
    that would occur due to simultaneous db accesses.
  <Arguments>
    command: The command to send to the database.
  <Side Effects>
    Executes the specified MySQL statement.
  <Exceptions>
    None
  <Returns>
    None
  """
  while True:
    try:
      result = cursor.execute(command)
      return result
    except MySQLdb.OperationalError, e:
      if (e.args == (1213, 'Deadlock found when trying to get lock; try restarting transaction') or
          e.args == (1205, 'Lock wait timeout exceeded; try restarting transaction')):
        continue
      raise




def format_geoinfo(geoinfo):
  '''
  <Purpose>
    Changes the geoinfo to a format that is consistent with the MySQL db.
  <Side Effects>
    The contents of 'country_code' and 'city' will be set to lower case, if they
      exist.

  '''
  for location_type in geoinfo:
    if type(geoinfo[location_type]) == type(''):
      geoinfo[location_type] = geoinfo[location_type].lower()


def probe_for_vessels():
  print "Probing for vessels..."
  # Look up as many values as possible
  nodes_to_check = advertise_lookup(nodestate_transition_key, maxvals=2**32)
  logger.info("Found " +str(len(nodes_to_check))+ " nodes")

  update_threads = []
  for thread_no in range(settings.num_probe_threads):
    thread = threading.Thread(
        target = contact_vessels_and_update_database,
        args=(configuration, nodes_to_check))
    # Allow threads to be terminated by a CTRL+C
    thread.daemon = True
    update_threads.append(thread)
    thread.start()

  # Wait until all threads finish running
  # Needed so that CTRL+C is not blocked by the join() calls
  while True:
    threads_remaining = 0
    for thread in update_threads:
      thread.join(0)
      if thread.isAlive():
        threads_remaining += 1
    if threads_remaining == 0:
      break
    time.sleep(1)

  print "Update complete"
  logger.info("Finished probing.")


def create_database():
  """
  Creates any missing tables.
  """
  db, cursor = selexorhelper.connect_to_db()

  # Do the tables we need exist?
  cursor.execute("show tables")
  result = cursor.fetchall()

  existing_tables = []
  for item in result:
    existing_tables.append(item[0])

  tables_contents = {
    'nodes': """
nodelocation varchar(40) NOT NULL PRIMARY KEY,
nodekey text NOT NULL
""",
    'vessels': """
nodelocation varchar(40) NOT NULL,
vesselname varchar(10) NOT NULL
""",
    'userkeys': """
nodelocation varchar(40) NOT NULL,
vesselname varchar(10) NOT NULL,
userkey text NOT NULL
""",
    'vesselports': """
nodelocation varchar(40) NOT NULL,
vesselname varchar(10) NOT NULL,
port int NOT NULL
""",
    'location': """
nodelocation varchar(40) NOT NULL,
city varchar(50),
country_code varchar(50),
longitude double,
latitude double
"""
  }

  for table in tables_contents:
    if table not in existing_tables:
      cursor.execute("CREATE TABLE "+table+" ("+tables_contents[table]+")")


def contact_vessels_and_update_database(nodes_to_check):
  '''
  Of all nodes that need checking, take one node.
  Obtain its:
    IP address
    name of each vessel on it
    Location
  For each vessel, obtain:
    vessel name
    ports available
  Finally, update the information about these nodes.

  '''
  db, cursor = selexorhelper.connect_to_db()

  while nodes_to_check:
    nodelocation = nodes_to_check.pop()
    nodeinfo = selexorhelper.get_node_ip_port_from_nodelocation(nodelocation)

    # We can't use NAT addresses, nor ipv6
    if not selexorhelper.is_ipv4_address(nodeinfo['id']):
      # if nodeinfo['id'].startswith('NAT'):
        # self._nat_nodes.append(nodeinfo['id'])
      continue

    # Used to communicate with the node
    node_nmhandle = None
    try:
      node_nmhandle = nmclient_createhandle(nodeinfo['id'], nodeinfo['port'])
      node_dict = nmclient_getvesseldict(node_nmhandle)

      # Retrieve the ports for each vessel
      ports = {}
      for vesselname in node_dict['vessels']:
        resources_string = nmclient_rawsay(node_nmhandle, "GetVesselResources", vesselname)
        ports[vesselname] = selexorhelper.get_ports_from_resource_string(resources_string)

      # Retrieve the geographic information
      try:
        # We need to some initial value so that it is not undefined when we check it later.
        geoinfo = None

        # Only retrieve the geographic information if we don't have it already
        # The geoip server's data doesn't change, so we don't need to constantly update it.
        geoinfo_exists = autoretry_mysql_command(cursor, "SELECT nodekey FROM nodes WHERE nodelocation='"+nodelocation+"'") == 1L
        if not geoinfo_exists:
          logger.info("Location data not in database, looking up on geoip: "+nodelocation)
          geoinfo = geoip_record_by_addr(nodeinfo['id'])

      except Exception, e:
        if not "Unable to contact the geoip server" in str(e):
          raise
      # The geoip lookup sometimes returns None.
      if geoinfo is None:
        geoinfo = {}
      format_geoinfo(geoinfo)

      commit_data_to_database(db, cursor, nodelocation, node_dict, ports, geoinfo)

    except NMClientException, e:
      if not node_nmhandle:
        nmclient_destroyhandle(node_nmhandle)
      # self._bad_node_locations.append(nodelocation)
      errstr = str(e)
      if ("timed out" in errstr or
          'No connection could be made because the target machine actively refused it' in errstr or
          "Connection refused" in errstr or
          "Socket closed" in errstr):
        continue
      logger.error("Unknown error contacting " + nodelocation + traceback.format_exc())
    except Exception, e:
      logger.error("Unknown Error contacting " + nodelocation + traceback.format_exc())

    nmclient_destroyhandle(node_nmhandle)



def commit_data_to_database(db, cursor, nodelocation, node_dict, ports, geoinfo):
  # Just in case we attempted to make any changes in a previous run and failed
  db.rollback()
  # == Update Nodes Table ==
  nodekeystr = rsa_publickey_to_string(node_dict['nodekey'])

  new_node = autoretry_mysql_command(cursor, "SELECT nodekey FROM nodes WHERE nodelocation='"+nodelocation+"'") == 0L

  if new_node:
    # Node isn't recognized, add it to the db
    autoretry_mysql_command(cursor, "INSERT INTO nodes (nodelocation, nodekey) VALUE ('" +
        nodelocation + "', '" + nodekeystr + "')")
  else:
    # Handle already exists, does the nodekey still match?
    if cursor.fetchone()[0] != nodekeystr:
      autoretry_mysql_command(cursor, "UPDATE nodes SET nodekey='"+
          nodekeystr+"' WHERE nodelocation='"+nodelocation+"'")

  # == Update Vessels Table ==
  autoretry_mysql_command(cursor, "DELETE FROM vessels WHERE nodelocation='"+nodelocation+"'")
  for vesselname in node_dict['vessels']:
    # v2 is a special vessel, we can't use it
    if vesselname == 'v2':
      continue
    autoretry_mysql_command(cursor, "INSERT INTO vessels (nodelocation, vesselname) VALUES ('"+nodelocation+"','"+vesselname+"')")

  # == Update Userkeys ==
  for vesselname in node_dict['vessels']:
    # v2 is a special vessel, we can't use it
    if vesselname == 'v2':
      continue

    autoretry_mysql_command(cursor, "DELETE FROM userkeys WHERE nodelocation='"+nodelocation+"'")
    for userkey in node_dict['vessels'][vesselname]['userkeys']:
      userkeystr = rsa_publickey_to_string(userkey)
      autoretry_mysql_command(cursor,
          "INSERT INTO userkeys (nodelocation, vesselname, userkey) VALUE ('"+
          nodelocation+"', '"+vesselname+"', '"+userkeystr+"')")

  # == Update Ports ==
  for vesselname in node_dict['vessels']:
    # v2 is a special vessel, we can't use it
    if vesselname == 'v2':
      continue

    autoretry_mysql_command(cursor, "DELETE FROM vesselports WHERE nodelocation='"+nodelocation+"'")
    for port in ports[vesselname]:
      cursor.execute("INSERT INTO vesselports (nodelocation, vesselname, port) VALUE ('"+nodelocation+"', '"+vesselname+"',"+str(port)+")")

  # == Update Location Table ==
  # Only update location information if it is provided.
  # Geoinfo is an empty dict if we don't have new information.
  if geoinfo:
    if new_node:
      cursor.execute("INSERT INTO location (nodelocation) VALUE ('"+nodelocation+"')")

    # Now update the database with the new information
    # First, the string types...
    for key in ['city', 'country_code']:
      if key in geoinfo:
        autoretry_mysql_command(cursor, "UPDATE location SET "+key+"='"+geoinfo[key]+"' WHERE nodelocation='"+nodelocation+"'")
      else:
        autoretry_mysql_command(cursor, "UPDATE location SET "+key+"=NULL WHERE nodelocation='"+nodelocation+"'")

    # Now, the double types
    for key in ['longitude', 'latitude']:
      if key in geoinfo:
        autoretry_mysql_command(cursor, "UPDATE location SET "+key+"="+str(geoinfo[key])+" WHERE nodelocation='"+nodelocation+"'")
      else:
        autoretry_mysql_command(cursor, "UPDATE location SET "+key+"=NULL WHERE nodelocation='"+nodelocation+"'")

  db.commit()



if __name__=='__main__':
  global logger
  global nodestate_transition_key
  logger = selexorhelper.setup_logging('selexordatabase')

  nodestate_transition_key = rsa_file_to_publickey(settings.path_to_nodestate_transition_key)

  geoip_init_client()

  # Create the databases if they haven't been created
  create_database()

  print "Probing service has started!"
  print "Press CTRL+C to stop the server."

  # Run until Ctrl+C is issued
  try:
    while True:
      probe_for_vessels()
      time.sleep(settings.probe_delay);
  except KeyboardInterrupt:
    pass