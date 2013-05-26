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
  [node_id][node_key][node_port][ip_addr][last_ip_change][last_seen]

  vessels Table:
  [node_id][vesselname]

  userkey Table
  [node_id][vesselname][userkey]

  vesselports Table:
  [node_id][vesselname][port]

  location Table:
  [ip_addr][country_code][city][longitude][latitude]


  Definitions:
    node_id: The internal identifier for each node, as nodekeys are too long to use as indexes.
    node_key: The public key that uniquely identifies each node.
    node_port: The port the nodemanager of the node is listening on.
    ip_addr: The IP address that the nodemanager of the node is listening on.
    last_ip_change: The time of the last IP change of this node.
    last_seen: The time this node was last seen.
    userkey: The public key associated with a user.
    port: A user port.

    country_code: A 2-digit country code.
    city: The name of the city.
    longitude/latitude: Coordinates where an IP is associated with.


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
import traceback
import MySQLdb







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
        args=(nodes_to_check,))
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
  code = open('database_create.sql', 'r').read()
  for line in code.split(';'):
    if line.strip():
      cursor.execute(line.strip())




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
        geoinfo_exists = selexorhelper.autoretry_mysql_command(cursor, "SELECT ip_addr FROM location WHERE ip_addr='"+nodeinfo['id']+"'") == 1L
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

      commit_data_to_database(db, cursor, nodeinfo['id'], nodeinfo['port'], node_dict, ports, geoinfo)

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



def commit_data_to_database(db, cursor, node_ip, node_port, node_dict, ports, geoinfo):
  # Just in case we attempted to make any changes in a previous run and failed
  db.rollback()

  # == Update Nodes Table ==
  nodekeystr = rsa_publickey_to_string(node_dict['nodekey'])

  new_node = selexorhelper.autoretry_mysql_command(cursor, "SELECT node_id FROM nodes WHERE node_key='"+nodekeystr+"'") == 0L

  if new_node:
    # Node isn't recognized, add it to the db
    cmd = ("INSERT INTO nodes (node_key, ip_addr, node_port, last_ip_change, last_seen) "+
          "VALUES ('%s', '%s', %i, NOW(), NOW())") % (nodekeystr, node_ip, node_port)
    selexorhelper.autoretry_mysql_command(cursor, cmd)

    # Now retrieve the internal node_id.
    selexorhelper.autoretry_mysql_command(cursor, "SELECT node_id FROM nodes WHERE node_key='"+nodekeystr+"'")
    node_id = cursor.fetchone()[0]
    logger.info('\n'.join([
        "New node found: #" + str(node_id),
        "Nodekey:",
        nodekeystr
      ]))
  else:
    # Handle already exists
    node_id = cursor.fetchone()[0]
    logger.info('\n'.join([
        "Updating node: #" + str(node_id),
        "Nodekey:",
        nodekeystr
      ]))


    # Did the IP address change?  If so mark that down.
    selexorhelper.autoretry_mysql_command(cursor, "SELECT (ip_addr) FROM nodes WHERE node_key='"+nodekeystr+"'")
    old_node_ip = cursor.fetchone()[0]
    if node_ip != old_node_ip:
      cmd = "UPDATE nodes SET ip_addr='%s', last_ip_change=NOW() WHERE node_id=%i" % (node_ip, node_id)
      selexorhelper.autoretry_mysql_command(cursor, cmd)

    # Update the last time we saw the node.
    cmd = "UPDATE nodes SET last_seen=NOW() WHERE node_id=%i" % (node_id)
    selexorhelper.autoretry_mysql_command(cursor, cmd)

  # == Update Vessels Table ==
  update_vessels_table(cursor, node_id, node_dict['vessels'])

  # == Update Userkeys ==
  update_userkeys_table(cursor, node_id, node_dict['vessels'])

  # == Update Ports ==
  update_ports_table(cursor, node_id, ports)

  # == Update Location Table ==
  if not geoinfo is None and geoinfo:
    update_location_table(cursor, node_ip, geoinfo)

  db.commit()






def update_vessels_table(cursor, node_id, vessel_dict):
  query = 'SELECT vessel_name FROM vessels WHERE node_id='+str(node_id)
  num_vessels = selexorhelper.autoretry_mysql_command(cursor, query)
  vessel_rows = cursor.fetchall()

  if vessel_rows:
    # Remove vessels that were lost, if any
    vessels_to_remove = []
    for [vessel_name] in vessel_rows:
      if not vessel_name in vessel_dict:
        # We pass this to MySQL later;  MySQL expects strings to be wrapped in quotes.
        vessels_to_remove += ['"'+vessel_name+'"']

    if vessels_to_remove:
      logger.info('\n'.join([
            "Node #"+str(node_id),
            "Lost vessels: "+str(vessels_to_remove),
            ]))

      query = 'DELETE FROM vessels WHERE node_id='+str(node_id)+' AND vessel_name in ('+', '.join(vessels_to_remove)+')'
      selexorhelper.autoretry_mysql_command(cursor, query)

  # Update the list of vessels
  logger.info('\n'.join([
      "Node #"+str(node_id),
      "Current vessels: "+str(vessel_dict.keys()),
      "Number of Vessels in database (excluding v2): " + str(num_vessels)
      ]))

  if vessel_dict:
    # IGNORE keyword is to tell MySQL to ignore vessels that already exist.
    query = 'INSERT IGNORE INTO vessels (node_id, vessel_name) VALUES'
    for vessel_name in vessel_dict:
      # v2 can never be used... No sense in tracking it in the vessel database.
      if vessel_name == 'v2':
        continue
      query += " ('"+str(node_id)+"', '"+vessel_name+"'),"
    # Get rid of the trailing comma after the last tuple
    query = query.strip(',')
    selexorhelper.autoretry_mysql_command(cursor, query)


def update_userkeys_table(cursor, node_id, vessel_dict):
  query = 'SELECT vessel_name FROM userkeys WHERE node_id='+str(node_id)
  num_vessels = selexorhelper.autoretry_mysql_command(cursor, query)
  vessel_rows = cursor.fetchall()

  if vessel_rows:
    # Remove vessels that were lost, if any
    vessels_to_remove = []
    for [vessel_name] in vessel_rows:
      if not vessel_name in vessel_dict:
        # We pass this to MySQL later;  MySQL expects strings to be wrapped in quotes.
        vessels_to_remove += ['"'+vessel_name+'"']

    if vessels_to_remove:
      query = 'DELETE FROM userkeys WHERE node_id='+str(node_id)+' AND vessel_name in ('+', '.join(vessels_to_remove)+')'
      selexorhelper.autoretry_mysql_command(cursor, query)


  # Update the userkeys
  for vessel_name in vessel_dict:
    # v2 can never be used... No sense in tracking it in the userkey database.
    if vessel_name == 'v2':
      continue


    query = 'SELECT userkey FROM userkeys WHERE node_id='+str(node_id)+" AND vessel_name='"+vessel_name+"'"
    selexorhelper.autoretry_mysql_command(cursor, query)
    userkey_rows = cursor.fetchall()

    userkeys_to_remove = []
    # Remove the userkeys that were lost, if any
    for [userkey] in userkey_rows:
      if not rsa_string_to_publickey(userkey) in vessel_dict[vessel_name]['userkeys']:
        # We pass this to MySQL later;  MySQL expects strings to be wrapped in quotes.
        userkeys_to_remove += ['"'+userkey+'"']

    if userkeys_to_remove:
      query = 'DELETE FROM userkeys WHERE (node_id, vessel_name)=('+str(node_id)+', "'+vessel_name+'") AND userkey in ('+', '.join(userkeys_to_remove)+')'
      selexorhelper.autoretry_mysql_command(cursor, query)


    # Vessels may not have userkeys on them.  Don't bother adding them in that case.
    if vessel_dict[vessel_name]['userkeys']:
      # IGNORE keyword is to tell MySQL to ignore userkeys that already exist.
      query = 'INSERT IGNORE INTO userkeys (node_id, vessel_name, userkey) VALUES'
      for userkey in vessel_dict[vessel_name]['userkeys']:
        query += " ('"+str(node_id)+"', '"+vessel_name+"', '"+rsa_publickey_to_string(userkey)+"'),"
      # Get rid of the trailing comma after the last tuple
      query = query.strip(',')
      selexorhelper.autoretry_mysql_command(cursor, query)



def update_ports_table(cursor, node_id, ports):
  query = 'SELECT vessel_name FROM vesselports WHERE node_id='+str(node_id)
  num_vessels = selexorhelper.autoretry_mysql_command(cursor, query)
  vessel_rows = cursor.fetchall()

  if vessel_rows:
    # Remove vessels that were lost, if any
    vessels_to_remove = []
    for [vessel_name] in vessel_rows:
      if not vessel_name in ports:
        # We pass this to MySQL later;  MySQL expects strings to be wrapped in quotes.
        vessels_to_remove += ['"'+vessel_name+'"']

    if vessels_to_remove:
      query = 'DELETE FROM vesselports WHERE node_id='+str(node_id)+' AND vessel_name in ('+', '.join(vessels_to_remove)+')'
      logger.debug("Vessel no longer exists: removing from ports table "+str(node_id)+":"+vessel_name+": "+ query)
      selexorhelper.autoretry_mysql_command(cursor, query)


  # Update the ports
  for vessel_name in ports:
    # v2 can never be used... No sense in tracking it in the userkey database.
    if vessel_name == 'v2':
      continue

    query = 'SELECT port FROM vesselports WHERE node_id='+str(node_id)+" AND vessel_name='"+vessel_name+"'"
    selexorhelper.autoretry_mysql_command(cursor, query)
    ports_rows = cursor.fetchall()

    ports_to_remove = []
    # Remove the userkeys that were lost, if any
    for [port] in ports_to_remove:
      if not port in ports[vessel_name]:
        # We pass this to MySQL later;  MySQL expects strings to be wrapped in quotes.
        ports_to_remove += ['"'+port+'"']

    if ports_to_remove:
      query = 'DELETE FROM vesselports WHERE (node_id, vessel_name)=('+str(node_id)+', "'+vessel_name+'") AND port in ('+', '.join(ports_to_remove)+')'
      logger.debug("Removing ports on vessel "+str(node_id)+":"+vessel_name+": "+ query)
      selexorhelper.autoretry_mysql_command(cursor, query)


    # Vessels may not have ports on them.  Don't bother adding them in that case.
    if ports[vessel_name]:
      # IGNORE keyword is to tell MySQL to ignore userkeys that already exist.
      query = 'INSERT IGNORE INTO vesselports (node_id, vessel_name, port) VALUES'
      for port in ports[vessel_name]:
        query += " ('"+str(node_id)+"', '"+vessel_name+"', "+str(port)+"),"
      # Get rid of the trailing comma after the last tuple
      query = query.strip(',')
      logger.debug("Adding ports on vessel "+str(node_id)+":"+vessel_name+": "+ query)
      selexorhelper.autoretry_mysql_command(cursor, query)




def update_location_table(cursor, ip_addr, geoinfo):
  # City is not always defined
  if 'city' in geoinfo:
    city = geoinfo['city']
  else:
    city = ""

  # == Update Userkeys ==
  country_code = geoinfo['country_code']
  longitude = str(geoinfo['longitude'])
  latitude = str(geoinfo['latitude'])

  query = 'INSERT INTO location (ip_addr, city, country_code, longitude, latitude) VALUES '
  # Specifies the location tuple
  query += "('%s', '%s', '%s', %s, %s) " % (ip_addr, city, country_code, longitude, latitude)
  query += "ON DUPLICATE KEY UPDATE "
  # Specifies the location tuple for the update clause
  query += "city='%s', country_code='%s', longitude=%s, latitude=%s" % (city, country_code, longitude, latitude)

  selexorhelper.autoretry_mysql_command(cursor, query)




if __name__=='__main__':
  global logger
  global nodestate_transition_key
  logger = selexorhelper.setup_logging('selexordatabase')

  nodestate_transition_key = rsa_file_to_publickey(settings.path_to_nodestate_transition_key)

  geoip_init_client()

  # Perform any first-time initialization if specified by the administrator.
  if len(sys.argv) > 1 and sys.argv[1] == 'initialize':
    # Create the databases if they haven't been created
    create_database()
    exit()

  print "Probing service has started!"
  print "Press CTRL+C to stop the server."

  # Run until Ctrl+C is issued
  try:
    while True:
      probe_for_vessels()
      time.sleep(settings.probe_delay);
  except KeyboardInterrupt:
    pass