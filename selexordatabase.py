"""
<Program Name>
  selexordatabase.py

<Started>
  July 7, 2012

<Author>
  leon.wlaw@gmail.com
  Leonard Law

<Purpose>
  Implements the handle lookup database.

<Usage>
  After importing, you must instantiate a database object. You may use this
  database object to find vessels that match your conditions. See documentation
  for the database for further details.

"""

import repyhelper
# Keep the mobules in their own namespace
repyhelper.translate_and_import("geoip_client.repy")
repyhelper.translate_and_import('nmclient.repy')
repyhelper.translate_and_import('advertise.repy')
from copy import deepcopy
import cPickle
import sys
import threading
import math
import time
import os
from collections import deque
import traceback

import logging

# Set up the logger
log_filehandler = logging.FileHandler('database.log', 'a')
log_filehandler.setLevel(logging.DEBUG)
log_filehandler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_filehandler)



# The default value for a vessel's ip_change_count.
# It is incremented everytime the node's IP address changes.
# Therefore, when a node's IP is first registered, the ip_change_count is incremented to 0.
INIT_IP_CHANGE_COUNT = -1

class database:
  def __init__(self,
      database_name,
      nodestate_transition_key,
      advertise_port = None,
      geoip_server_uri = None,
      begin_probing = True,
      update_threadcount = 4,
      probe_delay = 300):
    '''
    <Purpose>
      Sets up this instance of the database.

    <Arguments>
      database_name:
        The name of the current database.
        You can have separate instances of the database running with different names.
      nodestate_transition_key:
        The public key to use to look up currently advertising nodes.
      advertise_port:
        The port that the Clearinghouse nodes are advertising on. Defaults to
        Seattle Clearinghouse's value of 1224.
      geoip_server_uri:
        The URI to the GeoIP server to use. Defaults to the Seattle
        Clearinghouse GeoIP server.
      update_threadcount:
        The number of threads to use while updating.
    <Exceptions>
      ValueError
    <Side Effects>
      Initializes the GeoIP client
      Starts probing for handles

    <Return>
      None

    '''

    self.name = database_name

    ## Set up probing ##########################################################
    if probe_delay <= 0:
      raise ValueError("Probe delay must be positive!")

    # The timer object for the probing process
    self._probe_timer = None
    # How long SeleXor should wait inbetween probes, in seconds
    self.probe_delay = probe_delay
    self._probing = begin_probing
    self._last_probe_time = float('-inf')

    # The number of threads to use while probing.
    self.update_threadcount = update_threadcount
    # This should contain the threads that are updating selexor. This is used
    # to determine if an update process is currently running.
    self._update_threads = []

    if advertise_port is None:
      advertise_port = 1224   # Seattle Clearinghouse port
    self._advertise_port = advertise_port
    # Used to find advertising nodes
    self._nodestate_transition_key = nodestate_transition_key

    # Handles that, according to the last probe, are already allocated to a user.
    # The server will omit these handles while processing user requests.
    self.allocated_handles = set()

    # Handles that we cannot acquire, for whatever reason. This does not include
    # handles that are already allocated.
    self.invalid_handles = set()

    # Stores information about all known handles.
    # Access format:
    # handle_table[handle][datatype] = value
    # Valid datatypes: country, city, coordinate
    self.handle_table = {}

    # Stores information about handles that contain a specific port.
    # Access format:
    # ports_table[port_number] = set of handles that contain that port
    self.ports_table = {}

    # Stores information about handles in known locations,
    # in terms of city/country name.
    # Access format:
    # geo_policical_table[country-code][city-name] = set
    self.geo_political_table = {}

    # Stores information about handles with certain mobilities.
    # ip_change_count is defined as the number of times that the handle has
    # changed IP addresses.
    self.ip_change_table = {}

    # Attempt to populate the tables from a pickled file
    self._loadfromfile()

    # Connect to specified GeoIP server.
    if geoip_server_uri:
      geoip_init_client(geoip_server_uri)
    else:
      geoip_init_client()

    # Initialization complete
    if begin_probing:
      self.begin_probing()


  def _loadfromfile(self):
    '''
    Loads the lookup tables for the database from a file named self.name + '.data'.
    It should load the following:
      handle_table
      geo_political_table
      ports_table
      ip_change_table
    '''
    error_occurred = False
    try:
      picklejar = open(self.name + '.data', 'r')
      self.handle_table = cPickle.load(picklejar)
      self.geo_political_table = cPickle.load(picklejar)
      self.ports_table = cPickle.load(picklejar)
      self.ip_change_table = cPickle.load(picklejar)
      picklejar.close()
    except EOFError, e:
      error_occurred = True
      logger.info("Previous data was corrupted.")
    except IOError, e:
      if "No such file or directory" in str(e):
        logger.info("Previous data could not be located.")
      else:
        logger.error("Unknown error unpickling prior information\n" + traceback.format_exc())
      error_occurred = True
    except Exception, e:
      logger.error("Unknown error unpickling prior information\n" + traceback.format_exc())
      error_occurred = True

    # Clear all the tables if needed
    if error_occurred:
      self.handle_table = {}
      self.geo_political_table = {}
      self.ports_table = {}
      self.ip_change_table = {}


  def shutdown(self):
    self.stop_probing()
    # Wait until the update process finishes
    print "Waiting for probing process to finish..."
    for thread in self._update_threads:
      thread.join()

    try:
      print "Writing database to file:", os.path.abspath(self.name + '.data')
      picklejar = open(self.name + '.data', 'w')
      cPickle.dump(self.handle_table, picklejar)
      cPickle.dump(self.geo_political_table, picklejar)
      cPickle.dump(self.ports_table, picklejar)
      cPickle.dump(self.ip_change_table, picklejar)
      print "Write to disk completed."
      picklejar.close()
    except Exception, e:
      logger.error("Error writing database to disk\n" + traceback.format_exc())
      print "Error writing database to disk."
    self.running = False


  def begin_probing(self):
    '''
    <Purpose>
      Signals the selexor database to probe for new data on unallocated handles.
    <Arguments>
      None
    <Exceptions>
      None
    <Side Effects>
      Tells the selexordatabase to start probing for resources.
    <Return>
      None

    '''
    self._probing = True
    time_since_last_probe = time.time() - self._last_probe_time

    # If more time has passed since the last probe than the probe delay, probe immediately.
    # Otherwise, wait until the probe delay expires.
    time_until_next_probe = max(0, self.probe_delay - time_since_last_probe)

    self._probe_timer = threading.Timer(time_until_next_probe, self._probe_resources_periodic)
    self._probe_timer.daemon = True
    self._probe_timer.start()

  def stop_probing(self):
    '''
    <Purpose>
      Signals the selexor database to stop probing for new data on unallocated
      handles.
    <Arguments>
      None
    <Exceptions>
      None
    <Side Effects>
      Tells the selexordatabase to stop probing for resources.
    <Return>
      None

    '''
    self._probing = False
    if self._probe_timer and self._probe_timer.isAlive():
      self._probe_timer.cancel()


  def _probe_resources(self, num_resources_to_get = sys.maxsize, num_threads = None):
    logger.info("Probing for advertising nodes...")
    if num_threads is None:
      num_threads = self.update_threadcount
    # Gets all the node_ids that are being actively advertised
    nodelocations = advertise_lookup(self._nodestate_transition_key, maxvals = num_resources_to_get)

    self._nodes_to_check = nodelocations[:num_resources_to_get]
    self._checked_vessels = []
    self._bad_node_locations = []
    self._nat_nodes = []
    self.userkeys = {}

    self._update_threads = []
    starttime = time.time()
    logger.info("Found " + str(len(self._nodes_to_check)) + " nodes!")

    if self._probing:
      for thread_no in range(num_threads):
        thread = threading.Thread(target = self._update_table_from_nodelocations)
        self._update_threads.append(thread)
        thread.start()

    # Wait until all threads finish running
    for thread in self._update_threads:
      thread.join()

    if self._probing:
      # For vessels that were not contactable, increase their probe count
      vessels_not_contacted = set(self.handle_table.keys()) - set(self._checked_vessels)
      for uncontacted_vessel in vessels_not_contacted:
        self.handle_table[uncontacted_vessel]['total_probes'] += 1

    message = '\n'.join([
      "Update took: " + str(time.time() - starttime) + "s",
      "Good nodelocations: " + str(len(nodelocations) - len(self._bad_node_locations)),
      "Unchecked nodelocations: " + str(len(self._nodes_to_check)),
      "NAT nodelocations: " + str(len(self._nat_nodes)),
      "Total unusable nodelocations: " + str(len(self._bad_node_locations)),
      "Allocated vessels:" + str(len(self.allocated_handles)),
      "Active vessels: " + str(len(self._checked_vessels))
      ])

    logger.info(message)

    self._nat_nodes = []
    self._bad_node_locations = []
    self._checked_vessels = []
    self._update_threads = []


  def _update_tables(self, resources):
    '''
    <Purpose>
      Updates the information stored in this database from the provided resources.
    <Arguments>
      resources:
        A list of resources. The resources are in the same format as provided by
        the SeattleClearinghouse XMLRPC client after issuing a acquire_(type)_resources()
        or get_resource_info().

        Each resource is a dictionary, expected to have the following keys:
          'handle': resource handle
          'node_ip': resource's IP address
    <Exceptions>
      None
    <Side Effects>
      Modifies entries within the lookup tables.
    <Returns>
      None

    '''
    # Get geo. info
    for resource in resources:
      if resource['handle'] not in self.handle_table:
        # Create the resource entry
        self._create_resource_entry(resource)
      self._update_resource_clearinghouse(resource)


  def _update_table_from_nodelocations(self):
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
    while self._nodes_to_check:
      if not self._probing:
        break
      nodelocation = self._nodes_to_check.pop()
      nodeinfo = get_node_ip_port_from_nodelocation(nodelocation)

      # We can't use NAT addresses, nor ipv6
      if not is_ipv4_address(nodeinfo['id']):
        if nodeinfo['id'].startswith('NAT'):
          self._nat_nodes.append(nodeinfo['id'])
        continue

      # Handle all node-level information here
      try:
        geoinfo = geoip_record_by_addr(nodeinfo['id'])
      except Exception, e:
        if "Unable to contact the geoip server" in str(e):
          # Try it again later
          self._nodes_to_check.append(nodelocation)
          continue
        raise

      if type(geoinfo) != type(dict()):
        # Bad datatype
        # Maybe still allow access to vessel, but prevent from being used in
        # geographic-related searches?
        self._bad_node_locations.append(nodelocation)
        continue
      _format_geoinfo(geoinfo)

      # Used to communicate with the node
      node_nmhandle = None
      try:
        node_nmhandle = nmclient_createhandle(nodeinfo['id'], nodeinfo['port'])
        node_dict = nmclient_getvesseldict(node_nmhandle)

        for vesselname in node_dict['vessels']:
          # v2 is a special vessel, we can't use it
          if vesselname == 'v2':
            continue

          resources_string = nmclient_rawsay(node_nmhandle, "GetVesselResources", vesselname)
          handleinfo = nmclient_get_handle_info(node_nmhandle)
          vessel_handle = get_vesselhandle_from_identity_and_vesselname(handleinfo['identity'], vesselname)

          # Update the databases
          vessel_nmhandle = nodeinfo['id'] +':'+str(nodeinfo['port']) +':'+vesselname
          self._update_resource_entry(
              vessel_handle,
              handleinfo['IP'],
              geoinfo,
              ports = get_ports_from_resource_string(resources_string))

          # Is it currently allocated to a user?
          # Current method does not always obtain the correct status
          if node_dict['vessels'][vesselname]['userkeys']:
            self.mark_handles_as_allocated([vessel_handle])
          else:
            self.mark_handles_as_unallocated([vessel_handle])
          self._checked_vessels.append(vessel_handle)

      except NMClientException, e:
        if not node_nmhandle:
          nmclient_destroyhandle(node_nmhandle)
        self._bad_node_locations.append(nodelocation)
        logger.error("Unknown error contacting " + nodelocation + traceback.format_exc())

      nmclient_destroyhandle(node_nmhandle)


  def _probe_resources_periodic(self):
    ''' Calls the probing function periodically. '''
    if self._probing:
      self._probe_resources()
      self._probe_timer = threading.Timer(self.probe_delay, self._probe_resources_periodic)
      self._probe_timer.start()


#########################################
### GET DATA
#########################################
  def get_accessible_vessels(self, country = None, city = None, port = None, vesselset = None):
    '''
    <Purpose>
      Of the given vesselset, or all known vessels, returns the ones that
      satisfy the given parameters.
    <Parameters>
      country:
          The country name. This should be in the 2-letter code.
      city:
          The city name.
      port:
          The vessel port that should be available.
      vesselset:
          The set of vessels to consider. If set to None, all known
          vessels will be considered.
    <Exceptions>
      None
    <Side Effects>
      ValueError
    <Returns>
      The vessels that satisfy the given request.
    '''
    
    if vesselset is None:
      vesselset = set(self.handle_table.keys())
    accessible_vessels = vesselset.difference(self.invalid_handles.union(self.allocated_handles))
    if country is not None:
      accessible_vessels = accessible_vessels.intersection(self._get_vessels_in_location(country, city))
    if port is not None:
      accessible_vessels = accessible_vessels.intersection(self.ports_table[port])
    return accessible_vessels


  def _get_vessels_in_location(self, country_code, city = None):
    '''
    <Purpose>
      Returns the set of vessels in the specified location.
    <Arguments>
      country_code: 
        An ISO-3166-2 country identification code.
      city: 
        The name of the city. This field is optional. 
        If unspecified, then all the vessels in the country will be returned.
    <Side Effects>
      None
    <Exceptions>
      None
    <Returns>
      The set of all vessels inside the specified location.
    '''
    
    vesselset = set()
    if country_code not in self.geo_political_table:
      return vesselset
    if city is None:
      for city, cityvesselset in self.geo_political_table[country_code].iteritems():
        vesselset = vesselset.union(cityvesselset)
    else:
      # There may be no vessels in the specified city
      if city in self.geo_political_table[country_code]:
        vesselset = self.geo_political_table[country_code][city]
    return vesselset



#########################################
### TABLE UPDATE
#########################################

  def mark_handles_as_allocated(self, handles):
    '''
    <Purpose>
      Marks the specified handles to allocated, so that selexor will no longer
      use them until they are marked unallocated.
    <Arguments>
      handles:
        The list of handles to mark as being allocated.
    <Exceptions>
      None
    <Side Effects>
      Handles that are allocated are unable to be requested until they are
      marked as unallocated, either by release_vessels() or by the probe
      process.
    <Returns>
      List of handles that were valid.

    '''
    good_handles = []
    for handle in handles:
      if handle not in self.allocated_handles:
        self.allocated_handles.add(handle)
        good_handles.append(handle)
    return good_handles


  def mark_handles_as_unallocated(self, handles):
    '''
    <Purpose>
      Marks the specified handles to unallocated, so that selexor will use them
      again.
    <Arguments>
      handles:
        The list of handles to unallocate.
    <Exceptions>
      None
    <Side Effects>
      Removes the specified handles from the allocated handles list. Does nothing
      if the handle is already unallocated.
    <Returns>
      None
    '''
    
    for handle in handles:
      if handle in self.allocated_handles:
        self.allocated_handles.remove(handle)


  def _create_resource_entry(self, resource_handle):
    '''
    <Purpose>
      Creates a new resource entry for the specified resource in handle_table.
    <Arguments>
      resource:
        A resource in the same format as provided by the SeattleClearinghouse XMLRPC
        client after issuing a acquire_(type)_resources() or get_resource_info().

        Each resource is a dictionary, expected to have the following keys:
          'handle': resource handle
          'node_ip': resource's IP address
    <Exceptions>
      None
    <Side Effects>
      Modifies handle_table.
    <Returns>
      None

    '''
    self.handle_table[resource_handle] = {
        'ip': None,
        'vesselname': get_vesselname_from_vesselhandle(resource_handle),
        'ports': set(),
        'geographic': {
          'country_code': None,
          'city': None,
          'longitude': None,
          'latitude': None,
          },
        'ip_change_count': INIT_IP_CHANGE_COUNT,
        'successful_probes': 0,
        'total_probes': 0
        }


  def _update_resource_entry(self, resource_handle, ip, geoinfo, ports):
    '''
    <Arguments>
      resource_handle:
        The resource to change.
      resource_info:
        A dictionary containing the resource's updated information.
        Assumes that all data in this dictionary is pre-formatted.

    '''
    if resource_handle not in self.handle_table:
      self._create_resource_entry(resource_handle)

    old_resource_info = deepcopy(self.handle_table[resource_handle])
    old_geoinfo = old_resource_info['geographic']

    # Update main handle table
    self.handle_table[resource_handle]['successful_probes'] += 1
    self.handle_table[resource_handle]['total_probes'] += 1
    self.handle_table[resource_handle]['ip'] = ip
    self.handle_table[resource_handle]['geographic'] = geoinfo
    self.handle_table[resource_handle]['ports'] = ports

    # Update ip_change_count
    if ip != old_resource_info['ip']:
      self.handle_table[resource_handle]['ip_change_count'] = old_resource_info['ip_change_count'] + 1
      old_ip_change_count = []
      if old_resource_info['ip_change_count'] is not INIT_IP_CHANGE_COUNT:
        old_ip_change_count = [old_resource_info['ip_change_count']]
      new_ip_change_count = [self.handle_table[resource_handle]['ip_change_count']]
      _move_table_entry(self.ip_change_table, resource_handle, new_ip_change_count, old_ip_change_count)

    # Update political table
    new_location = [geoinfo['country_code'], geoinfo['city']]
    old_location = []
    if old_geoinfo['country_code'] is not None:
      old_location = [old_geoinfo['country_code'], old_geoinfo['city']]

    _move_table_entry(self.geo_political_table, resource_handle, new_location, old_location)

    # we don't use this right now...
##    # Update longitude/latitude table
##    new_coordinates = [geoinfo['longitude'], geoinfo['latitude']]
##    old_coordinates = []
##    if old_geoinfo['longitude']:
##      old_location = [old_geoinfo['longitude'], old_geoinfo['latitude']]
##    _move_table_entry(self.geo_long_lat_table, resource_handle, new_coordinates, old_coordinates)

    # Update ports table
    # Check if they are different first, since there may be a lot of operations
    if old_resource_info['ports'] != ports:
      ports_to_add = ports - old_resource_info['ports']
      ports_to_remove = old_resource_info['ports'] - ports
      for port in ports_to_remove:
        _remove_table_entry(self.ports_table, resource_handle, [port])
      for port in ports_to_add:
        _add_table_entry(self.ports_table, resource_handle, [port])


def _add_table_entry(table, data, destination_keys):
  '''
  <Purpose>
    Adds a table entry to the destination position.
    We assume that the actual table consists of sets.
  <Arguments>
    table:
      The table that contains the data.
    data:
      The actual data in the table to move.
    destination_keys:
      A tuple representing where to place the new data. If there are several
      layers in this table, specify the keys in depth-order.
      e.g.
        a_handle = 'a_handle'
        vessel_table['us']['new york'] = [a_handle]
        # We want to move a_handle to "ca", "toronto"
        _move_table_entry(vessel_table, a_handle, ["ca", "toronto"])
  <Side Effects>
    If keys do not exist in the table, those keys will be created.
      If there are no more keys remaining, then a set will be created.
      Otherwise, a dictionary will be put in its place.

  '''
  # See if next destination exists; if not, create it
  if destination_keys[0] not in table:
    if len(destination_keys[1:]) > 0:
      table[destination_keys[0]] = {}
    else:
      table[destination_keys[0]] = set()

  if len(destination_keys) == 1:
    # Base case: We are at the data set
    table[destination_keys[0]].add(data)
    return

  # Go deeper
  _add_table_entry(table[destination_keys[0]], data, destination_keys[1:])



def _remove_table_entry(table, data, source_keys):
  '''
  <Purpose>
    Moves a table entry from the source position to the destination position.
    We assume that the actual table consists of sets.
  <Arguments>
    table:
      The table that contains the data.
    data:
      The actual data in the table to move.
    source_keys:
      A tuple representing where the data is from. If there are several layers
      in this table, specify the keys in depth-order.
      e.g.
        a_handle = 'a_handle'
        vessel_table['us']['new york'] = [a_handle]
        # We want to move a_handle to "ca", "toronto"
        _move_table_entry(vessel_table, a_handle, ["ca", "toronto"])
  <Exceptions>
    ValueError
  <Side Effects>
    If the keys are not in the dictionary, it will simply return.
    If this results in empty entries within the dictionary, these entries will be
      removed to conserve memory.
  <Returns>
    None

  '''
  if not source_keys:
    raise ValueError("Expected source keys!")

  # Source key is not in table, there is nothing to remove
  if source_keys[0] not in table:
    return

  # Base case: we are at the data set
  # Remove the data
  if not source_keys[1:]:
    if data in table[source_keys[0]]:
      table[source_keys[0]].remove(data)
  else:
    _remove_table_entry(table[source_keys[0]], data, source_keys[1:])

  # Entry is empty, remove it
  if  source_keys[0] in table and \
      not table[source_keys[0]]:
    table.pop(source_keys[0])


def _move_table_entry(table, data, destination_keys, source_keys = None):
  '''
  <Purpose>
    Moves a table entry from the source position to the destination position.
    We assume that the actual table consists of sets.
  <Arguments>
    table:
      The table that contains the data.
    data:
      The actual data in the table to move.
    destination_keys:
      A tuple representing where to place the new data. If there are several
      layers in this table, specify the keys in depth-order.
      e.g.
        a_handle = 'a_handle'
        vessel_table['us']['new york'] = [a_handle]
        # We want to move a_handle to "ca", "toronto"
        _move_table_entry(vessel_table, a_handle, ["ca", "toronto"], ['us', 'new york'])
        # Now the following key will exist
        # vessel_table['ca']['toronto'] = [a_handle]
        # This will no longer exist
        # vessel_table['us']['new york'] = [a_handle]
    source_keys:
      A tuple representing where the data is from. You should specify this when
      the data is not already in the table. Format is the same as destination_keys.
      Leave it as None to indicate that it is not in the table.
  <Side Effects>
    If keys do not exist in the table, those keys will be created.
      If there are no more keys remaining, then a set will be created.
      Otherwise, a dictionary will be put in its place.
    If this results in empty entries within the dictionary, these entries will be
      removed to conserve memory.

  '''
  # No need to perform any moving
  if source_keys == destination_keys:
    return
  if source_keys:
    _remove_table_entry(table, data, source_keys)
  _add_table_entry(table, data, destination_keys)



def get_ports_from_resource_string(resource_string):
  '''
  <Purpose>
    Returns the set of all ports accessible on a resource with the given
    resource string.
  <Parameters>
    resource_string: A string representing the resources file on a vessel.
  <Exceptions>
    None
  <Side Effects>
    None
  <Return>
    The set of all accessible ports on TCP/UDP.

  '''
  available_ports = set()
  for resource_description in resource_string.split('\n'):
    if  'messport' in resource_description or\
        'connport' in resource_description:
      # ports are sometimes specified as floats
      port_number = int(float(resource_description.split()[2]))
      available_ports.add(port_number)
  return available_ports


def get_vesselhandle_from_identity_and_vesselname(identity, vesselname):
  '''
  <Purpose>
    Obtains the vesselhandle for a given nodekey and vesselname pair.
  <Arguments>
    nodekey: dict
      A dictionary representing the nodekey of a node. The following keys are
      expected:
        'e', 'n'
    vesselname: string
      The identifier for a specific vessel on a node.
  <Exceptions>
    None
  <Side Effects>
    None
  <Return>
    The vesselhandle for the given identity/vesselname pair.

  '''
  return identity + ":" + vesselname


def get_node_ip_port_from_nodelocation(nodelocation):
  '''
  <Purpose>
    Obtains the nodeid and nodeport from a given nodelocation.
  <Arguments>
    nodelocation: string
      A string representing a nodelocation.
  <Exceptions>
    None
  <Side Effects>
    None
  <Return>
    A dictionary containing:
      'nodeid': the node ID
      'port': the port

  '''
  node_info = nodelocation.split(':')
  return {'id': node_info[0], 'port': int(node_info[1])}


def get_nodestate_transition_key(port):
  '''
  <Purpose>
    Obtains the nodestate transition key for a clearinghouse. This may be used
    to get all vessels that belong to a clearinghouse.
  <Arguments>
    port: int
      The port number associated with the clearinghouse.
  <Exceptions>
    NMClientException
  <Side Effects>
    Creates an outgoing connection to the node manager at the specified port.
    This connection is closed upon termination of this function.
  <Return>
    None

  '''
  nmhandle = nmclient_repy.nmclient_createhandle(getmyip(), port)
  vesseldict = nmclient_repy.nmclient_getvesseldict(nmhandle)
  nmclient_repy.nmclient_destroyhandle(nmhandle)
  return vesseldict['vessels']['v2']['ownerkey']


def is_ipv4_address(ipstring):
  ''' Checks if the given string is a valid IPv4 address. '''
  tokens = ipstring.split(".")  # ipv4 octets are '.' separated
  if len(tokens) != 4:  # there must be 4 octets
    return False
  for token in tokens:
    # each octet must be an integer in [0, 256)
    if not (token.isdigit() and int(token) in range(256)):
      return False
  return True


def _format_geoinfo(geoinfo):
  '''
  <Purpose>
    Changes the geoinfo to a format that is consistent with selexordatabase's
    data structures.
  <Side Effects>
    The contents of 'country_code' and 'city' will be set to lower case, if they
      exist.
    If 'city' does not exist, then it will be instantiated with None.

  '''
  # Sometimes, 'city' cannot be found
  if 'city' not in geoinfo:
    geoinfo['city'] = None
  for location_type in geoinfo:
    if type(geoinfo[location_type]) == type(''):
      geoinfo[location_type] = geoinfo[location_type].lower()


def get_vesselname_from_vesselhandle(vesselhandle):
  return vesselhandle.split(":")[1]