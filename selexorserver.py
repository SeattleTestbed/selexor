"""
<Program Name>
  selexorserver.py

<Started>
  July 7, 2012

<Author>
  leon.wlaw@gmail.com
  Leonard Law

<Purpose>
  Implements the server for SeleXor.

  identity:
    A unique identifier for each user. It is a tuple in the form: (username, IP)

  authdict:
    A dictionary used to store authentication information.
    It should contain a single key indicating the username,
    value being a dictionary containing at least one of the following:
      'api_key':
        The api_key associated with the active user.
      'privatekey':
        The private key associated with the active user.

  requestdict:
    Represents an acquisition request. It contains the following:
    'groups': A dictionary of groupnames, mapped against their respective
              groupdicts.
    'status': The request's current status. It can have the following values:
        'processing': The request was received, and SeleXor is currently
                      parsing it.
        'accepted': The request is valid, but SeleXor has not yet started to
                    solve it.
        'working': The request is valid, an SeleXor is currently resolving it.
        'complete': The request is fully resolved.
        'unknown': This is an unknown request. Serverside requestdicts will
                   never have this status. This status is returned to clients
                   if they are requesting a requestdict that the server does
                   not know about.

  groupdict:
    Represents a group. It contains the following:
    'id': The group's ID. This should be a string.
    'rules': A ruledict of the rules each vessel in this group should contain.
    'acquired': A list of acquired vessel handles that belong in this group.
    'allocate': The total number of vessels to acquire.
    'status': The current request status of the group. See groupstatus.
    'error': This flag only exists when an error occurs.

  groupstatus:
    A string indicating a group's current status.
    It can have the following values:

    'resolved': The group has finished processing.
    'incomplete': The group has not finished processing, and has not been
                  processed on the active pass.
    'unresolved': The group has not finished processing, but has already been
                  processed on the active pass.
    'failed': The group could not be completed, because the number of passes
              attempted has exceeded the maximum allowed limit.
    'error': The group could not be completed because of some unexpected error.

  ruledict:
    A dictionary representing the rules that a group should follow. See the
    documentation for selexorruleparser.py for more information.


"""

import sys
import selexordatabase
import seattleclearinghouse_xmlrpc as xmlrpc_client
import time
import copy
import selexorruleparser as parser
import random
import rsa_repy
import cPickle
import os
import fastnmclient
import threading
from collections import deque
import traceback
import selexorexceptions
import logging


# Set up the logger
log_filehandler = logging.FileHandler('selexor_server.log', 'a')
log_filehandler.setLevel(logging.DEBUG)
log_filehandler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_filehandler)


# Contains information about all requests.
# (username, ip): {
#   'status': (string) The current status of this request.
#   'groups': ([node]) Dicts of all groups in request, with group ID as the key
#   'tree': ([node]) Group tree for this request,
#   'expiretime': (float) The time to consider this entry as void.
#   }
request_datum = {}


# This indicates the maximum number of times we should attempt to resolve each
# group.
MAX_PASSES_PER_NODE = 5


FINISHED_PROCESSING_STATUSES = ['resolved', 'failed', 'error']
STATUSES_TO_SKIP = FINISHED_PROCESSING_STATUSES + ['incomplete']



def _get_next_group_to_resolve(requestdict):
  '''
  <Purpose>
    Given a requestdict, returns the next group that the program should resolve.
  <Arguments>
    requestdict:
      A dictionary representing a user's request.
  <Exceptions>
    None
  <Side Effects>
    None
  <Returns>
    The next node that should be resolved.
    Returns None if there are no more nodes to resolve.

  '''
  min_rulecount = 2 ** 32  # Should be as large as possible
  next_group = None

  for groupname in requestdict['groups']:
    group = requestdict['groups'][groupname]
    if  group['status'] not in STATUSES_TO_SKIP and \
        len(group['rules']) < min_rulecount:
      next_group = groupname
      min_rulecount = len(group['rules'])
  return next_group



def connect_to_clearinghouse(authdata, xmlrpc_url = None):
  '''
  <Purpose>
    Wrapper for a SeattleClearinghouseClient constructor.
  <Arguments>
    authdata:
      An authdict. See module documentation for more information.
  <Exceptions>
    SelexorAuthenticationFailed
  <Side Effects>
    Opens an outgoing connection to the specified clearinghouse.
  <Returns>
    A client object that can be used to communicate with the Clearinghouse as the
    specified user.

  '''
  username = authdata.keys()[0]
  apikey = None
  private_key_string = None

  if 'apikey' in authdata[username]:
    apikey = authdata[username]['apikey']
  if 'privatekey' in authdata[username]:
      private_key_string = rsa_repy.rsa_privatekey_to_string(authdata[username]['privatekey'])

  if not (apikey or private_key_string):
    raise selexorexceptions.SelexorAuthenticationFailed("Either apikey or privatekey must be given!")

  try:
    client = xmlrpc_client.SeattleClearinghouseClient(
        username = username,
        api_key = apikey,
        xmlrpc_url = xmlrpc_url,
        private_key_string = private_key_string)
  except Exception:
    print traceback.format_exc()
    raise

  return client



class SelexorServer:
  def __init__(self,
               database_name,
               advertise_port,
               nodestate_transition_key,
               clearinghouse_xmlrpc_uri = None,
               geoip_server_uri = None,
               begin_probing = True,
               update_threadcount = None,
               probe_delay = 300):
    '''
    <Purpose>
      Creates an instance of a selexor server.
    <Arguments>
      database_name:
        The name of the database.
      advertise_port:
        The port to listen on for vessel information.
      nodestate_transition_key:
        The public key of the Clearinghouse.
      clearinghouse_xmlrpc_uri:
        The URI of the XMLRPC server for the Clearinghouse. Set to None to use
        the default (Seattle Clearinghouse XMLRPC server)
      geoip_server_uri:
        The GEOIP server to use to determine node locations.
      begin_probing:
        If set to True, the database will start probing.
      update_threadcount:
        The number of threads to use when probing for resources.
    <Side Effects>
      Initializes the GeoIP client.
      Initializes a selexordatabase.
    <Exceptions>
      None
    <Returns>
      A selexorserver instance.

    '''
    self.name = database_name
    self.advertise_port = advertise_port
    self.clearinghouse_xmlrpc_uri = clearinghouse_xmlrpc_uri
    self.database = selexordatabase.database(
                  database_name,
                  advertise_port = advertise_port,
                  nodestate_transition_key = nodestate_transition_key,
                  geoip_server_uri = geoip_server_uri,
                  begin_probing = begin_probing,
                  update_threadcount = update_threadcount,
                  probe_delay = probe_delay)

    self._accepting_requests = True
    self._running = True

    # The threads that are involved in resolving group requests.
    # We keep track of these to ensure we stop resolving groups before writing
    # the database to disk.
    # We use the (username, ip_addr) tuple to identify threads.
    self._resolution_threads = {}



  def shutdown(self):
    '''
    <Purpose>
      Shuts down this instance of the selexor server.
    <Arguments>
      None
    <Side Effects>
      Shuts down the database.
      Tells the logging utility to terminate.
      Stops accepting requests.
      Stops parsing requests.
    <Exceptions>
      None
    <Return>
      None

    '''
    self._accepting_requests = False
    self._running = False
    self.database.stop_probing()
    self.database.shutdown()


##  def accepting_requests(self):
##    return self._accepting_requests


  def _compile_nodeinfolist_from_vessel_handles(self, handles):
    nodeinfolist = []
    
    return nodeinfolist


  def release_vessels(self, authdata, vessels, remoteip):
    '''
    <Purpose>
      Returns the # of vessels released
    <Arguments>
      authdata:
        An authdict. See module documentation for more information.
      vessels:
        A list of node handles to release.
      remoteip:
        The remote IP address of the client. This is used for client identification.
    <Exceptions>
      None
    <Side Effects>
      None
    <Return>
      A dictionary containing the status of each group.
      'group_id': 'group_status'

    '''
    try:
      username = authdata.keys()[0]
      identity = (username, remoteip)
      logger.info(str(identity) + "> Release: " + str(vessels))

      # There's nothing to do if there aren't any vessels to release
      if not vessels:
        return 0

      released_vesselhandles = []

      client = connect_to_clearinghouse(authdata, self.clearinghouse_xmlrpc_uri)
      resource_info = client.get_resource_info()
      print resource_info
      print "Vessels:"
      print vessels
      found_vesselhandles = []
      for resource_dict in resource_info:
        nmvesselhandle = resource_dict['node_ip'] + ":" + str(resource_dict['node_port']) + ':' + resource_dict['vessel_id']
        if nmvesselhandle in vessels:
          found_vesselhandles.append(resource_dict['handle'])
      client.release_resources(found_vesselhandles)

      # Do we need to check if a vessel failed to release?
      # Maybe it means that a vessel has gone offline/is now invalid.

      # Assume all the vessels were released successfully
      self.database.mark_handles_as_unallocated(found_vesselhandles)
      num_released = len(found_vesselhandles)

      # Remove vessel entries from the groups tables.
      if identity in request_datum:
        for group in request_datum[identity]['groups']:
          for vesselhandle in found_vesselhandles:
            if vesselhandle in request_datum[identity]['groups'][group]['acquired']:
              request_datum[identity]['groups'][group]['acquired'].remove(vesselhandle)
              request_datum[identity]['groups'][group]['allocate'] -= 1

    except Exception, e:
      logger.error(str(identity) +': Unknown error while releasing vessels\n' + traceback.format_exc())
      return (False, "Internal error occurred.")

    logger.info(str(identity) +': Successfully released ' + str(len(found_vesselhandles)) + ' vessel(s)')
    return (True, num_released)


  def get_request_status(self, authinfo, remoteip):
    '''
    <Purpose>
      Returns the status of the current request.
    <Arguments>
      None
    <Exceptions>
      None
    <Side Effects>
      None
    <Return>
      A dictionary containing the status of each group.
      'group_id': 'group_status'

    '''
    data = {'groups':{}}
    try:
      username = authinfo.keys()[0]
      identity = (username, remoteip)
      if identity not in request_datum:
        data['status'] = "unknown"
        return data

      request_data = request_datum[identity]

      data['status'] = request_data['status']
      for group in request_data['groups'].values():
        data['groups'][group['id']] = {}
        group_data = data['groups'][group['id']]
        group_data['status'] = group['status']
        if 'error' in group:
          group_data['error'] = group['error']
          
        group_data['vessels_acquired'] = []
        for handle in group['acquired']:
          handle_entry = self.database.handle_table[handle]
          nodeinfo = {
            'node_ip': handle_entry['ip'],
            'vesselname': handle_entry['vesselname'],
            'handle': handle,
            'node_port': self.advertise_port
          }
          group_data['vessels_acquired'].append(nodeinfo)
          
        group_data['target_num_vessels'] = group['allocate']
    except Exception, e:
      logger.error(str(identity) + ": Error while responding to status query\n" + traceback.format_exc())
      data['error'] = str(e)
    return data


  def resolve_node(self, identity, client, node, port):
    # We should never run into these...
    if node['status'] == "resolved":
      logger.error(str(identity) + ": Group already resolved: " + str(node))
      return node
    elif node['status'] == 'failed':
      logger.error(str(identity) + ": Exceeded pass limit: " + str(node))
      return node

    logger.info(str(identity) + ": Group " + node['id'] + " on Pass " + str(node['pass']))
    vessels_to_acquire = []
    remaining = node['allocate'] - len(node['acquired'])

    if port and not port in self.database.ports_table:
      logger.info(str(identity) + ": No vessels with port " + str(port))
    else:
      # Get vessels that match the vessel rules
      handles_vesselrulematch = parser.apply_vessel_rules(
          rules = node['rules'],
          database = self.database,
          vesselset = self.database.get_accessible_vessels(port = port))
      logger.info(str(identity) + ": Vessel-level matches: " + str(len(handles_vesselrulematch)))

      # The number of times we tried to resolve this group in the current attempt
      in_group_retry_count = 0
      MAX_IN_GROUP_RETRIES = 3

      while len(node['acquired']) < remaining and \
            in_group_retry_count < MAX_IN_GROUP_RETRIES:

        if not self._running:
          break

        if node['pass'] < MAX_PASSES_PER_NODE:
          # Make sure the vesselset is updated
          handles_vesselrulematch = self.database.get_accessible_vessels(vesselset = handles_vesselrulematch)

        elif node['pass'] == MAX_PASSES_PER_NODE:
          # Perform last attempt, get fresh set of nodes
          handles_vesselrulematch = parser.apply_vessel_rules(
            rules = node['rules'],
            database = self.database,
            vesselset = self.database.get_accessible_vessels(port = port))
        else:
          raise selexorexceptions.SelexorInternalError("Performing more passes than max pass!")

        handles_grouprulematch = parser.apply_group_rules(
            database = self.database,
            acquired_vessels = node['acquired'],
            rules = node['rules'],
            vesselset = handles_vesselrulematch)

        # Try each vessel until we successfully acquire one
        handle_list = list(handles_grouprulematch)
        while handle_list:
          logger.info(str(identity) + ": Candidates for next vessel: " + str(len(handle_list)))
          # If we run out of handles, we simply get another random one, instead of
          # programming a special case.
          handle = random.choice(handle_list)
          handle_list.remove(handle)
          try:
            acquired_vesseldicts = client.acquire_specific_vessels([handle])
            # This vessel will be acquired regardless of whether or not acquisition
            # succeeded or not
            acquired_handles = get_handle_list_from_vesseldicts(acquired_vesseldicts)
            self.database.mark_handles_as_allocated(acquired_handles)

            if acquired_vesseldicts:
              logger.info(str(identity) + ": Acquired " + str(acquired_handles))
              node['acquired'] += acquired_handles  # Should be 1
              break
          except xmlrpc_client.NotEnoughCreditsError, e:
            logger.error(str(identity) + ": Not enough vessel credits")
            raise
          except xmlrpc_client.InvalidRequestError, e:
            logger.error(str(identity) + ": " + str(e))
            self.database.invalid_handles.add(handle)
            continue
          except Exception, e:
            logger.error(str(identity) +": Unknown error while acquiring vessels\n" + traceback.format_exc())
            raise selexorexceptions.SelexorInternalError(str(e))

        # We ran out of vessels to check
        else:
          logger.info(str(identity) + ": Can't find any suitable vessels!")
          in_group_retry_count += 1

          # Have we exceeded the maximum in-group retry count?
          if in_group_retry_count > MAX_IN_GROUP_RETRIES:
            break

          # We retry if there could be another combination that MIGHT satisfy
          # the group rules. If there are no group rules, there is no point
          # to retry.
          if not parser.has_group_rules(node['rules']):
            logger.info(str(identity) + ": There are no group rules applied; no point in retrying.")
            break

          if node['acquired']:
            # Get the vessel that causes the largest drop in the
            # size of the available vessel pool
            worst_vessel = parser.get_worst_vessel(
                node['acquired'],
                handles_grouprulematch,
                self.database,
                node['rules'])

            # Release the worst vessel so that we can try to get a better one
            # in the next iteration
            logger.info(str(identity) + ": Releasing: " + str(worst_vessel))
            node['acquired'].remove(worst_vessel)
            client.release_resources([worst_vessel])
            self.database.mark_handles_as_unallocated([worst_vessel])

    node['pass'] += 1
    if node['pass'] >= MAX_PASSES_PER_NODE:
      logger.info(str(identity) + ": Group exceeds pass limit. Designating group as failed: " + str(node))
      node['status'] = 'failed'
    elif len(node['acquired']) == node['allocate']:
      node['status'] = 'resolved'
    else:
      node['status'] = 'incomplete'
    return node


  def handle_request(self, authinfo, request, port, remoteip):
    '''
    <Purpose>
      Handles a host request for the specified user.

    <Arguments>
      userdata:
        A dictionary representing the user's authentication information. It should
        contain the following:
          'username':
            The user's username.
          'api_key':
            The user's API key.
          'clearinghouse_uri':
            The target Clearinghouse to connect to.

      request:
        A string representing a user request. This string must not contain any
        spaces (except for a parameter value) nor newlines. Groups are separated
        with semicolons. Each group must have a group ID, number of vessels,
        and optionally rules, all colon separated. The group ID should be an
        integer. Rules are also colon separated. Rules should have a rule type,
        followed by their list of parameters, comma separated. Parameters are
        in the format of parameter_name~parameter_value. Each parameter may only
        have one value.

        Example (split across multiple lines for readability):
          0:3:location_specific,city~?,country~USA:location_different,num_locations~4,location_type~city;
          1:2:latency_average,min_latency~40ms,max_latency~200ms;
      port:
        The port to obtain the vessels on.

    <Side Effects>
      Attempts to obtain vessels described in the request_data. This is not
      guaranteed, depending on the ddata collected in the selexordatabase.

    <Exceptions>
      None

    <Returns>
      Returns the status of the request, as a string.

      'timeout':
        The request did not finish in the allocated time.
      'complete':
        Selexor successfully finished the request.

    '''
    if not self._accepting_requests:
      return {'error': "Server is not accepting requests."}

    # Get ready to handle the request
    username = authinfo.keys()[0]
    identity = (username, remoteip)
    request_datum[identity] = {'status': 'processing'}
    logger.info(str(identity) + ": Obtained request: " + str(request))

    # Make sure the request is valid
    request_data = self._validate_request(identity, request)
    request_datum[identity] = request_data
    logger.info(str(identity) + ": Generated Request data: " + str(request_data))

    if request_data['status'] == 'accepted':
      try:
        client = connect_to_clearinghouse(authinfo, self.clearinghouse_xmlrpc_uri)
      except selexorexceptions.SelexorException, e:
        request_data['status'] = 'error'
        request_data['error'] = str(e)
        raise
      except:
        request_data['status'] = 'error'
        request_data['error'] = "An internal error occurred."
        logger.error(str(identity) + ": Error connecting to clearinghouse" + traceback.format_exc())
        raise

      if port is None:
        port = client.get_account_info()['user_port']
      else:
        port = int(port)

      # Start working
      request_data['status'] = "working"
      logger.error(str(identity) + ": Working on request")
      resolution_thread = threading.Thread(target=self.serve_request, args=(identity, request_data, client, port))
      resolution_thread.start()
      self._resolution_threads[identity] = resolution_thread

    else:
      logger.info(str(identity) + ": Could not process request")
    return self.get_request_status(authinfo, remoteip)


  def serve_request(self, identity, request_data, client, port):
    '''
    <Purpose>
      Serves a host request.

    <Arguments>
      identity:
        A user identity.
      request_data:
        A requestdict.
      client:
        The Seattle Clearinghouse XMLRPC client to use.
      port:
        The port that all the vessels should have.

    <Side Effects>
      Attempts to obtain vessels described in the request_data. This is not
      guaranteed, depending on the ddata collected in the selexordatabase.

    <Exceptions>
      SeattleClearinghouse.
      InvalidRequestStringError

    <Returns>
      Returns the status of the request, as a string.

      'timeout':
        The request did not finish in the allocated time.
      'complete':
        Selexor successfully finished the request.

    '''
    logger.info(str(identity) + ": Request data:\n" + str(request_data))
    incomplete_groups = copy.copy(request_data['groups'])
    next_groupname = _get_next_group_to_resolve(request_data)

    # Start processing loop
    while   incomplete_groups and\
            next_groupname is not None:
      # If the server shutting down, stop processing.
      if not self._running:
        break
      group = request_data['groups'][next_groupname]
      try:
        logger.info(str(identity) + ": Resolving group: " + str(group))
        group = self.resolve_node(identity, client, group, port)
      except xmlrpc_client.NotEnoughCreditsError, e:
        group['status'] = 'error'
        group['error'] = str(e)
        logger.info(str(identity) + ": Not enough credits.")
      except:
        group['status'] = 'error'
        group['error'] = "An internal error occured while resolving this group."
        logger.error(str(identity) + ": Unknown error while resolving nodes\n" + traceback.format_exc()) 
      if group['status'] in FINISHED_PROCESSING_STATUSES:
        logger.info(str(identity) + ": Group finished processing with status: " + str(group['status']))
        incomplete_groups.pop(group['id'])
      if not incomplete_groups:
        for group in request_data['groups'].values():
          if group['status'] == ['incomplete']:
            group['status'] = 'unresolved'
      # Get the next
      next_groupname = _get_next_group_to_resolve(request_data)

    logger.info(str(identity) + ": Resolution Complete")
    request_data['status'] = 'complete'


##  def _request_data_from_string(self, identity, raw_request):
##    '''
##    <Purpose>
##      Checks that the given request is valid.
##    <Arguments>
##      raw_request:
##        A string representing a selexor host request.
##    <Exceptions>
##      InvalidRequestStringError
##    <Side Effects>
##      None
##    <Returns>
##      A dictionary containing the following keys:
##        'groups': (list of Nodes)
##          A dictionary containing all groups.
##        'num_vessels':
##          The total number of vessels requested.
##        'port':
##          The port to request the vessels on.
##
##    '''
##    content_cell = 1
##    first_rule_cell = 2
##    requests = raw_request.split(';')
##    groups = {}
##    has_errors = False
##
##    for request in requests:
##      # Skip empty lines
##      if not request:
##        continue
##
##      tokens = request.split(":")
##      node_id = tokens[0]
##
##      new_group = {
##          'id': node_id,
##          'rules': {},
##          'status': 'unresolved',
##          'allocate': int(tokens[1]),
##          'acquired': [],
##          'pass': 0,
##          }
##
##      try:
####        rules = parser.rules_from_strings(tokens[first_rule_cell:])
##        rules = parser.preprocess_rules(rules)
##        new_group['rules'] = rules
##      except Exception, e:
##        new_group['error'] = str(e)        
##        logger.info(str(identity) + ": Error while parsing rulestring for node " + node_id + '\n' + traceback.format_exc())
##        has_errors = True
##      except:
##        new_group['error'] = "An internal error occurred."
##        logger.error(str(identity) + ": Error while parsing rulestring for node " + node_id + '\n' + traceback.format_exc())
##        has_errors = True
##      groups[node_id] = new_group
##
##    if has_errors:
##      status = "error"
##    else:
##      status = 'accepted'
##    return {'groups': groups, 'status': status}

  def _validate_request(self, identity, request):
    '''
    <Purpose>
      Checks that the given request is valid.
    <Arguments>
      raw_request:
        A string representing a selexor host request.
    <Exceptions>
      InvalidRequestStringError
    <Side Effects>
      None
    <Returns>
      A dictionary containing the following keys:
        'groups': (list of Nodes)
          A dictionary containing all groups.
        'num_vessels':
          The total number of vessels requested.
        'port':
          The port to request the vessels on.

    '''
    groups = {}
    has_errors = False

    for groupid, groupdata in request.iteritems():
      new_group = {
          'id': groupid,
          'rules': {},
          'status': 'unresolved',
          'allocate': int(groupdata['allocate']),
          'acquired': [],
          'pass': 0,
          }

      try:
        new_group['rules'] = parser.preprocess_rules(groupdata['rules'])
      except Exception, e:
        new_group['error'] = str(e)        
        logger.info(str(identity) + ": Error while parsing rulestring for group " + groupid + '\n' + traceback.format_exc())
        has_errors = True
      except:
        new_group['error'] = "An internal error occurred."
        logger.error(str(identity) + ": Error while parsing rulestring for group " + groupid + '\n' + traceback.format_exc())
        has_errors = True
      groups[groupid] = new_group

    if has_errors:
      status = "error"
    else:
      status = 'accepted'
    return {'groups': groups, 'status': status}


def get_alpha_characters():
  alpha = ""
  uppercase_ord_values = range(ord('A'), ord('Z') + 1)
  lowercase_ord_values = range(ord('a'), ord('z') + 1)
  for char_ord in uppercase_ord_values + lowercase_ord_values:
    alpha += chr(char_ord)
  return alpha


def get_handle_list_from_vesseldicts(vesseldicts):
  handle_list = []
  for vesseldict in vesseldicts:
    handle_list.append(vesseldict['handle'])
  return handle_list


def get_handle_from_nodehandle(nodehandle):
  '''
  <Purpose>
    Given a node handle (e.g. 192.168.1.1:1224:v8), figure out its vesselhandle.
    
  <Parameters>
    nodehandle:
      A string representing a vessel, in the format 'node_id:port:vesselname'
      
  <Exceptions>
    NMClientException
    
  <Side Effects>
    Connect to the given node handle.
    
  <Return>
    The vesselhandle that corresponds with the node handle.

  '''
  nodeid, port, vesselname = nodehandle.split(':')
  port = int(port)
  nmhandle = fastnmclient.nmclient_createhandle(nodeid, port)
  try:
    handleinfo = fastnmclient.nmclient_get_handle_info(nmhandle)
  finally:
    fastnmclient.nmclient_destroyhandle(nmhandle)
  return handleinfo['identity'] + ':' + vesselname

