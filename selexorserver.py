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

    STATUS_RESOLVED:
      The group has finished processing.

    STATUS_INCOMPLETE:
      The group has not finished processing, and has not been processed
      on the active pass.

    STATUS_UNRESOLVED:
      The group has not finished processing, but has already been
      processed on the active pass.

    STATUS_FAILED:
      The group could not be completed, because the number of passes
      attempted has exceeded the maximum allowed limit.

    STATUS_ERROR:
      The group could not be completed because of some unexpected error.

  ruledict:
    A dictionary representing the rules that a group should follow. See the
    documentation for selexorruleparser.py for more information.


"""

import seattleclearinghouse_xmlrpc
import copy
import selexorruleparser
import selexorhelper
import random
import fastnmclient
import threading
import traceback
import selexorexceptions
import MySQLdb
import settings

import repyhelper
repyhelper.translate_and_import('rsa.repy')


logger = selexorhelper.setup_logging(__name__)


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


STATUS_RESOLVED = 'resolved'
STATUS_FAILED = 'failed'
STATUS_INCOMPLETE = 'incomplete'
STATUS_ERROR = 'error'
STATUS_UNRESOLVED = 'unresolved'


class SelexorServer:
  def __init__(self):
    '''
    <Purpose>
      Creates an instance of a selexor server.
    <Arguments>
      None
    <Side Effects>
      Initializes the GeoIP client.
      Initializes a selexordatabase.
    <Exceptions>
      None
    <Returns>
      A selexorserver instance.

    '''
    self._accepting_requests = True
    self._running = True



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


  def release_vessels(self, authdata, vessels_to_release, remoteip):
    '''
    <Purpose>
      Returns the # of vessels released
    <Arguments>
      authdata:
        An authdict. See module documentation for more information.
      vessels:
        A list of dictionaries containing vessel information of the vessels
        to release.  These dictionaries should either contain the vessel handle,
        or node_ip:node_port:vesselname.
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
      logger.info(str(identity) + "> Release: " + str(vessels_to_release))

      # There's nothing to do if there aren't any vessels to release
      if not vessels_to_release:
        return 0

      handles_of_vessels_to_release = []
      for vesseldict in vessels_to_release:
        if 'node_handle' in vesseldict:
          handles_of_vessels_to_release.append(vesseldict['node_handle'])
        else:
          # Do we have this information in the database?
          db, cursor = selexorhelper.connect_to_db()

          # If it is found, the lookup returns a 1L.
          node_in_db = 1L == cursor.execute("SELECT node_key FROM nodes WHERE ip_addr='"+vesseldict['node_ip']+"' AND node_port="+str(vesseldict['node_port']))
          if node_in_db:
            [node_key] = cursor.fetchone()
            logger.debug('\n'.join([
                str(identity),
                "Found node in database: "+vesseldict['node_ip']+':'+str(int(vesseldict['node_port']))+':'+vesseldict['vesselname']+" with key:",
                node_key]))
            handles_of_vessels_to_release.append(node_key+':'+vesseldict['vesselname'])

          else:
            # Try to connect to that node to get the handle
            vessel_location = vesseldict['node_ip']+':'+str(int(vesseldict['node_port']))+':'+vesseldict['vesselname']
            try:
              handles_of_vessels_to_release.append(get_handle_from_nodehandle(vessel_location))
            except fastnmclient.NMClientException, e:
              logger.info("Failed to look up vessel "+vessel_location+' through nodemanager: '+ str(e))

      client = selexorhelper.connect_to_clearinghouse(authdata)

      # Release the remaining vessels
      for vessel in handles_of_vessels_to_release:
        # Do we need to check if a vessel failed to release?
        # Maybe it means that a vessel has gone offline/is now invalid.
        client.release_resources([vessel])

      # Assume all the vessels were released successfully
      num_released = len(handles_of_vessels_to_release)

      # Remove vessel entries from the groups tables.
      if identity in request_datum:
        for group in request_datum[identity]['groups']:
          for vesselhandle in handles_of_vessels_to_release:
            if vesselhandle in request_datum[identity]['groups'][group]['acquired']:
              request_datum[identity]['groups'][group]['acquired'].remove(vesselhandle)
              request_datum[identity]['groups'][group]['allocate'] -= 1

    except Exception, e:
      logger.error(str(identity) +': Unknown error while releasing vessels\n' + traceback.format_exc())
      return (False, "Internal error occurred.")

    logger.info(str(identity) +': Successfully released ' + str(len(handles_of_vessels_to_release)) + ' vessel(s)')
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
    db, cursor = selexorhelper.connect_to_db()

    cursor = db.cursor()

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
        for vesseldict in group['acquired']:
          vesselhandle = vesseldict['handle']
          nodeinfo = {}

          nodekey, nodeinfo['vesselname'] = vesselhandle.split(':')
          cursor.execute('SELECT ip_addr, node_port FROM nodes WHERE node_key="'+nodekey+'"')
          nodeinfo['node_ip'], nodeinfo['node_port'] = cursor.fetchone()
          nodeinfo['handle'] = vesselhandle

          group_data['vessels_acquired'].append(nodeinfo)

        group_data['target_num_vessels'] = group['allocate']
    except Exception, e:
      logger.error(str(identity) + ": Error while responding to status query\n" + traceback.format_exc())
      data['error'] = str(e)
    return data


  def resolve_node(self, identity, client, node, db, cursor):

    # We should never run into these...
    if node['status'] == STATUS_RESOLVED:
      logger.error(str(identity) + ": Group already resolved: " + str(node))
      return node
    elif node['status'] == STATUS_FAILED:
      logger.error(str(identity) + ": Exceeded pass limit: " + str(node))
      return node

    logger.info(str(identity) + ": Group " + node['id'] + " on Pass " + str(node['pass']))
    vessels_to_acquire = []
    remaining = node['allocate'] - len(node['acquired'])

    selexorhelper.autoretry_mysql_command(cursor, "SELECT node_id, vessel_name FROM vessels WHERE acquirable")
    all_vessels = cursor.fetchall()

    # Get vessels that match the vessel rules
    handles_vesselrulematch = selexorruleparser.apply_vessel_rules(node['rules'], cursor, all_vessels)
    logger.info(str(identity) + ": Vessel-level matches: " + str(len(handles_vesselrulematch)))

    # The number of times we tried to resolve this group in the current attempt
    in_group_retry_count = 0
    MAX_IN_GROUP_RETRIES = 3

    candidate_vessels = []

    while len(candidate_vessels) < remaining and \
          in_group_retry_count < MAX_IN_GROUP_RETRIES:

      if not self._running:
        # Stop if we receive a quit message
        break

      if node['pass'] >= MAX_PASSES_PER_NODE:
        raise selexorexceptions.SelexorInternalError("Performing more passes than max pass!")

      handles_grouprulematch = selexorruleparser.apply_group_rules(
          cursor = cursor,
          acquired_vessels = candidate_vessels,
          rules = node['rules'],
          vesselset = handles_vesselrulematch)

      # Pick any vessel.
      vessellist = list(handles_grouprulematch)
      if vessellist:
        logger.info(str(identity) + ": Candidates for next vessel: " + str(len(vessellist)))
        # If we run out of handles, we simply get another random one, instead of
        # programming a special case.
        node_id, vesselname = random.choice(vessellist)
        vessellist.remove((node_id, vesselname))

        selexorhelper.autoretry_mysql_command(cursor, 'SELECT node_key FROM nodes WHERE node_id='+str(node_id))
        nodekey = cursor.fetchone()[0]
        handle = nodekey + ':' + vesselname
        logger.info(str(identity)+":\n"+"Considering: "+str(handle))

        # node_id and vessel_name are used extensively by rule parsers
        # We should include them here to prevent each rule from looking the up
        vessel_dict = {
          'handle': handle,
          'node_id': node_id,
          'node_key': nodekey,
          'vessel_name': vesselname,
        }
        candidate_vessels.append(vessel_dict)

      # We ran out of vessels to check
      else:
        logger.info(str(identity) + ": Can't find any suitable vessels!")
        in_group_retry_count += 1

        # Have we exceeded the maximum in-group retry count?
        if in_group_retry_count >= MAX_IN_GROUP_RETRIES:
          break

        # We retry if there could be another combination that MIGHT satisfy
        # the group rules. If there are no group rules, there is no point
        # to retry.
        if not selexorruleparser.has_group_rules(node['rules']):
          logger.info(str(identity) + ": There are no group rules applied; no point in retrying.")
          break

        if candidate_vessels:
          # Get the vessel that causes the largest drop in the
          # size of the available vessel pool
          worst_vessel = selexorruleparser.get_worst_vessel(
              candidate_vessels,
              handles_grouprulematch,
              cursor,
              node['rules'])

          # Release the worst vessel so that we can try to get a better one
          # in the next iteration
          logger.info(str(identity) + ": Releasing: " + str(worst_vessel))
          candidate_vessels.remove(worst_vessel)
          client.release_resources([worst_vessel['handle']])

    # We may get vessels that are unusable (i.e. extra vessels containing
    # leftover resources).  If so, drop them and try again
    while candidate_vessels:
      vessels_to_acquire = []
      for vesseldict in candidate_vessels:
        vessels_to_acquire.append(vesseldict['handle'])

      try:
        acquired_vesseldicts = client.acquire_specific_vessels(vessels_to_acquire)
        logger.info(str(identity)+": Requested "+str(len(vessels_to_acquire))+" vessels, acquired "+str(len(acquired_vesseldicts))+":\n"+'\n'.join(i['handle'][-10:] +':'+ i['vessel_id'] for i in acquired_vesseldicts))
        node['acquired'] += candidate_vessels
        break

      except seattleclearinghouse_xmlrpc.NotEnoughCreditsError, e:
        logger.error(str(identity) + ": Not enough vessel credits")
        raise
      except seattleclearinghouse_xmlrpc.InvalidRequestError, e:
        error_string = str(e)
        # This may be an extra vessel.
        if 'There is no vessel with the node identifier' in error_string:
          logger.error(str(identity) + ": " + str(e))
          extra_vessels = []
          for vessel in candidate_vessels:
            if (vessel['node_key'] in error_string and
                vessel['vessel_name'] in error_string):
              extra_vessels.append(vessel)

          for vessel in extra_vessels:
            candidate_vessels.remove(vessel)
            logger.info("Removing: ..." + vessel['node_key'][-10:] + ':' + vessel['vessel_name'])

          # Store into the db so that future lookups do not need to
          # spend time acquiring the vessel to discover that it is not
          # acquirable, as there are no definitive ways of determining
          # if a vessel is non-acquirable, aside from the management
          # vessel (v2)
          update_command = ("UPDATE vessels SET acquirable=false \
            WHERE (node_id, vessel_name) IN (" +
              ", ".join( "(%s, '%s')" % (handle['node_id'], handle['vessel_name'])
                for handle in extra_vessels
              ) +
            ")")

          selexorhelper.autoretry_mysql_command(cursor, update_command)
          db.commit()

        else:
          logger.error(str(identity) + ": " + str(e))
          raise

      except Exception, e:
        logger.error(str(identity) +": Unknown error while acquiring vessels\n" + traceback.format_exc())
        pdb.set_trace()
        raise selexorexceptions.SelexorInternalError(str(e))

    node['pass'] += 1
    if node['pass'] >= MAX_PASSES_PER_NODE:
      logger.info(str(identity) + ": Group exceeds pass limit. Designating group as failed: " + str(node))
      node['status'] = STATUS_FAILED
    elif len(node['acquired']) == node['allocate']:
      node['status'] = STATUS_RESOLVED
    else:
      node['status'] = STATUS_INCOMPLETE
    return node


  def handle_request(self, authinfo, request, remoteip):
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

      remoteip: The IP address where this request originated from.

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
        client = selexorhelper.connect_to_clearinghouse(authinfo)
      except selexorexceptions.SelexorException, e:
        request_data['status'] = 'error'
        request_data['error'] = str(e)
        raise
      except:
        request_data['status'] = 'error'
        request_data['error'] = "An internal error occurred."
        logger.error(str(identity) + ": Error connecting to clearinghouse" + traceback.format_exc())
        raise

      # Start working
      request_data['status'] = "working"
      logger.error(str(identity) + ": Working on request")
      resolution_thread = threading.Thread(target=self.serve_request, args=(identity, request_data, client))
      resolution_thread.start()

    else:
      logger.info(str(identity) + ": Could not process request")
    return self.get_request_status(authinfo, remoteip)


  def serve_request(self, identity, request_data, client):
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
    db, cursor = selexorhelper.connect_to_db()

    for groupname, group in request_data['groups'].iteritems():
      pass_no = 0
      while pass_no < 5:
        try:
          logger.info(str(identity) + ": Resolving group: " + str(groupname))
          group = self.resolve_node(identity, client, group, db, cursor)
          # We are done here, no need to proceed with the remaining
          # passes
          if group['status'] != STATUS_INCOMPLETE:
            break

        except seattleclearinghouse_xmlrpc.NotEnoughCreditsError, e:
          group['status'] = 'error'
          group['error'] = str(e)
          logger.info(str(identity) + ": Not enough credits.")
          request_data['status'] = 'error'
          return
        except:
          group['status'] = 'error'
          group['error'] = "An internal error occured while resolving this group."
          logger.error(str(identity) + ": Unknown error while resolving nodes\n" + traceback.format_exc())
          request_data['status'] = 'error'
          return

        pass_no += 1

    logger.info(str(identity) + ": Resolution Complete")
    request_data['status'] = 'complete'


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
          'status': STATUS_UNRESOLVED,
          'allocate': int(groupdata['allocate']),
          'acquired': [],
          'pass': 0,
          }

      try:
        new_group['rules'] = selexorruleparser.preprocess_rules(groupdata['rules'])
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
    vesseldict = fastnmclient.nmclient_getvesseldict(nmhandle)
  finally:
    fastnmclient.nmclient_destroyhandle(nmhandle)
  return rsa_publickey_to_string(vesseldict['nodekey']) + ':' + vesselname

