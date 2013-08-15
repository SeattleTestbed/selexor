"""
<Program Name>
  selexorruleparser.py

<Started>
  July 7, 2012

<Author>
  leon.wlaw@gmail.com
  Leonard Law

<Purpose>
  Contains all the rule parsers and their callbacks.

ruledict:
  A dictionary containing rule parameter definitions.
  Keys are rule names, while the parameters are stored in a dictionary as its values.
  e.g. {  'location-specific': {
            'city': "new york",
            'country': 'us'
          }
          'latency-average': {
            'min_latency': '200ms'
            'max_latency': '400ms'
          }
      }

<Usage>
  To define new rules, a callback function must be defined.
  The callback function must accept the following parameters:
    handleset:
      A set of handle handles.
    cursor:
      A database cursor object.  This is can be acquired by calling db.cursor().
    invert: (bool)
      If set to true, invert the rule.
    parameters: (dictionary)
      A dictionary of parameters that the rule expects.
  The callback function should also return the list of (nodelocation, vesselname)
  that pass the rule.  It should be in the same format as returned by a MySQL
  lookup.

  After defining the callback function, simply place it into the corresponding
  rules dictionary in the _init function.

  vessel_rules:
    Rules that operate on independent vessels. These are generally rules that
    use properties that are for the most part, do not change often and can be
    easily looked up. E.g. vessel location, vessel type, vessel IP change count.

  group_rules:
    Rules that operate on groups of vessels. These rules use properties that are
    dynamic, and must be calculated at runtime. E.g. average latency, radius
    between acquired vessels

  ruledict:
    This dictionary should have rulenames as keys. These keys will map to
    dictionaries containing parameter/value pairs to the specified rule.

    For example, a ruledict specifying that vessels should be from
    San Francisco, USA and have an average latency less than 400ms would look
    like this:
    { 'location_specific': {'city': 'san francisco', 'country': 'usa'},
      'average_latency': {'min_latency': 0, 'max_latency': 400}
    }





"""
import selexorhelper
import selexorexceptions
from copy import deepcopy



rule_callbacks = {
  'group': {},
  'vessel': {}
}

parameter_preprocess_callbacks = {}

all_known_rules = set()

def rules_from_strings(strings):
  rules = {}
  for string in strings:
    # Skip empty strings
    if not string:
      continue
    string = string.lower()
    rule_params = {}

    if string.startswith('!'):
      # If the 'invert' key is in the dictionary, then the rule will invert.
      rule_params['invert'] = True
      string = string[1:]

    # Parameters are in the format of:
    # [param_name] '~' [param_value]
    # They are always in pairs
    parameters = string.split(",")
    rule_name = parameters[0]

    if rule_name in rules:
      raise selexorexceptions.SelexorInvalidRequest("A rule was specified multiple times!")
    parameters = parameters[1:]
    for parameter in parameters:
      (param_type, param_value) = parameter.split('~')
      rule_params[param_type] = param_value

  return rules


def preprocess_rules(rules):
  try:
    for rule_name, rule_params in rules.iteritems():
      # Only preprocess if preprocessor is available
      if parameter_preprocess_callbacks[rule_name]:
        replacement_params = parameter_preprocess_callbacks[rule_name](rule_params)
        if replacement_params is None:
          logger.error("Rule does not return parameters: " + rule_name)
        else:
          rule_params = replacement_params
      rules[rule_name] = rule_params
    return rules
  except KeyError, e:
    raise selexorexceptions.UnknownRule(str(e) + ' is not a recognized rule')

def has_group_rules(rules):
  '''
  <Purpose>
    Iterates through the given rule dictionary to see if there are any group
    rules.
  <Parameters>
    rules: A ruledict. See module documentation for more information.
  <Exceptions>
    None
  <Side Effects>
    None
  <Return>
    True if there are group rules, False otherwise

  '''
  for rulename in rules:
    if rulename in rule_callbacks['group']:
      return True
  return False


def apply_vessel_rules(rules, cursor, vesselset):
  '''
  <Purpose>
    Parse handles within handleset based on the specified rules. This should be
    called once every pass.
  <Arguments>
    rules: (dict)
      A dictionary containing ruletypes and their parameters. See the rule callbacks
      for more information regarding the parameters.
    cursor: MySQLdb cursor
      A cursor to the MySQLdb that contains the latest vessel information.
    vesselset:
      The set of vessels to consider for these rules.
  <Exceptions>
    None
  <Side Effects>
    Applies all known rules onto the input set.
  <Return>
    The set of vessels that satisfy the given condition.

  '''
  vesselset = set(vesselset)
  for rule_name, rule_params in rules.iteritems():
    if rule_name in rule_callbacks['vessel']:
      invert = 'invert' in rule_params
      vesselset.intersection_update(rule_callbacks['vessel'][rule_name](
                      cursor,
                      invert,
                      rule_params))
  return vesselset


def apply_group_rules(rules, cursor, vesselset, acquired_vessels):
  '''
  <Purpose>
    Parse handles within handleset based on the specified rules.
    This should be called as many times as needed until either:
      No more vessels remain in the vesselset, or;
      The vessels acquired
  <Arguments>
    rules: (dict)
      A ruledict. See module documentation for more information.
    cursor: MySQLdb cursor
      A cursor to the MySQLdb that contains the latest vessel information.
    handleset:
      The set of handles to consider for these rules.
  <Exceptions>
    None
  <Side Effects>
    Applies all known rules onto the input set.
  <Return>
    The set of handles that satisfy the given condition.
  '''
  # We need at least one vessel before we can start applying group rules.
  # Need not apply to all rules... i.e. if separation distance is specified,
  # all acquired vessels MUST have coordinates.
  if not acquired_vessels:
    return vesselset
  vesselset = set(vesselset)

  for rule_name, rule_params in rules.iteritems():
    if rule_name in rule_callbacks['group']:
      invert = 'invert' in rule_params
      vesselset.intersection_update(rule_callbacks['group'][rule_name](
                      cursor,
                      invert,
                      rule_params,
                      acquired_vessels))
  return vesselset


def get_worst_vessel(acquired_vessels, handleset, cursor, rules):
  '''
  <Purpose>
    Returns the vessel that, when removed, gives the largest accessible
    vesselset.
  <Arguments>
    acquired_vessels: list of vessel handles currently acquired.
    handleset: The set of all valid handles. (without group-level rules applied)
    cursor: The cursor that we should use to check the database.
    rules: The rules to use.
  <Exceptions>
    ValueError
  <Side Effects>
    None
  <Return>
    The vesselhandle of the vessel that should be removed.

  '''
  worst_vessel = None
  largest_accessible_vessels_size = -1
  if not acquired_vessels:
    raise ValueError("No vessels have been acquired")
  for vessel in acquired_vessels:
    acquired_vessels_except_one = deepcopy(acquired_vessels)
    acquired_vessels_except_one.remove(vessel)
    accessible_vessels = apply_group_rules(rules, cursor, handleset, acquired_vessels_except_one)
    if len(accessible_vessels) > largest_accessible_vessels_size:
      largest_accessible_vessels = accessible_vessels
      worst_vessel = vessel
  return worst_vessel


def _specific_location_preprocessor(parameters):
  '''
  <Purpose>
    Rule Preprocesor for ip_change_count.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'city':
        The city's name. It should be '?' if left blank.
    'country':
        The 2-letter ISO-3166 country code.
  <Exceptions>
    MissingParameter
    BadParameter
  <Side Effects>
    After running:
      'city' should be present if the user specified a city.
      'country' should be a valid ISO-3166-2 country code.

  '''
  required_parameters = ['city', 'country']
  for parameter in required_parameters:
    if parameter not in required_parameters:
      raise selexorexceptions.MissingParameter(parameter)

  retdict = {}
  try:
    if parameters['city'] == '?':
      retdict['city'] = None
    else:
      retdict['city'] = selexorhelper.get_city_id(parameters['city'])
    retdict['country_code'] = selexorhelper.get_country_id(parameters['country'])
  except selexorexceptions.UnknownLocation, e:
    raise selexorexceptions.BadParameter(str(e))

  return retdict


def _different_location_preprocessor(parameters):
  '''
  <Purpose>
    Rule Preprocesor for ip_change_count.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'location_count':
        The number of locations that must be present. This should either be a
        numeric string, or the string "infinity".
        Expected Range: [0, Infinity)
    'location_type':
        The type of location that should be differentiated. It can be 'city',
        'cities', 'country', or 'countries'.

  <Exceptions>
    ValueError
    MissingParameter
    BadParameter
  <Side Effects>
    After running:
      location_count must either be an int, or a float with the value +Infinity.
      location_type must be either 'cities' or 'countries'

  '''
  required_parameters = ['location_count', 'location_type']
  for parameter in required_parameters:
    if parameter not in required_parameters:
      raise selexorexceptions.MissingParameter(parameter)
  try:
    parameters['location_count'] = float(parameters['location_count'])
    if parameters['location_count'] == float('inf'):
      parameters['location_count'] = 2 ** 32
    else:
      parameters['location_count'] = int(parameters['location_count'])
  except ValueError:
    selexorexceptions.BadParameter("Location count must be a number!")
  if parameters['location_count'] <= 0:
    raise selexorexceptions.BadParameter("Location count must be a positive integer!")

  parameters['location_type'] = parameters['location_type'].lower()

  if not parameters['location_type'] in ['city', 'country_code']:
    raise selexorexceptions.BadParameter("Unknown location type: " + parameters['location_type'])
  return parameters


def _specific_location_parser(cursor, invert, parameters):
  '''
  <Purpose>
    Vessel-Level Rule. Performs location-based parsing for handles.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'city': City name. This field is optional. Leave it at None to ignore
            a handle's city.
    'country': Country identifier. This should be a ISO 3166 2-letter
               identifier.

  '''
  # Get the nodelocations that are good.
  # The city field is optional
  if parameters['city'] is None:
    condition = 'location.country_code="'+parameters['country_code']+'"'
  else:
    condition = 'location.city="'+parameters['city']+'" AND location.country_code="'+parameters['country_code']+'"'
  if invert:
    condition = 'NOT ' + condition

  query = """SELECT node_id, vessel_name FROM 
      (SELECT ip_addr FROM location WHERE """+condition+""") as valid_ips
      LEFT JOIN nodes USING (ip_addr) LEFT JOIN vessels USING (node_id)"""
  logger.debug(query)
  cursor.execute(query)
  return cursor.fetchall()


def _separation_radius_preprocessor(parameters):
  '''
  <Purpose>
    Rule Preprocesor for separation_radius.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'min_radius', 'max_radius':
      These indicate the minimum and maximum radii for every vessel pair in
      the group, in kilometers.
      Expected Range: [0, Infinity)
  <Exceptions>
    ValueError - Parameter(s) passed in are not floats
    MissingParameter - Parameters are missing
  <Side Effects>
    After running:
      min_radius and max_radius must both be floats
      min_radius <= max_radius

  '''
  required_parameters = ['min_radius', 'max_radius']
  for parameter in required_parameters:
    if parameter not in required_parameters:
      raise selexorexceptions.MissingParameter(parameter)

  for parameter in required_parameters:
    parameters[parameter] = float(parameters[parameter])
  if parameters['min_radius'] > parameters['max_radius']:
    # Switch min/max if necessary
    (parameters['min_radius'], parameters['max_radius']) = (parameters['max_radius'], parameters['min_radius'])
  return parameters


def _ip_change_count_preprocessor(parameters):
  '''
  <Purpose>
    Rule Preprocesor for ip_change_count.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'min_change', 'max_change':
      These indicate the minimum and maximum IP address change for each vessel
      in the group.
      Expected Range: [0, Infinity)
  <Exceptions>
    ValueError
    MissingParameter
  <Side Effects>
    After running:
      min_change and max_change must both be floats.
      min_change <= max_change.

  '''
  required_parameters = ['min_change', 'max_change']
  for parameter in required_parameters:
    if parameter not in required_parameters:
      raise selexorexceptions.MissingParameter(parameter)

  for parameter in required_parameters:
    parameters[parameter] = float(parameters[parameter])

  if parameters['min_change'] > parameters['max_change']:
    temp = parameters['min_change']
    parameters['min_change'] = parameters['max_change']
    parameters['max_change'] = temp
  return parameters


def _node_type_preprocessor(parameters):
  '''
  <Purpose>
    Rule Preprocesor for node_type.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'node_type':
      The type of nodes that the user wants.
      This should be a value in selexorhelper.VALID_NODETYPES.
  <Exceptions>
    MissingParameter
  <Side Effects>
    None

  '''
  required_parameters = ['node_type']

  if 'node_type' not in parameters:
    raise selexorexceptions.MissingParameter(parameter)
  if parameters['node_type'] not in selexorhelper.VALID_NODETYPES:
    raise selexorexceptions.BadParameter(parameters['node_type']+" must be a\
      value in "+str(selexorhelper.VALID_NODETYPES))

  return parameters


def _port_preprocessor(parameters):
  '''
  <Purpose>
    Rule Preprocesor for port.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'port': The port value that all vessels in the group must have.
  <Exceptions>
    ValueError
    MissingParameter
  <Side Effects>
    After running:
      port must be an int.

  '''
  required_parameters = ['port']
  for parameter in required_parameters:
    if parameter not in required_parameters:
      raise selexorexceptions.MissingParameter(parameter)

  for parameter in required_parameters:
    parameters[parameter] = int(float(parameters[parameter]))

  return parameters







def _separation_radius_parser(cursor, invert, parameters, acquired_vessels):
  '''
  <Purpose>
    Group-Level Rule. Performs distance-based parsing for handles.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'min_radius', 'max_radius':
        The radii range of which the vessels must be in, in kilometers.
        Expected Range: [0, Infinity)

  '''

  acquired_coordinates = set()
  # Get the coordinates of the acquired vessels
  # acquired_vessels is a list of vesseldicts
  for vesseldict in acquired_vessels:
    # We may have NULL/NULL for the coordinate data.
    # Make sure we don't fetch any of those entries.
    if cursor.execute('''
        SELECT longitude, latitude FROM location LEFT JOIN nodes
        USING (ip_addr) WHERE longitude IS NOT NULL AND latitude IS NOT NULL
        AND node_id='''+str(vesseldict['node_id'])) == 1L:

      acquired_coordinates.add(cursor.fetchone())

  # Compile the list of good nodelocations
  # Performing this on the database is really slow...
  # We might as well do it here to avoid having too much pressure on the DB.
  good_nodes = []
  cursor.execute('''
      SELECT DISTINCT node_id, longitude, latitude FROM location LEFT JOIN nodes
      USING (ip_addr) WHERE longitude IS NOT NULL and latitude IS NOT NULL''')
  for node_id, longitude, latitude in cursor.fetchall():
    good_radius = True
    for acquired_longitude, acquired_latitude in acquired_coordinates:
      distance = selexorhelper.haversine_distance(longitude, latitude, acquired_longitude, acquired_latitude)
      good_radius = distance >= parameters['min_radius'] and \
                    distance <= parameters['max_radius']
      # If distance to one is incorrect, then we don't need to check the rest
      if not good_radius:
        break
    if invert ^ good_radius:
      good_nodes.append(node_id)

  # Of the list of good nodelocations, compile the set of good vessels
  good_vessels = []
  for node_id in good_nodes:
    cursor.execute('SELECT node_id, vessel_name FROM vessels WHERE node_id='+str(node_id))
    good_vessels += cursor.fetchall()

  return good_vessels


def _different_location_type_parser(cursor, invert, parameters, acquired_vessels):
  '''
  <Purpose>
    Group-Level Rule. Performs location type-based parsing for handles.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'location_count':
        The maximum number of unique locations to have. While this number is not
        reached, each vessel in the group will be from a unique location.
        Expected Range: [1, Infinity)
    'location_type':
        The kind of location that is differentiated. 'cities' or 'countries'.

  '''
  locations= set()
  # Compile list of locations
  for vesseldict in acquired_vessels:
    nodekey = vesseldict['handle'].split(':')[0]
    query = """
      SELECT """+parameters['location_type']+""" FROM
        (SELECT ip_addr FROM nodes WHERE node_key='"""+nodekey+"""') AS node_row
      LEFT JOIN location USING (ip_addr)"""
    selexorhelper.autoretry_mysql_command(cursor, query)
    locations.add(cursor.fetchone()[0])

  query = parameters['location_type'] + " "

  # If we have enough locations, we want vessels to only be from the
  # already acquired locations.
  # If we don't have enough locations, we want vesels to not be from
  # the already acquired locations.

  # Truth table:
  #                        |  Invert  | Dont invert
  # Not enough locations   |    IN    |   NOT IN
  # Enough Locations       |  NOT IN  |     IN
  if ((not invert and len(locations) < parameters['location_count']) or
      (invert and len(locations) == parameters['location_count'])):
    query += 'NOT '
  query += 'IN ("'+'", "'.join(locations)+'")'
  query = """
    SELECT node_id, vessel_name FROM
      (SELECT node_id FROM
        (SELECT ip_addr FROM location WHERE city NOT IN ("""+query+""")
        ) as matching_locations LEFT JOIN nodes using (ip_addr)
      ) as matching_nodes LEFT JOIN vessels USING (node_id)"""
  logger.debug(query)
  cursor.execute(query)

  return cursor.fetchall()


def _ip_change_count_parser(handleset, database, invert, parameters):
  '''
  <Purpose>
    Vessel-Level Rule. Performs IP-change parsing for handles.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'min_change', 'max_change':
        Floats indicating the range of IP changes to accept.
        Expected Range: [0, Infinity)

  '''
  good_handles = set()
  for ip_change_count in database.ip_change_table:
    if  parameters['min_change'] <= ip_change_count and \
        ip_change_count <= parameters['max_change']:
      good_handles = good_handles.union(database.ip_change_table[ip_change_count])
  if invert:
    good_handles = handleset - good_handles
  return good_handles


def _node_type_parser(cursor, invert, parameters):
  '''
  <Purpose>
    Vessel-Level Rule. Ensures that all vessels in the group are of the
    specified type.

    This is a rule callback. See the Usage section of the module
    docstring for more information.
  <Arguments>
    'node_type':
      The node type to filter.
      This should be a value in selexorhelper.VALID_NODETYPES.

  '''
  good_handles = set()
  node_type = parameters['node_type']
  if not invert:
    query = (
      "SELECT node_id, vessel_name FROM vessels WHERE node_id IN "
      "(SELECT node_id FROM nodes WHERE node_type='"+node_type+"')"
      )
  else:
    query = (
      "SELECT node_id, vessel_name FROM vessels WHERE node_id IN "
      "(SELECT node_id FROM nodes WHERE node_type!='"+node_type+"')"
      )

  selexorhelper.autoretry_mysql_command(cursor, query)
  return cursor.fetchall()


def _port_parser(cursor, invert, parameters):
  '''
  <Purpose>
    Vessel-Level Rule. Ensures that all vessels in the group have the specified port number.

    This is a rule callback. See the Usage section of the module docstring for more
    information.
  <Arguments>
    'port': The port number that all vessels in the set must have available.

  '''
  good_handles = set()
  port = parameters['port']
  if not invert:
    query = "SELECT node_id, vessel_name FROM vesselports WHERE port="+str(port)
  else:
    query = "SELECT node_id, vessel_name FROM vesselports WHERE port !="+str(port)
  logger.debug(query)
  selexorhelper.autoretry_mysql_command(cursor, query)
  return cursor.fetchall()






def register_callback(rule_name, rule_type, acquire_callback, parameter_preprocess_callback = None):
  '''
  <Purpose>
    Registers the callback in the rule parser.

  <Arguments>
    rule_name: The name of the rule.
    rule_type: The type of rule.
    acquire_callback:
        The function to call when parsing vessels during acquisition.
    parameter_preprocess_callback:
        The function to call to check if parameter values are correct, and
        optionally preprocess the parameter values if needed.
        Unless your rule only operates on strings, you will need to preprocess
        parameters.

  <Side Effects>
    Rules with the specified rule name will now use the specified callbacks.

  <Exceptions>
    InvalidRuleType
    InvalidRuleReregistration

  <Returns>
    None

  '''
  if rule_type not in rule_callbacks:
    raise selexorexceptions.SelexorInvalidOperation("Bad rule type: " + rule_type)
  # Make sure that this rule doesn't already exist
  for ruleset in rule_callbacks.values():
    if rule_name in ruleset:
      raise SelexorInvalidOperation("Rule already exists: " + rule_name)
  all_known_rules.add(rule_name)
  rule_callbacks[rule_type][rule_name] = acquire_callback
  parameter_preprocess_callbacks[rule_name] = parameter_preprocess_callback


def deregister_callback(rule_name):
  '''
  <Purpose>
    Registers the callback in the rule parser.

  <Arguments>
    rule_name: The name of the rule.
    rule_type: The type of rule.
    acquire_callback:
        The function to call when parsing vessels during acquisition.
    parameter_preprocess_callback:
        The function to call to check if parameter values are correct, and
        optionally preprocess the parameter values if needed.
        Unless your rule only operates on strings, you will need to preprocess
        parameters.

  <Side Effects>
    Rules with the specified rule name will now use the specified callbacks.

  <Exceptions>
    InvalidRuleType
    InvalidRuleReregistration

  <Returns>
    None

  '''
  for ruleset in rule_callbacks.values():
    if rule_name in ruleset:
      ruleset.pop(rule_name)
      return
  raise selexorexceptions.SelexorInvalidOperation("Rule does not exist: ", rule_name)



def _init():
  global logger
  logger = selexorhelper.setup_logging(__name__)

  register_callback('location_specific', 'vessel', _specific_location_parser, _specific_location_preprocessor)
  register_callback('location_separation_radius', 'group', _separation_radius_parser, _separation_radius_preprocessor)
  register_callback('location_different', 'group', _different_location_type_parser, _different_location_preprocessor)
  register_callback('num_ip_change', 'vessel', _ip_change_count_parser, _ip_change_count_preprocessor)
  register_callback('node_type', 'vessel', _node_type_parser, _node_type_preprocessor)
  register_callback('port', 'vessel', _port_parser, _port_preprocessor)



_init()