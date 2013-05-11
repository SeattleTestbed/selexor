"""
<Program Name>
  selexorruleparserhelper.py

<Started>
  July 24, 2012

<Author>
  leon.wlaw@gmail.com
  Leonard Law

<Purpose>
  Contains helper functions that are needed by selexor.

"""
import math
import selexorexceptions
import os
import seattleclearinghouse_xmlrpc
import MySQLdb
import settings

helpercontext = {}


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


def initialize():
  helpercontext['COUNTRY_TO_ID'] = load_ids('country')
  # This takes up too much memory...
  # helpercontext['CITY_TO_ID'] = load_ids('city')


def load_config_with_file(configname, configuration):
  '''
  <Purpose>
    Loads the configuration file and applies the changes listed to the 
    configuration.
    
  <Arguments>
    configname: 
      The name of the configuration file to open, without the '.conf' extension.
    configuration:
      The configuration dictionary to modify. This can be empty.
      
  <Side Effects>
    Opens the configuration file named configfn + '.conf', and loads the
    configuration details into a dict.
    All whitespace surrounding key/value entries in the configuration file will 
    be ignored. e.g. "  hello world  " and "hello world" are identical.

  <Exceptions>
    None
    
  <Returns>
    The modified configuration.
    
  '''
  
  # The parameters that require casts. 
  # Keys are the parameter names, values are the types to cast to. 
  cast_type = {
    'http_port': int,
    'advertise_port': int,
    'num_probe_threads': int,
    'probe_delay': int,
    'allow_ssl_insecure': bool
  }

  configuration['server_name'] = configname
  configfile = open(configname + '.conf', 'r')

  # File parse loop.
  data = configfile.readline()
  while data:
    # Characters after '#' are comments.
    # Take all characters preceding it and get rid of any surrounding whitespace.
    data = data.split('#', 1)[0].strip()
    # Ignore empty strings
    if data:
      # Each entry is in the format of: "Key: Value"
      key, value = data.split(":", 1)
      key = key.strip()
      value = value.strip()
      # Cast to correct type if needed
      if key in cast_type:
        # Default bool casting would require users to type '' -> False, and 
        # anything else -> True.
        if cast_type[key] == bool:
          if value.lower() == 'true':
            value = True
          elif value.lower() == 'false':
            value = False
          else:
            raise ValueError("Invalid value '" + value + "' for key '" + key + 
              "' in config " + configname + ", expected " + str(cast_type[key]))
        else:
          value = cast_type[key](value)
      # Insert into configuration
      configuration[key] = value
    data = configfile.readline()
    # End of file parse loop
  return configuration


def get_city_id(cityname):
  # City table takes up several hundred MB of space... We treat all city names
  # as valid for now
  return cityname


def get_country_id(countryname):
  countryname = countryname.lower()
  try:
    return helpercontext['COUNTRY_TO_ID'][countryname]
  except KeyError, e:
    raise selexorexceptions.UnknownLocation(countryname)


def load_ids(idtype):
  id_file = open('./lookup/' + idtype + '.txt', 'r')
  id_map = {}

  line = id_file.readline().lower()
  while line:
    ids = line.split('\t')
    good_id = ids[0].strip()
    for id in ids:
      id_map[id.strip()] = good_id

    line = id_file.readline().lower()
  id_file.close()
  return id_map


def connect_to_clearinghouse(authdata):
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
  
  logger.info("Connecting to the clearinghouse on behalf of "+username)
  client = seattleclearinghouse_xmlrpc.SeattleClearinghouseClient(
    username = username,
    api_key = apikey,
    private_key_string = private_key_string,
    xmlrpc_url = settings.clearinghouse_xmlrpc_url,
    allow_ssl_insecure = settings.allow_ssl_insecure)
  return client


def haversine_distance(long1, lat1, long2, lat2):
  '''
  Given two coordinates, calculate the great circle distance between them.
  These coordinates are specified in decimal degrees.

  '''
  # convert decimal degrees to radians
  long1 = math.radians(long1)
  long2 = math.radians(long2)
  lat1 = math.radians(lat1)
  lat2 = math.radians(lat2)
  # haversine formula
  dlong = long2 - long1
  dlat = lat2 - lat1
  a = math.sin(dlat / 2) ** 2
  b = math.cos(lat1) * math.cos(lat2) * math.sin(dlong / 2) ** 2
  c = 2 * math.asin(math.sqrt(a + b))
  dist = 6367 * c
  return dist


def haversine_distance_between_handles(handledict1, handledict2):
  return haversine_distance(handledict1['geographic']['longitude'], handledict1['geographic']['latitude'],
                            handledict2['geographic']['longitude'], handledict2['geographic']['latitude'])


def get_handle_location(handle, loctype, database):
  if loctype == 'cities':
    return (database.handle_table[handle]['geographic']['city'], database.handle_table[handle]['geographic']['country_code'])
  if loctype == 'countries':
    return (database.handle_table[handle]['geographic']['country_code'],)
  raise UnknownLocationType(loctype)


def connect_to_db():
  """
  <Purpose>
    Connect to the MySQL database using the user/pass/db specified in the 
    configuration file.
  <Arguments>
    configuration - Configuration dictionary from load_config_with_file().
  <Exceptions>
    None
  <Side Effects>
    Connects to the specified db.
  <Return>
    A db and cursor object representing the connection.
  """
  
  db = MySQLdb.connect(
      host='localhost', port=3306, 
      user=settings.dbusername, passwd=settings.dbpassword, 
      db=settings.dbname)
  cursor = db.cursor()
  return db, cursor


initialize()