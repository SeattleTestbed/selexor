"""
<Module Name>
  settings.py

<Purpose>
  This is selexor's configuration file.  Modify the contents of this file to
  alter how selexor behaves.

"""

# The IP address to listen on.
# Use 0.0.0.0 to listen on all interfaces.
http_ip_addr = '0.0.0.0'

# The port that we should listen for incoming connections.
# Listens on the HTTPS port by default.
http_port = 443


"""
HTTPS Configuration
"""
# If HTTPS is set, you must have M2Crypto installed.  In addition, you must have the paths
# to the SSL certificate and keys configured correctly.
enable_https = True

# The certificate file that is used with SSL.
# This file should be in a PEM format.
# This can be generated through OpenSSL for testing, but a valid certificate
# provided by a trusted certificate provider must be used for production.
path_to_ssl_certificate = 'server.crt'

# The key file that is used with SSL.  This key must be the one that is paired
# with the certificate provided above.
path_to_ssl_key = 'server.key'

# The root to the local Seattle SVN repository.
path_to_seattle_trunk = '/path/to/seattle/trunk'

# The number of threads to use to probe
# Set this to 1 to disable threading.
num_probe_threads = 4

# The path to the file that contains the nodestate transition key.
# The key specified must be the nodestate transition key for the same
# clearinghouse specified at clearinghouse_xmlrpc_url.
# Example:
# path_to_nodestate_transition_key = '/home/selexor/public/seattle_nodestatetransition.key'
path_to_nodestate_transition_key = "seattle_nodestatetransition.key"

# The XMLRPC server for the Clearinghouse.  Leave at None to use the default.
# Example:
# clearinghouse_xmlrpc_url = "https://seattleclearinghouse.poly.edu/xmlrpc/"
clearinghouse_xmlrpc_url = None

# The GeoIP XMLRPC server to use.
# Set to None to use the default.
# geoip_server_url = http://geoip.cs.washington.edu:12679
geoip_server_url = None

# Sets the use of SSL in insecure mode.
allow_ssl_insecure = False

# The time to wait after a probe before probing again, in seconds.
# Default is 10 minutes.
probe_delay = 10 * 60

# If set to True, the node type will be refreshed every time a node is
# seen, regardless of if it is needed or not.  Otherwise, only refresh
# the node type when the node's IP address changes.
# This is useful when changing the way node types are determined.
force_refresh_node_type = False

"""
Database Configurations

This section defines which database and what users selexor is allowed to
connect as.
"""

# This is the name of the database that we should connect to.
# Make sure to set it to the database that you created for selexor.
dbname = 'selexordb'

# dbuser is the account that will be used for creating the initial tables
# on the database, as well   This account has all privileges.
dbusername = 'selexor'

# Password that will be used to authenticate with mysql
dbpassword = 'selexorpass'
