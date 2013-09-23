"""
<Program Name>
  selexorweb.py

<Started>
  July 7, 2012

<Author>
  leon.wlaw@gmail.com
  Leonard Law

<Purpose>
  Implements a server for SeleXor that allows users to access SeleXor through a
  web interface.

<Usage>
  Simply start this program via the command line with these parameters:
    $ python selexorweb.py [instance_name]

  The instance name can be anything; this is not visible by the user. There
  should be a file named instance_name.conf that contains the configurations
  that are to be used by SeleXor. A default configuration file is included.


"""

import BaseHTTPServer
import SocketServer
import selexorserver
import selexorexceptions
import os
import sys
import threading
import seattleclearinghouse_xmlrpc   # Needed for checking auth. exceptions
import logging
import traceback
import selexorhelper
import ssl
import settings
import substitutions
# Raised when we cannot connect to the clearinghouse XMLRPC server
import xmlrpclib
import time

# We need to translate first, then import separately
# This is so that they do not overwrite python's open()
import repyhelper
# Used for serializing objects to comm. with clients
repyhelper.translate('serialize.repy')
# Used to read the nodestate transition key
repyhelper.translate('rsa.repy')

import serialize_repy
import rsa_repy



# This is a fix for slow response times for python's base http server.
# See: http://bugs.python.org/issue6085
def _bare_address_string(self):
    host, port = self.client_address[:2]
    return str(host)
BaseHTTPServer.BaseHTTPRequestHandler.address_string = _bare_address_string
# End slow respond time fix for python's base http server.


TEMPLATE_INDEX_FN = 'web_ui_template.html'
INDEX_FN = 'index.html'
WEB_PATH = './web/'




def main():
  global logger
  # Needed so that the event handler for the HTTP server can see the selexor server
  global selexor_server
  logger = selexorhelper.setup_logging("selexorweb")

  # Generate the index file
  _generate_request_form()

  http_server = SelexorHTTPServer((settings.http_ip_addr, settings.http_port), SelexorHandler)
  http_thread = threading.Thread(target=http_server.serve_forever)
  nodestate_transition_key = rsa_repy.rsa_file_to_publickey(settings.path_to_nodestate_transition_key)

  selexor_server = selexorserver.SelexorServer()


  http_thread.start()
  print "Listening for connections on", settings.http_ip_addr + ':' + str(settings.http_port)

  # Run indefinitely until CTRL+C is pressed.
  try:
    while True:
      time.sleep(1.0)
  except KeyboardInterrupt, e:
    pass

  print "Stopping web server..."
  http_server.shutdown()
  print "Stopping SeleXor server..."
  selexor_server.shutdown()
  print "Shutdown Complete."




# Browsers now perform pre-connections.  This means that they will spawn multiple connections
# to the web server in order for pages to load faster.  This is bad for us because the 
# HTTP server is single-threaded by default.  If we happen to handle one of the preconnect connections
# and don't receive any data from it (browser is sending request information on another connection)
# we end up blocking until the preconnect connection times out.  We use the ThreadingMixIn to handle
# all of these connections simultaneously, so that the browser can actually react to the initial page
# request and send more requests along the preconnects.
class SelexorHTTPServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):
  def __init__(self, address_tuple, handler_class):
    BaseHTTPServer.HTTPServer.__init__(self, address_tuple, handler_class)
    if settings.enable_https:
      # Enable SSL support
      self.socket = ssl.wrap_socket(
              self.socket,
              certfile=settings.path_to_ssl_certificate,
              keyfile=settings.path_to_ssl_key,
              server_side=True)



def _generate_request_form():
  '''
  <Purpose>
    Takes the web index file and replaces every instance of [[ VAR_NAME ]] with its
    substitution, found within config. The outputted file will be index.html.
  <Parameters>
    config: A dictionary obtained from _modify_config_with_file().
  <Exceptions>
    IOError
  <Side Effects>
    Generates an index file for this instance of selexor and places it in the current directory.
  <Return>
    None

  '''

  # This is the source file to parse
  srcfile = open(os.path.normpath(WEB_PATH + TEMPLATE_INDEX_FN), 'r')
  # This is the file that will contain the outputted data.
  destfile = open(os.path.normpath(WEB_PATH + INDEX_FN), 'w')

  data = srcfile.readline()
  lineno = 0
  while data:
    while '}}' in data:
      # For every '{{ x }}' pair, check what x is and replace it with the
      # corresponding values.
      before, remainder = data.split('{{', 1)
      token, remainder = remainder.split('}}', 1)
      token = token.strip()

      # We have no way of determining ahead of time which substitutions will
      # be defined without having to list them here...  If it isn't defined,
      # then let's just allow the exception to terminate the program.
      replacement = getattr(substitutions, token)
      result = before + replacement
      destfile.write(result)

      # Check the remainder
      data = remainder

    # Write anything left over
    destfile.write(data)
    data = srcfile.readline()
    lineno += 1

  srcfile.close()
  destfile.close()



class SelexorHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  '''
  <Purpose>
    Selexor handler for use with the BaseHTTPServer class.
  <Side Effects>
    Will serve requests pointing to files in the WEB_PATH directory.
    Also, will communicate with the selexorserver to perform user authentication
    and host requests.
  <Example Use>
    http_server = BaseHTTPServer.HTTPServer(IP, PORT), SelexorHandler)
  '''

  def do_GET(self):
    ''' Serves files that are needed for the SeleXor web client. '''
    # Check what the client is requesting
    if self.path[1:]:
      # Requesting specific file
      filepath = self.path[1:]
    else:
      # Requesting index
      filepath = INDEX_FN
    filepath = WEB_PATH + filepath

    # Write the header
    dataFile = None
    try:
      # Image files are binary, reading them in line mode causes corruption
      dataFile = open(filepath, 'rb')

      # How long is this file?
      dataFile.seek(0, 2)
      data_length = dataFile.tell()
      dataFile.seek(0, 0)

      # Set up webpage headers
      self.send_response(200)
      self.send_header("Content-type", self._get_mime_type_from_path(filepath))
      self.send_header("Content-Length", str(data_length))
      self.send_header("Connection", "Close")

    except IOError, e:
      # Cannot find file
      logger.error(str(e))
      # We can't find the file, send HTTP 404 NOT FOUND error message
      self.send_response(404)
    finally:
      self.end_headers()

    # Writing to self.wfile MUST occur after ending the headers.
    if dataFile:
      # Put the file's contents to the write buffer
      # Read file in increments to avoid memory overflow
      chunksize = 1000
      data = dataFile.read(chunksize)
      while data:
        self.wfile.write(data)
        data = dataFile.read(chunksize)
      dataFile.close()


  def do_POST(self):
    '''
    <Purpose>
      Responds to POST messages and handles them accordingly. Expects data in
      the JSON format.
    <Arguments>
      None
    <Exception>
      Exceptions thrown by _parse_post_data().
    <Side Effects>
      Calls a handler depending on the type of message received.
    <Returns>
      None
    '''
    # The handlers should take the following parameters:
    # data: The data expected by the handler.
    # remoteip: The IP address of the remote machine.
    #
    # Whatever that is returned by the handler will be put into the 'data'
    # key of the response dictionary.

    self.action_handlers = {
      'check_available': self._check_available_vessels,
      'request': self._handle_host_request,
      'query': self._handle_status_query,
      'release': self._release_vessel,
    }

    remoteip = self.client_address[0]
    # Only read the amount of data that is specified.
    rawdata = self.rfile.read(int(self.headers.getheader("Content-Length")))
    response = {}
    try:
      postdict = serialize_repy.serialize_deserializedata(rawdata)
      action = postdict.keys()[0]
      response['action'] = action + "_response"
      if action in self.action_handlers:
        data_to_send = self.action_handlers[action](postdict[action], remoteip)
      else:
        raise selexorexceptions.SelexorInvalidRequest("Unknown Action: " + action)
      response['status'] = 'ok'
      response['data'] = data_to_send
      output = serialize_repy.serialize_serializedata(response)
    except:
      # Catch all exceptions/errors that happen and log them.
      # Then tell the user an internal error occurred.
      logger.error("Unknown error occurred while serving request.\n" + traceback.format_exc())
      errstr = "An internal error occurred."
      data_to_send = None
      response['status'] = 'error'
      response['error'] = errstr

    # Send HTTP 200 OK message since this is a good request
    self.send_response(200)
    self.send_header("Content-Length", str(len(output)))
    self.send_header("Connection", "Close")
    self.end_headers()

    self.wfile.write(output)


  def _check_available_vessels(self, data, remoteip):
    '''
    Connects to the clearinghouse and returns a response_dict containing
    the following keys:
      'status':
        'ok' on success /'error' on failure
      'max_hosts':
        The remaining number of hosts that the user can acquire.
        On failure, this is '?'.
      'default_port':
        The user's assigned userport.
      'error':
        A description of the error that occurred. This only exists if an error
        happened.

    '''
    response_dict = {}
    try:
      client = selexorhelper.connect_to_clearinghouse(data)
      accinfo = client.get_account_info()
      acquired_resources = client.get_resource_info()

      response_dict['status'] = 'ok'
      response_dict['max_hosts'] = accinfo['max_vessels'] - len(acquired_resources)
      response_dict['default_port'] = accinfo['user_port']

    except seattleclearinghouse_xmlrpc.AuthenticationError, e:
      response_dict['status'] = 'error'
      response_dict['error'] = str(e)
      response_dict['max_hosts'] = "?"
    except xmlrpclib.ProtocolError, e:
      response_dict['status'] = 'error'
      response_dict['error'] = "SeleXor could not connect to the clearinghouse's XMLRPC server at this moment.  Please try again later."
      response_dict['max_hosts'] = "?"
    except Exception, e:
      logger.error("Unknown error while connecting to the XMLRPC server.\n"+traceback.format_exc())
      response_dict['status'] = 'error'
      response_dict['error'] = "An internal server error occurred."
      response_dict['max_hosts'] = "?"
    return response_dict


  def _handle_host_request(self, data, remoteip):
    ''' Wrapper for selexor server's host request function. '''
    return selexor_server.handle_request(data['userdata'], data['groups'], remoteip)


  def _handle_status_query(self, data, remoteip):
    ''' Wrapper for selexor server's status query function. '''
    return selexor_server.get_request_status(data['userdata'], remoteip)


  def _release_vessel(self, data, remoteip):
    ''' Wrapper for selexor server's vessel release function.'''
    return selexor_server.release_vessels(data['userdata'], data['vessels'], remoteip)


  def _get_mime_type_from_path(self, path):
    '''
    Returns the MIME type for a file with the specified path.
    Returns text/plain if the MIME type cannot be determined.

    '''
    if path.endswith(".png"):
      return 'image/png'
    if path.endswith('.gif'):
      return 'image/gif'
    if path.endswith('.ico'):
      return 'image/vnd.microsoft.icon'
    if path.endswith('.html'):
      return 'application/xhtml+xml; charset=utf-8'
    if path.endswith('.css'):
      return 'text/css; charset=utf-8'
    if path.endswith('.js'):
      return 'text/javascript; charset=utf-8'
    return 'text/plain; charset=utf-8'


if __name__ == "__main__":
  main()
