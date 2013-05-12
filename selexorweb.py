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
from xmlrpclib import ProtocolError
from time import sleep

import repyhelper
# Used for serializing objects to comm. with clients
repyhelper.translate_and_import('serialize.repy')
# Used to read the nodestate transition key
repyhelper.translate_and_import('rsa.repy')   




# This is a fix for slow response times for python's base http server.
# See: http://bugs.python.org/issue6085
def _bare_address_string(self):
    host, port = self.client_address[:2]
    return str(host)
BaseHTTPServer.BaseHTTPRequestHandler.address_string = _bare_address_string
# End slow respond time fix for python's base http server.


context = {}
context['INDEX_FILE'] = 'web_ui_template.html'
context['WEB_PATH'] = './web/'

def main():
  global logger
  if len(sys.argv) != 2:
    print "Unexpected arguments!"
    print 'Usage: $ python selexorweb.py [instance name]'
    return

  logger = selexorhelper.setup_logging("selexorweb")

  # Generate the index file
  _generate_request_form()
  
  http_server = SelexorHTTPServer((settings.http_ip_addr, settings.http_port), SelexorHandler)
  http_thread = threading.Thread(target=http_server.serve_forever)
  nodestate_transition_key = rsa_file_to_publickey(settings.path_to_nodestate_transition_key)
  
  context['selexor_server'] = selexorserver.SelexorServer()
  

  http_thread.start()
  print "Listening for connections on", settings.http_ip_addr + ':' + str(settings.http_port)

  # Run indefinitely until CTRL+C is pressed.
  try:
    while True:
      sleep(1.0)
  except KeyboardInterrupt, e:
    pass

  print "Stopping web server..."
  http_server.shutdown()
  print "Stopping SeleXor server..."
  selexor_server.shutdown()
  print "Shutdown Complete."




class SelexorHTTPServer(BaseHTTPServer.HTTPServer):
  def __init__(self, address_tuple, handler_class):
    BaseHTTPServer.HTTPServer.__init__(self, address_tuple, handler_class)
    if settings.enable_ssl:
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
    Changes context['INDEX_FILE'] to point to the generated file.
  <Return>
    None

  '''
  outputfn = "index.html"

  # This is the source file to parse
  srcfile = open(os.path.normpath(context['WEB_PATH'] + 'web_ui_template.html'), 'r')
  # This is the file that will contain the outputted data.
  destfile = open(os.path.normpath(context['WEB_PATH'] + outputfn), 'w')

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
  context['INDEX_FILE'] = outputfn


class SelexorHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  '''
  <Purpose>
    Selexor handler for use with the BaseHTTPServer class.
  <Side Effects>
    Will serve requests pointing to files in the context['web_path'] directory.
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
      filepath = context['INDEX_FILE']
    filepath = context['WEB_PATH'] + filepath
    
    # Write the header
    dataFile = None
    try:
      # Image files are binary, reading them in line mode causes corruption
      dataFile = open(filepath, 'rb')
      # Set up webpage headers
      self.send_response(200)
      self.send_header("Content-type", self._get_mime_type_from_path(filepath))
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
    self.wfile.close()


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
      postdict = serialize_deserializedata(rawdata)
      action = postdict.keys()[0]
      response['action'] = action + "_response"
      if action in self.action_handlers:
        data_to_send = self.action_handlers[action](postdict[action], remoteip)
      else:
        raise selexorexceptions.SelexorInvalidRequest("Unknown Action: " + action)
      response['status'] = 'ok'
      response['data'] = data_to_send
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
    self.end_headers()

    output = serialize_serializedata(response)
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
    except ProtocolError, e:
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
    return context['selexor_server'].handle_request(data['userdata'], data['groups'], data['port'], remoteip)


  def _handle_status_query(self, data, remoteip):
    ''' Wrapper for selexor server's status query function. '''
    return context['selexor_server'].get_request_status(data['userdata'], remoteip)


  def _release_vessel(self, data, remoteip):
    ''' Wrapper for selexor server's vessel release function.'''
    return context['selexor_server'].release_vessels(data['userdata'], data['vessels'], remoteip)


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