"""
<Module Name>
  selexorexceptions.py

<Started>
  August 8, 2012

<Author>
  leon.wlaw@gmail.com
  Leonard Law

<Purpose>
  Defines all exceptions that are raised by SeleXor components.

"""

class SelexorException(Exception):
  """ Base class for all Selexor exceptions. """

class SelexorInvalidOperation(SelexorException):
  """ Attempted to perform an operation that is not supported. """

class SelexorAuthenticationFailed(SelexorException):
  """ An authentication attempt failed. """
  def __init__(self, errstring = ""):
    self.errstring = str(errstring)
  def __str__(self):
    return "Authentication failed: " + errstring

class SelexorInvalidRequest(SelexorException):
  """ An invalid request was passed in. """

class SelexorInternalError(SelexorException):
  """ An internal error in SeleXor occurred. """



class RuleException(Exception):
  ''' Base exception describing an invalid rule action. '''


class UnknownRule(RuleException):
  '''
  An unknown rule was specified.
  To see a list of known rules, use the list_all_rules() function.

  '''

class UnknownRuleType(RuleException):
  '''
  Attempted to register a rule to an unknown rule type.
  See _RULE_TYPES for a list of valid rules.

  '''

class MissingParameter(RuleException):
  '''  '''
  def __init__(self, paramname = ""):
    self.paramname = str(paramname)
  def __str__(self):
    return "Missing Parameter: " + paramname


class BadParameter(RuleException):
  ''' The parameter passed in is not what was expected. '''

class InvalidRuleReregistration(RuleException):
  '''
  Attempted to register a rule callback to a rule that was already defined.
  Unregister a rule before replacing it.

  '''

class UnknownLocation(Exception):
  """ Unknown location """
  def __init__(self, location = ""):
    self.location = location
  def __str__(self):
    return "Unknown location: " + self.location