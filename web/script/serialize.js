/*
<Filename>
  serialize_repy.js
<Author>
  leon.wlaw@gmail.com
  Leonard Law
<Start Date>
  August 5th, 2012
<Purpose>
  Serialization module for RePy, ported to javascript.
  
  
*/

function repy_serialize(object) {
  // Null
  if (object === null)
    return 'N'
    
  switch(typeof(object)) {
  // Boolean
  case typeof(true):
    if (object == true)
      return 'BT'
    return 'BF'
    
  // Numbers
  // Javascript doesn't differentiate between ints and floats.
  case typeof(0.0):
    return 'F' + object
    
  // String
  case typeof(''):
    return 'S' + object
  
  default:
    switch(object.constructor) {
    // Array
    // JS doesn't differentiate between list/set/tuple/frozenset
    case Array:
      var mystr = 'L'
      for (var itemkey in object) {
        var itemstr = repy_serialize(object[itemkey])
        // Append the length of the item, plus ':', plus the item.
        // e.g. 1 -> 2:I1
        mystr += itemstr.length + ':' + itemstr
      }
      return mystr + '0:'
      
    // Dictionary
    case Object:
      var mystr = 'D'
      var keysstr = repy_serialize(getkeys(object))
      // Append the length of the list, plus ':', plus the list.
      mystr = mystr + keysstr.length + ':' + keysstr
      // Add the remaining values in the end.
      mystr += repy_serialize(getvalues(object))
      return mystr
      
    default:
      throw "Unknown type '"+typeof(object)+"' for data :"+object.toString()
    }
  }
}


function repy_deserialize(datastr) {
  
  if (typeof(datastr) != typeof(''))
    throw "Cannot deserialize non-string of type '" + String(typeof(string)) + "'"
  var typeindicator = datastr[0]
  var restofstring = datastr.substr(1)
  // None
  switch(typeindicator) {
  case 'N':
    if (restofstring.length)
      throw "Malformed None string '" + restofstring + "'"
    return null
  // Boolean
  case 'B':
    switch(restofstring) {
    case 'T':
      return true
    case 'F':
      return false
    default:
      throw "Malformed Boolean string '" + restofstring + "'"
    }
  // Integers
  case 'I':
    var retobject = parseInt(restofstring)
    if (retobject === NaN)
      throw "Malformed Integer string '"+restofstring+"'"
    return retobject
  // Floats
  case 'F':
    var retobject = parseFloat(restofstring)
    if (retobject === NaN)
      throw "Malformed Float string '"+restofstring+"'"
    return retobject
  // Strings
  case 'S':
    return restofstring
  // Arrays
  // Tuples
  // Sets
  // Frozensets
  case 'L':
  case 'T':
  case 's':
  case 'f':
    var thislist = Array()
    
    var data = restofstring
    // We'll use '0:' as our 'end separator'
    while (data != '0:') {
      // Extract the length from the string
      var lengthstr = String(data.split(':', 1))
      // var lengthstr = data.split(':', 1)  // There's a bug with this version, length is incorrect
      var length = parseInt(lengthstr)
      // Toss away lengthstr from the data string
      data = data.substring(lengthstr.length + 1)
      // get this item, convert to a string, append to the list.
      var thisitemdata = data.substring(0, length)
      var thisitem = repy_deserialize(thisitemdata)
      thislist.push(thisitem)

      // Now toss away the part we parsed.
      data = data.substring(length)
    }
    return thislist
    
  // Dictionary/Assoc. Arrays
  case 'D':    
    var keys_lengthstr = String(restofstring.split(':', 1))
    var keys_length = parseInt(keys_lengthstr)
    // offsets
    // +1 for 'D' type indicator
    // +1 for ':' data length separator
    restofstring = restofstring.substring(keys_lengthstr.length + 1)
    
    var keysstr = restofstring.substring(0, keys_length)
    var valuestr = restofstring.substring(keys_length)
    
    var keys = repy_deserialize(keysstr)
    var values = repy_deserialize(valuestr)
    
    if (keys.constructor != Array || values.constructor != Array || keys.length != values.length)
      throw ("Malformed Object string '"+restofstring+"'")
    
    var thisobject = {}
    for (var index in keys)
      thisobject[keys[index]] = values[index]
    
    return thisobject
  
  // Unknown type
  default:
    throw "InternalError: " + typeindicator + " is not a known type after checking"
  }
}

function getkeys(object) {
  var keys = []
  for (var key in object)
    keys.push(key)
  return keys
}

function getvalues(object) {
  var values = []
  for (var key in object)
    values.push(object[key])
  return values
}

function isArrayEqual(lhs, rhs) {
  return !(lhs < rhs || rhs < lhs)
}

function isObjectEqual(lhs, rhs) {
  if (!isArrayEqual(getkeys(lhs), getkeys(rhs)))
    return false
    
  for (var key in lhs) {
    if (lhs[key] !== rhs[key])
      return false
  }
  return true
}


function repy_serialization_test() {
  var things_to_test = [null, true, false, 0, 1, [0, 1, 2], {'str': 'key', 'int':1, 'float': 3.0, 'null':null}]
  var successful = true
  for (var key in things_to_test) {
    try {
      var thing = things_to_test[key]
      var serialized = repy_serialize(thing)
      var deserialized = repy_deserialize(serialized)
      
      var isEqual = deserialized === thing
      isEqual = isEqual || (thing.constructor === Array && isArrayEqual(thing, deserialized))
      isEqual = isEqual || (thing.constructor === Object && isObjectEqual(thing, deserialized))
      
      if (!isEqual)
        throw ("Incorrect deserialization value!" +
            "\nOriginal: "+thing+'('+typeof(thing)+')'+
            "\nSerialized: "+serialized+
            "\nDeserialized: "+deserialized)
    }
    catch(err) {
    successful = false
      console.log("Serialization failed: " + String(things_to_test[key]))
      console.log("Reason:" + err)
    }
  }
  return successful
}