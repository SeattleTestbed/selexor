/*
<Filename>
  selexor_script.js
<Purpose>
  Manages the client-side interface for SeleXor. It allows the user to select

  groupdict:
    An associative array representing a group request. It should contain the
    following keys:

    'id': groupid
    'allocate': The number of vessels to acquire
    'acquired': A list of acquired vesseldicts.
    'status': The current status of the group.
    'error': The error string for the group. This is only present if 'status'
             is 'error'.

  rulestring:
    A string representing a set of rules for a group. They are in the following
    format:
        rule_name:param1~value1:param2~value2 ...

*/

var DEBUGGING = false;

var CONTENT_TYPES = ["Get Vessels", "Include Other Groups"]
var CONTENT_TOOLTIPS = {
  "Get Vessels": 'The number of vessels to obtain.',
  "Include Other Groups": "Groups to apply this condition to.<br /><select><option>N/A</option></select> indicates that there is no target.<br />Press the <input type='button' value='+' /> button to add more targets.<br />Press the <input type='button' value='-' /> button to remove the last target. Does nothing if there is only one target left."
}
var IMPLEMENTED_RULES = [
  "None",
  "location_specific",
  "location_different",
  "location_separation_radius",
  'node_type',
  ]

var RULE_DEF = {
  "None": {
    'text':"of any type",
    "parameters":[
      {'type': 'none', 'tooltip': "This option allows any kind of vessel to be taken, even if <select><option>are not</option></select> is chosen."}
    ]},
  "average_latency": {
    'text':"with an average latency",
    'parameters':[
      {'type': 'num_range', 'names': ['min_latency', 'max_latency'], 'placeholders': [0, Infinity],'default_values': [0, Infinity]}
    ]},
  "uptime": {
    'text':"with an uptime",
    'parameters':[
      {'type': 'num_range', 'names': ['min_uptime', 'max_uptime'], 'placeholders': [0, 100], 'default_values': [0, 100], 'tooltip': 'Percentage uptime.<br />You can specify this in terms of percentages (0% ~ 100%), or as a number (0.0 ~ 1.0).'}
    ]},
  "num_ip_change": {
    'text':"with number of IP address changes",
    'parameters':  [
      {'type': 'num_range', 'names': ['min_change', 'max_change'], 'placeholders': [0, Infinity],'default_values': [0, Infinity],'tooltip': 'How often vessels change IP addresses.'}
    ]},
  "location_specific": {
    'text':"from a specific location",
    'parameters':[
      {'type': 'text', 'names': ['city'], 'placeholders': ["City (optional)"],'default_values': ["?"]},
      {'type': 'text', 'names': ['country'], 'placeholders': ["US"], 'default_values': ["US"], 'tooltip': "The country identifier can either be 2-/3-letter ISO-3166 codes, or the country name."}
    ]},
  "location_different": {
    'text': 'from different locations',
    'parameters':[
      {'type': 'num_text','names': ['location_count'],'placeholders': [Infinity],'default_values': [Infinity],'tooltip': "The number of different locations to search from.<br />If this is set to a value larger than the number of vessels to acquire, then all vessels will be from different locations."},
      {'type': 'select', 'names': ['location_type'], /*'default_values': null, */ 'param_values': ['city', 'country_code'], 'param_valuelabels':['cities', 'countries']}
    ]},
  "location_separation_radius": {
    'text': 'with a separation radius of',
    'parameters':[
      {'type': 'num_range', 'names': ['min_radius', 'max_radius'], 'placeholders': [0, Infinity], 'default_values': [0, Infinity], 'tooltip': "The distance from every vessel to every other vessel must be within the specified range.<br />Valid units: m, km (default), mi"}
    ]},
  "node_type": {
    'text': 'running on',
    'parameters':[
      {
        'type': 'select',
        'names': ['node_type'],
        /*'default_values': null, */ 
        'param_values': ['testbed', 'university', 'home', 'unknown'],
        'param_valuelabels': ['testbeds', 'universities', 'homes', 'unknown']
      }],
    }
}



// Links group IDs to the HTML-DOM row objects
var g_groupid_to_row;

// The maximum number of hosts that the user can acquire.
var g_max_hosts;
// The remaining number of hosts that the user can acquire.
var g_num_hosts_remaining;

var g_next_groupid;

var g_auth_errors = {}
var g_request_errors = {}

var SHOW_UNIMPLEMENTED = true;

var g_authenticated = false;
var g_server_status;

var SERVER_POLL_INTERVAL = 2000;
var g_server_status_poll_timer;

var g_vessels_acquired = {}

var PROGRESS_IMAGE_SRC = './images/progress.gif'

/*
<Purpose>
  Checks if the object is empty.
<Arguments>
  obj:
    The object to check.
<Side Effects>
  None
<Exceptions>
  None
<Returns>
  true if the object is empty, false otherwise.

*/
function isEmpty(obj) {
  for (var name in obj)
    if (obj.hasOwnProperty(name))
      return false;
  return true;
}

/*
<Purpose>
  Adds a tooltip to the specified object.
<Arguments>
  data: The contents of the tooltip.
  icon_name:
    The type of tooltip to display. This is either 'info' or 'error'.
<Side Effects>
  Adds a tooltip to the specified object.
<Exceptions>
  None
<Returns>
  None

*/
jQuery.fn.addTooltip = function(data, icon_name) {
  return this.each(function() {
    var image;
    var node;
    switch (this.tagName) {
    case "TR":
    case "TD":
    case "TH":
    case "H1":
    case "H2":
    case "DIV":
    case "SPAN":
      if (this.lastChild === null)
        this.appendChild(document.createTextNode(''))
      node = this.lastChild;
      if (node.className == 'tooltip_img')
        node = node.previousSibling;
      break;
    default:
      node = this;
      break;
    }
    if (node.nextSibling == null || node.nextSibling.className != "tooltip_img") {
      image = document.createElement("img");
      image.className = "tooltip_img";
      if (icon_name === undefined)
        icon_name = 'info'
      if (icon_name == 'error')
        image.src = './images/tooltip_error.png';
      else if (icon_name == 'info')
        image.src = "./images/tooltip_question.png";
      ($(image)).insertAfter($(node));
    } else {
      image = this.nextSibling;
    }
    $(image).tooltip({
      delay: 0,
      bodyHandler: function() {
        return data;
      },
      showURL: false
    });
  });
};

/*
<Purpose>
  Removes a tooltip from the specified object.
<Arguments>
  None
<Side Effects>
  Removes a tooltip from the specified object.
<Exceptions>
  None
<Returns>
  None

*/
jQuery.fn.removeTooltip = function() {
  return this.each(function() {
    switch (this.tagName) {
    case "TR":
    case "TD":
    case "TH":
    case "H1":
    case "H2":
    case "DIV":
    case "SPAN":
      if (this.lastChild === null)
        this.appendChild(document.createTextNode(''))
      node = this.lastChild;
      if (node.className == 'tooltip_img')
        node = node.previousSibling;
      break;
    default:
      node = this;
      break;
    }

    if (node != null && node.parentNode != null && node.nextSibling != null && $(node.nextSibling).hasClass("tooltip_img"))
      $(node.nextSibling).remove()
  });
};

/*
<Purpose>
  Adds a tooltip to the specified object to indicate an error.
<Arguments>
  data: The error message to display
<Side Effects>
  Adds a tooltip to the specified object.
  If the specified object is a text input field, it also changes the
  background-color to the displaycolor.
<Exceptions>
  None
<Returns>
  None

*/
jQuery.fn.show_error = function(data) {
  return this.each(function() {
    $(this).addTooltip(data, 'error')
           .addClass('has_error')
  });
};


/*
<Purpose>
  Puts floating nodes back into the DOM tree. This is to prevent them from
  displaying in an unexpected manner, cross browser.
<Arguments>
  None
<Side Effects>
  Adds a DIV object with style='clear:both' as a child to the selected
  DOM objects.
<Exceptions>
  None
<Returns>
  None

*/
jQuery.fn.clear_error = function(displaycolor) {
  if (displaycolor === undefined)
    displaycolor = "#fff"
  return this.each(function() {
    $(this).removeTooltip()
           .removeClass('has_error')
  });
};


/*
<Purpose>
  Initializes the state of the selexor client.
<Arguments>
  None
<Side Effects>
  Resets all global variables to their default state.
<Exceptions>
  None
<Returns>
  None

*/
var init = function() {
  // Load global variables
  g_groupid_to_row = {}

  // Reset everything to initial state
  g_next_groupid = 0;
  last_selected = 0;
  g_max_hosts = "?";
  g_num_hosts_remaining = g_max_hosts;

  update_remaining_host_count();

  if (location.hash) {
    extract_login_info_from_url_hash()
    authenticate()
  }

};


/*
<Purpose>
  Extracts user credentials from the browser's URL hash.
<Arguments>
  None
<Side Effects>
  Fills the username and API key fields with values provided 
  in the URL hash.
<Exceptions>
  None
<Returns>
  None

*/
var extract_login_info_from_url_hash = function() {
  var inputs = location.hash.substring(1).split('&')
  var authinfo = {}
  for (var i in inputs) {
    var key = inputs[i].split('=')[0]
    var value = inputs[i].split('=')[1]
    authinfo[key] = value
  }
  if ('username' in authinfo && 'apikey' in authinfo) {
    $('#username_text').val(authinfo['username'])
    $('#apikey_text').val(authinfo['apikey'])
  }
}


/*
<Purpose>
  Checks the selexor server's current status.
<Arguments>
  None
<Side Effects>
  Updates each group's status.
  Schedules another poll in SERVER_POLL_INTERVAL seconds.
<Exceptions>
  None
<Returns>
  The TD object that contains rule controls.

*/
var poll_server_status = function() {
  var requestinfo = {
    'userdata': get_user_data()
  }
  $.ajax({
    url:'', type:'POST', dataType: 'text',
    data: repy_serialize({'query': requestinfo}),
    beforeSend: function(jqXhr) {
      if (jqXhr && jqXhr.overrideMimeType)
        jqXhr.overrideMimeType("text/plain;charset=UTF-8");
      }
  }).done(function(rawdata, textStatus, jqXhr) {
    var response = repy_deserialize(rawdata.trim())
    var data = response['data']
    g_server_status = data['status']
    update_group_status_spans(data['groups'])
    if (g_server_status == 'working')
      g_server_status_poll = setTimeout(poll_server_status, SERVER_POLL_INTERVAL)
    else if (g_server_status == 'complete') {
      update_vessels_dict(data['groups'])
      $('.vessel_release').removeAttr('disabled')
    }
  }).fail(function(data, textStatus, jqXhr) {
    alert("An error occurred while contacting the server...")
  });
}


/*
<Purpose>
  Updates g_vessels_acquired to reflect what's inside groups.
<Arguments>
  groups:
    A requestdict that contains the current state of the request.
<Side Effects>
  Modifies g_vessels_acquired such that g_vessels_acquired contains dictionaries
  that map groupids to the list of acquired vessels in each group.
<Exceptions>
  None
<Returns>
  None

*/
function update_vessels_dict(groups) {
  g_vessels_acquired = {}
  for (var groupname in groups) {
    g_vessels_acquired[groupname] = []
    for (var vessel_num in groups[groupname]['vessels_acquired'])
      g_vessels_acquired[groupname].push(groups[groupname]['vessels_acquired'][vessel_num])
  }
}

/*
<Purpose>
  Given a vesseldict, extract the node handle.
<Arguments>
  vesseldict:
    A dictionary containing vessel information. Specifically, it should have
    the following keys:
    'node_ip': IP address
    'node_port': Clearinghouse port.
    'vesselname': The vessel's identifier on its node.
<Exceptions>
  None

*/
function get_nodehandle_from_vesseldict(vesseldict) {
  return  vesseldict['node_ip'] + ":" + vesseldict['node_port'] + ':' +
          vesseldict['vesselname']
}

/*
<Purpose>
  Updates the status that is displayed next to each group.
<Arguments>
  groups:
    A groupdict. See script documentation for more information.
<Side Effects>
  Updates the text/images that indicate the status of each group.
<Exceptions>
  None
<Returns>
  None
*/
function update_group_status_spans(groups) {
  for (var groupid in groups) {
    var row_id = 'group_' + groupid + '_status'
    var image_source = '';
    var notice = '';
    var error = null;
    var group = g_groupid_to_row[groupid];

    var resolved = groups[groupid]['vessels_acquired'] == groups[groupid]['target_num_vessels']

    // Display errors
    if (groups[groupid]['error'])
      error = groups[groupid]['error']

    // Set image src
    if (resolved) {
      image_source = ''
    } else {
      switch (g_server_status) {
      case 'accepted':
      case 'working':
        image_source = PROGRESS_IMAGE_SRC
        break
      }
    }

    // Set notice
    if (resolved) {
      switch(g_server_status) {
      case 'accepted':
      case 'working':
      case 'complete':
        notice = "Complete!"
        break
      }
    } else {
      switch(g_server_status) {
      case 'accepted':
      case 'working':
      case 'complete':
        notice = groups[groupid]['vessels_acquired'].length + " of " + groups[groupid]['target_num_vessels']
        break
      }
    }

    // Change + Show progress image
    var progress_image = $(group).find('.progress_image')
    if (progress_image.attr('src') != image_source && image_source != '')
      progress_image.attr('src', image_source)
    if (image_source == '')
      progress_image.hide()
    else
      progress_image.css('display', 'inline')

    // Show status text
    $(group).find('.progress_text').text(notice)

    // Show error if needed
    var error_node = $(group).find('.error_node')
    error_node.clear_error()
    if (error)
      error_node.show_error(error)

    // Show vessel release cell
    switch(g_server_status) {
    case 'complete':
      // Don't display remove button if there are no vessels
      if (groups[groupid]['vessels_acquired'].length)
        $('.vessel_release').css('display', 'table-cell')
      break
    }
  }
}


/*
<Purpose>
  Creates a table cell that will show the status for the group with the
  specified groupid.
<Arguments>
  groupid:
    The ID of the group that the created cell should be linked to.
<Side Effects>
  None
<Exceptions>
  None
<Returns>
  A TD object that shows the status for the specified groupid.

*/
var create_status_cell = function(groupid) {
  // This is the progress image to show, if needed
  var image = document.createElement('img')
  image.className = 'progress_image'

  // This is where status text will be
  var text = document.createElement('span')
  text.className = 'progress_text'

  // This is where the error tooltip will be anchored onto
  var error = document.createElement('span')
  error.className = 'error_node'

  // Put everything into a span
  // Do we really need to put this in a span?
  // Consider putting things directly into the TD cell.
  var span = document.createElement('span')
  span.className = 'group_status_span'
  span.id = 'group_' + groupid + '_status'
  $(span).append(image).append(text).append(error)

  // Put everything into a cell
  var cell = document.createElement('td')
  cell.appendChild(span)
  cell.className = "status_cell"
  return cell
}

/*
<Purpose>
  Updates the remaining host count notification text.
<Arguments>
  None
<Side Effects>
  Changes the host count notification text.
  Triggers the send request error checker to flag the request as
    unservicable if needed.
<Exceptions>
  None
<Returns>
  None

*/
var update_remaining_host_count = function() {
  $('#num_hosts_remaining').text(g_num_hosts_remaining);
  $('#num_hosts_remaining').blur()
};


/*
<Purpose>
  Gets the current snapshot of the user's authentication data.
<Arguments>
  None
<Side Effects>
  None
<Exceptions>
  None
<Returns>
  The userdict representing the current authentication data.

*/
function get_user_data() {
  var retdict = {}
  retdict[$('#username_text').val()] = {'apikey': $('#apikey_text').val()}
  return retdict
}


/*
<Purpose>
  Converts the current rule definitions into a format recognizable by SeleXor.
<Arguments>
  None
<Side Effects>
  None
<Exceptions>
  None
<Returns>
  A rulestring representing the current rule definition.

*/
// Needs clearer info
function convert_rules_to_string() {
  var string = '';
  var requestdict = {}
  $("tr.group_definition").each(function() {
    var id = $(this).data('id');
    var vesselcount = $(this).find('.vessel_selection').val()
    if (parseInt(vesselcount)) {
      var group_str = id + ":" + vesselcount
      var groupdict = {'id': id, 'allocate': vesselcount, 'rules': {}}

      // Is the port global rule set?
      if ($('#port_restriction').prop('checked')) {
        // Emulate the "global" behavior by inserting the rule to every group
        groupdict['rules']['port'] = {
          port: $('#port_restriction_option').val(),
        }
      }

      // Add the rule definitions
      $(this).find('.rule_span').each(function() {
        var rule_str = ""
        var invert = $(this).children('.invert_rule').val() == "true";
        var type = $(this).children('.condition_type').val();
        if (type != "None") {
          groupdict['rules'][type] = {}
          var parameter = '';
          if (invert)
            rule_str += "!"
          rule_str += type
          $(this).find('input, select')
                 .not('.invert_rule, .condition_type, .condition_count_modifier')
                 .each(function(){
            var name = $(this).data('name')
            var value = $(this).val()
            if (value == '')
              value = $(this).data('default_value')
            rule_str += ',' + name + '~' + value
            groupdict['rules'][type][name] = value
          });
          group_str += ":" + rule_str
        }
      })
      string += group_str + ';'
      requestdict[id] = groupdict;
      ++id
    }
  })
  return requestdict;
}


/*
<Purpose>
  Connects to the selexor server and gets the user's seattle state information.
<Arguments>
  None
<Side Effects>
  Checks all authentication parameters and flags errors.
  Disables authentication fields on successful authentication.
  Updates g_max_hosts and g_num_hosts_remaining.
  Resets hosts to allocate for all groups to 0.
  Changes the URL hash to reflect the current active session on successful login.
<Exceptions>
  None
<Returns>
  None

*/
function authenticate() {
  $(".auth_input").blur();
  if (!isEmpty(g_auth_errors)) {
    console.log("Errors found!")
    return;
  }
  var userinfo = get_user_data()

  $('#authentication_progress').css('display', 'inline')
  g_authenticated = false
  $.ajax({
    url: '', type: 'POST', dataType: 'text',
    data: repy_serialize({'check_available': userinfo}),
    beforeSend: function(jqXhr) {
      if (jqXhr && jqXhr.overrideMimeType)
        jqXhr.overrideMimeType("text/plain");
      }
  }).done(function(rawdata, textStatus, jqXhr) {
    var response = repy_deserialize(rawdata.trim())
    if ('error' in response) {
      $('#authentication_errtext').addTooltip(response['error'], 'error')
                                  .css('display', 'inline')
      $('#username_text, #apikey_text').removeAttr('disabled')
      return;
    }
    var data = response['data']
    if ('error' in data) {
      $('#authentication_errtext').addTooltip(data['error'], 'error')
                                  .css('display', 'inline')
      $('#username_text, #apikey_text').removeAttr('disabled')
    } else {
      // No errors!
      $('#authentication_errtext').removeTooltip().hide()
      if (response.action == 'check_available_response') {
        var data = response.data
        g_max_hosts = data.max_hosts

        $('select#port_restriction_option').val(data.default_port)

        if (!DEBUGGING) {
          $('#username_text, #apikey_text').attr('disabled', 1)
        }
        g_authenticated = true
        $('.authentication_required').show()

        // Update the URL hash so that the user can easily login after this
        location.hash = '#username='+$('#username_text').val()+'&apikey='+$('#apikey_text').val()
      }
    }
  }).fail(function(data, textStatus, jqXhr) {
    alert("Failed to connect to the server!")
  }).always(function() {
    if (g_max_hosts == undefined)
      g_max_hosts = '?'
    g_num_hosts_remaining = g_max_hosts;
    update_remaining_host_count();
    $('#authentication_progress').hide()
    $('.vessel_selection').val(0)
  });

}


/*
<Purpose>
  Sends the current request to the selexor server.
<Arguments>
  None
<Side Effects>
  Checks all authentication parameters and flags errors.
  Checks all request parameters and flags errors.
  Asks user for confirmation before sending the request.
  Connects to the server and sends the request.
  Starts a server poll.
  Disables controls that may change the state of the request
<Exceptions>
  None
<Returns>
  None

*/
var send_host_request = function() {
  $('request_input').blur();
  if (!g_authenticated) {
    alert("Please authenticate first!")
    return
  }

  if (!isEmpty(g_request_errors)) {
    console.log("Error with request!");
    return;
  }

  if (!confirm("Are you sure you want to request these vessels?"))
    return;

  var requestinfo = {}
  requestinfo['userdata'] = get_user_data()
  requestinfo['groups'] = convert_rules_to_string()
  $('.vessel_release').hide()
  $('.status_cell').css('display', 'table-cell')

  var progress_image = document.createElement('img')
  progress_image.src = PROGRESS_IMAGE_SRC
  $('#vessel_acquire_button').after(progress_image)

  $.ajax({
    url:'', type:'POST', dataType: 'text',
    data: repy_serialize({'request': requestinfo}),
    beforeSend: function(jqXhr) {
      g_server_status = ""
      if (jqXhr && jqXhr.overrideMimeType)
        jqXhr.overrideMimeType("text/plain;charset=UTF-8");
      $('#request_progress').css('display', 'inline')
    }
  }).done(function(rawdata, textStatus, jqXhr) {
    var response = repy_deserialize(rawdata.trim())
    if (response['status'] == 'error') {
      $('#vessel_acquire_errtext').text(response['error'])
      $('#acquire_capsule').addClass('error');
    } else {
      $('#vessel_acquire_errtext').text('')
      $('#acquire_capsule').removeClass('error');

      var data = response['data']
      update_group_status_spans(data['groups'])

      if (data['status'] == 'error') {
        update_group_status_spans(data['groups'])
        $('.vessel_release').removeAttr('disabled')
      } else {
        // Success!
        if (!DEBUGGING) {
          // Prevent user from making changes while code is running
          $('input, select').attr('disabled', 'disabled')
        }
        poll_server_status()
      }
    }
  }).fail(function(data, textStatus, jqXhr) {
    alert("Failed to connect to server!")
  }).always(function() {
    $(progress_image).hide()
  });
}



/*
<Purpose>
  Creates a drop-down menu.
<Arguments>
  option_values:
    The list of values that the options should have.
  option_labels:
    The list of option labels. This is optional.
    This list should have the same number of items as in option_values.
    If left undefined, this will be the same as option_values.
<Side Effects>
  Adds a text node containing the given text into the node.
<Exceptions>
  "The number of labels and values must be equal!"
<Returns>
  None

*/
var create_selection = function (option_values, option_labels, selector) {
  if (selector === undefined)
    var selector = $('<select>');
  else {
    // Wipe out the existing items within the select, otherwise we will have duplicates
    selector.children().remove()
  }
  if (option_labels === undefined)
    option_labels = option_values;
  if (option_labels.length != option_values.length)
    throw "The number of labels and values must be equal!"

  for (var i = 0; i < option_values.length; ++i) {
    var new_option = document.createElement("option");
    new_option.value = option_values[i];
    new_option.textContent = option_labels[i];
    selector.append(new_option);
  }
  return selector;
}


/*
<Purpose>
  Creates a cell to indicate what's contained within the group.
<Arguments>
  None
<Side Effects>
  None
<Exceptions>
  None
<Returns>
  A cell containing a drop-down for specifying how many vessels should be
  acquired.

*/
function create_content_cell() {
  // Drop-down for vessel count
  var selection = $('<select>')
    .addClass("vessel_selection")
    // Create the initial selection with 0 vessels
    .append($('<option>').val(0).text(0))
    .focus(function() {
      add_vessels_until_max(selection)
      $(selection).data('last_selected', this.selectedIndex)
    })
    .change(function() {
      var delta = parseInt($(this).val()) - $(selection).data('last_selected')
      g_num_hosts_remaining -= delta;
      $(selection).data('last_selected', this.selectedIndex)
      update_remaining_host_count();
    })
    .blur(function () {
      remove_targets_until_selected(selection);
    })

  var cell = $('<td>')
        .append("Get", selection, "vessels that")
  return cell;
}


/*
<Purpose>
  Creates a SPAN object that contains controls to allow users to define a rule.
  Each rulespan should have the following:
    Negation dropdown
    Rule type dropdown
    Parameter list span
    New rule button
    Remove this rule button
<Arguments>
  groupid:
    The groupid to associate with this rule span.
<Side Effects>
  None
<Exceptions>
  None
<Returns>
  Returns the SPAN object created.

*/
function create_rule_span(groupid) {
  // Allow the user to specify NOT
  var condition_negation_select = create_selection([false, true], ["are", "are not"])
          .addClass('invert_rule')

  var span = $('<span>')
        .addClass('rule_span')
        .append(condition_negation_select)

  var condition_ids = IMPLEMENTED_RULES
  // The descriptions for each rule.
  // This is shown to the user in the rule dropdown.
  var condition_texts = []
  for (var condition_index in condition_ids) {
    condition_texts.push(RULE_DEF[condition_ids[condition_index]]['text'])
  }

  var condition_selection = create_selection(condition_ids, condition_texts)
          .addClass('condition_type')
          .append(condition_selection);

  var condition_parameters = $("<span>")
          .addClass('condition_parameters')

  condition_selection.change(function() {
    update_condition_parameters(condition_selection.val(), condition_parameters)
  })

  // Button to add a new rule span after this
  var add_condition_button = $('<button />')
        .text('and ...')
        .click(function() {
            $(span).after(create_rule_span(groupid));
        })

  // Button to remove this rule span
  var remove_condition_button = $("<button />")
        .text('X')
        .click(function() {
            if (span.siblings().length == 0) {
              remove_group(groupid);
            }
            span.remove()
        })

  $([add_condition_button, remove_condition_button])
      .addClass('condition_count_modifier')
      .addClass('float_right');

  span.append(condition_selection)
      .append(condition_parameters)
      .append(add_condition_button)
      .append(remove_condition_button)
  condition_selection.change();

  return span;
}


/*
<Purpose>
  Modify rule_parameter_span to show the parameters that are relevant
  to the condition.
<Arguments>
  condition:
    A string indicating
<Side Effects>
  None
<Exceptions>
  None
<Returns>
  The userdict representing the current authentication data.

*/
function update_condition_parameters(condition, rule_parameter_span) {
  rule_parameter_span.empty()
  if (RULE_DEF[condition]) {
    for (var modifiername in RULE_DEF[condition]['parameters']) {
      var modifier = RULE_DEF[condition]['parameters'][modifiername]
      switch (modifier.type) {
      case "num_range":
        var min_input = $("<input/>")
              .prop('placeholder', modifier.placeholders[0])
              .data('name', modifier.names[0])
              .data('default_value', modifier.default_values[0])

        var max_input = $("<input/>")
              .prop('placeholder', modifier.placeholders[1])
              .data('name', modifier.names[1])
              .data('default_value', modifier.default_values[1])
        
        // Operations common to both min and max inputs
        $([min_input, max_input]).each(function() {
            $(this).addClass('numeric_text')
                  .numeric({'negative':false})
                  .data("name", modifier.name)
          })

        rule_parameter_span.append("between", min_input, "and", max_input);
        break;

      case "text":
      case "num_text":
        var input = $('<input />')
              .prop('placeholder', modifier.placeholders[0])
              .data('name', modifier.names[0])
              .data('default_value', modifier.default_values[0])
        if (modifier.type == 'num_text') {
          input.numeric({'negative': false})
              .addClass('numeric_text')
        }
        rule_parameter_span.append(input);
        break;

      case "select":
        var selection = create_selection(modifier.param_values, modifier.param_valuelabels);

        // Select the default value
        $(selection).data('name', modifier.names[0])
            .val(modifier.default_values);

        rule_parameter_span.append(selection);
      }

      // Add a tooltip if it is specified in the rule definition
      if (modifier.tooltip) {
        rule_parameter_span.addTooltip(modifier.tooltip, "info");
      } else {
        rule_parameter_span.removeTooltip();
      }
    }
  }
}


/*
<Purpose>
  Creates a cell containing controls to remove a group.
<Arguments>
  groupid:
    The groupid of the group to remove.
<Side Effects>
  The created cell will have a button that, when clicked, removes the group
  with the specified groupid.
<Exceptions>
  None
<Returns>
  The TD object that contains removal controls.

*/
function create_release_cell(groupid) {
  var release_cell = document.createElement('td')
  var release_button = document.createElement('input')
  release_button.type = 'button'
  release_button.value = 'Release'
  release_button.className = 'vessel_release'
  release_cell.className = 'vessel_release'

  var progress_img = document.createElement('img')
  progress_img.src = PROGRESS_IMAGE_SRC
  $(progress_img).addClass('release_progress_image').hide()

  release_cell.appendChild(release_button)
  release_cell.appendChild(progress_img)

  release_button.onclick = function() {
    release_group(String(groupid));
  }
  return release_cell
}



/*
<Purpose>
  Removes all siblings after the HTML-DOM node.
<Arguments>
  node:
    A DOM element that has a parent.
<Side Effects>
  Removes all siblings that follow the child node.
<Exceptions>
  None
<Returns>
  The userdict representing the current authentication data.

*/
function remove_following_siblings(node) {
  if (node.parentNode)
    while (node.nextSibling)
      node.parentNode.removeChild(node.nextSibling);
}


/*
<Purpose>
  Creates a new rule row.
<Arguments>
  None
<Side Effects>
  Increases g_next_groupid
<Exceptions>
  None
<Returns>
  The userdict representing the current authentication data.

*/
var create_new_group = function() {
  var my_id = g_next_groupid;
  var row = $('<tr>')
        .addClass("group_definition")

  // Add contents cell
  var contents_cell = create_content_cell();
  
  var rules_cell = $('<td/>').append(create_rule_span(my_id))

  // Status cell
  var status_cell = create_status_cell(my_id)

  // Add release cell
  var release_cell = create_release_cell(my_id)

  $(release_cell).hide()
  $(status_cell).hide()

  // Construct the row
  row.append(contents_cell, rules_cell, status_cell, release_cell);

  // Insert the group row at the end of the table
  $('#group_table').append($(row).data('id', my_id))
  // Map the groupid to this row
  g_groupid_to_row[my_id] = row
  // add_row_to_group_table(my_id, row);
  ++g_next_groupid;
}

/*
<Purpose>
  Releases the group with the specified groupid.
<Arguments>
  groupid: The groupid of the group to release.
<Side Effects>
  Connects to the selexor server and releases the vessels inside the group with
  the specified groupid.
  Updates g_num_hosts_remaining
  Resets the vesselcount to acquire for the released group
  Hides the status cell and vessel release cell for the released group
<Exceptions>
  None
<Returns>
  None

*/
function release_group(groupid) {
  release_data = {
    'userdata': get_user_data(),
    'vessels': g_vessels_acquired[groupid]
  }
  var row = $(g_groupid_to_row[groupid])
  // Show progress image
  row.find('.release_progress_image').css('display', 'inline')
  $.ajax({
    url: '', type: 'POST', dataType: 'text',
    data: repy_serialize({'release': release_data}),
    beforeSend: function(jqXhr) {
      if (jqXhr && jqXhr.overrideMimeType)
        jqXhr.overrideMimeType("text/plain;charset=UTF-8");
      }
  }).done(function(rawdata, textStatus, jqXhr) {
    var response = repy_deserialize(rawdata.trim())
    var data = response['data']
    row.find('.vessel_selection').val(0);
    // No errors occur
    var error = ''
    if (response['status'] == 'error')
      error = response['error']
    else {
      if (data[0]) {
        g_num_hosts_remaining += data[1]
  
        update_remaining_host_count()
        row.find('.vessel_release, .status_cell').hide();
        // We don't need to keep track of these vessels anymore
        delete g_vessels_acquired[groupid]
      } else
        error = data[1]
      } 
    // There was an error
    if (error != ''){
      row.find('.status_cell').text(error).css('display', 'table-cell')
    }
  }).fail(function(data, textStatus, jqXhr) {
    alert("Failed to connect to server!")
  }).complete(function(data, textStatus, jqXhr) {
    row.find('.release_progress_image').hide()
  });
}


/*
<Purpose>
  Adds options for the user to select up to g_num_hosts_remaining vessels.
<Arguments>
  dropdown:
    A SELECT object to add the new options to.
<Side Effects>
  Adds options that specify a vesselcount that selexor should get, up until
  g_num_hosts_remaining.
<Exceptions>
  None
<Returns>
  None

*/
var add_vessels_until_max = function(dropdown) {
  var selected = dropdown.val();
  if (selected === undefined)
    selected = 0;
  else
    // dropdown.val() returns a string, we need an int value
    selected = parseInt(selected)

  for (var option_no = dropdown.children().length; option_no < g_num_hosts_remaining + selected + 1; ++option_no) {
    var option = $('<option>').val(option_no).text(option_no)
    dropdown.append(option);
  }
}


/*
<Purpose>
  Removes options larger than the selected option in the given dropdown menu.
<Arguments>
  dropdown:
    A SELECT object to remove the options from. The contained options should
    be in ascending order, starting from 0.
<Side Effects>
  Removes options that specify a vesselcount greater than the selected option's
  vesselcount.
<Exceptions>
  None
<Returns>
  None

*/
var remove_targets_until_selected = function(dropdown) {
  var selected = dropdown.val()
  while (dropdown.children().length - 1 > selected) {
    dropdown.children().last().remove()
  }
}

/*
<Purpose>
  Gets the current snapshot of the user's authentication data.
<Arguments>
  None
<Side Effects>
  None
<Exceptions>
  None
<Returns>
  The userdict representing the current authentication data.

*/
var remove_group = function(groupid) {
  var row = g_groupid_to_row[groupid]
  if (!isNaN(g_num_hosts_remaining))
    g_num_hosts_remaining += parseInt($(row).find(".vessel_selection").val())

  update_remaining_host_count()
  delete g_groupid_to_row[groupid]

  $(row).remove();
}



/*
<Purpose>
  Performs initial setup on the selexor form.
<Arguments>
  None
<Side Effects>
  Attaches event callbacks onto form elements:
    Username, API key
    Add New Group button
  Triggers initial error checking to start up in an invalid request state to
    prevent user from submitting an incomplete request.

<Exceptions>
  None
<Returns>
  None

*/
var setup_form = function() {
  g_auth_errors['username'] = true
  $("input#username_text")
    .addClass("auth_input")
    .bind("blur", function() {
        validate_username()
    })

  g_auth_errors['apikey'] = true
  $("input#apikey_text")
    .addClass("auth_input")
    .bind("blur", function() {
        validate_apikey()
    })

  $("#num_hosts_remaining")
    .bind('blur', function() {
      update_send_request_button()
  })

  // Prepare the port restriction selection
  var valid_ports = generate_numeric_list(63100, 63180)
  create_selection(valid_ports, valid_ports, $('#port_restriction_option'))

  // Enable port selection by default
  $('#port_restriction').prop('checked', true)

  $('.authentication_required').hide()

  $("input#new_group_button").bind('click', function() {
      create_new_group()
    })
  $("input#vessel_acquire_button").bind('click', function() {
      send_host_request()
    })
  $("input#authentication_button").bind('click', function() {
      authenticate()
    })
}

/*
<Purpose>
  Checks the current username to make sure it is valid. The username is valid
  if there is at least one character in the username field.
<Arguments>
  None
<Side Effects>
  If the username is invalid, the username field will be marked red, and a
  tooltip indicating the error will be shown.
  Otherwise, its color will be restored and no tooltip will be shown.

  The 'username' error key will be added/removed to g_auth_errors
  depending on the validity of the username field.
<Exceptions>
  None
<Returns>
  None

*/
function validate_username() {
  var username_text = $('input#username_text')
  var username = username_text.val()
  var errorString = "";

  if (!username.length)
    errorString += "Username must be defined!\n";

  if (errorString.length) {
    username_text.show_error(errorString);
    g_auth_errors['username'] = true
  } else {
    username_text.clear_error();
    delete g_auth_errors['username']
  }
}


/*
<Purpose>
  Checks the current apikey to make sure it is valid. The apikey is valid
  if it is defined, and has 32 characters in it.
<Arguments>
  None
<Side Effects>
  If the apikey is invalid, the username field will be marked red, and a
  tooltip indicating the error will be shown.
  Otherwise, its color will be restored and no tooltip will be shown.

  The 'apikey' error key will be added/removed to g_auth_errors
  depending on the validity of the apikey field.
<Exceptions>
  None
<Returns>
  None

*/
function validate_apikey() {
  var apikey_text = $('input#apikey_text')
  var apikey = apikey_text.val()

  var errorString = "";
  if (apikey.length != 32)
    errorString += "API Key must be 32 alphanumeric characters!\n";

  if (errorString.length) {
    apikey_text.show_error(errorString);
    g_auth_errors['apikey'] = true
  } else {
    apikey_text.clear_error();
    delete g_auth_errors['apikey']
  }
}


function update_send_request_button() {
  var errorString = ''
  if (g_max_hosts == g_num_hosts_remaining || g_max_hosts == '?')
    errorString += "You cannot send a request until you specify to acquire some hosts!<br />If you don't have enough hosts, consider releasing some from the Clearinghouse, or donate more resources. <br />You may re-authenticate to refresh your remaining vessel count."
  if (errorString.length) {
    g_request_errors['vesselcount'] = true
    $('#vessel_acquire_button').show_error(errorString, null)
  } else {
    delete g_request_errors['vesselcount']
    $('#vessel_acquire_button').clear_error(null)
  }
  if (isEmpty(g_request_errors))
    $("#acquire_capsule").show()
        .addClass('request_ready')
  else
    $("#acquire_capsule").hide()
        .removeClass('request_ready')
}


var attach_tooltips = function() {
  $("#username_tooltip").addTooltip("This is your Username that is associated with the Clearinghouse.");
  $("#apikey_tooltip").addTooltip("A 32-character API key is required to allow SeleXor to perform actions on your behalf. <br />You can obtain this by visiting your Clearinghouse by clicking on this tooltip.");
}


function preload() {
  var progress_img = document.createElement('img')
  progress_img.src = PROGRESS_IMAGE_SRC
  progress_img.style.display = 'none'
  document.getElementsByTagName('body')[0].appendChild(progress_img)
}



/* Generates a list of numbers, inclusive of the lower bound, exclusive the upper bound */
function generate_numeric_list(start, end) {
  var list = new Array();
  for (var i = start; i < end; ++i) {
    list.push(i)
  }
  return list;
}




$(document).ready( function() {
  //preload();
  setup_form();
  attach_tooltips();
  init();
});