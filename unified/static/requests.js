function XML_HTTP()
{
    var xmlhttp;

    if (window.XMLHttpRequest)
      {// code for IE7+, Firefox, Chrome, Opera, Safari
      xmlhttp=new XMLHttpRequest();
      }
    else
      {// code for IE6, IE5
      xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
      }
    return xmlhttp;
}

function state_changed_callback()
{
    if (this.readyState==4 && this.status==200)
        this.data_received_callback();
}

function data_received_callback()
{
        var c = this.data_receiver;
        this.data_receiver= null;
        var parsed = null;
        var error = false;
        try         {   
                        parsed = JSON.parse(this.responseText); 
                    }
        catch(err)  {
                        error = true;
                        if( c.data_error )
                        {   c.data_error(this, "JSON parse error"); }
                    }

        if( !error )
            c.data_received(parsed);
}

function XMLRequest(url, receiver, cacheable)
{
    var http_request = XML_HTTP();
    http_request.data_receiver = receiver;
    http_request.data_received_callback = data_received_callback;
    http_request.onreadystatechange = state_changed_callback;
    if( cacheable != null && !cacheable )
    {
        if( url.indexOf("?") < 0 )
            url += "?_=" + Math.random();
        else
            url += "&_=" + Math.random();
    }
    http_request.open("GET", url, true);
    http_request.send();
    return http_request;
}            
