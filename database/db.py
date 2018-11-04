# NoSql database written in python

import socket

HOST = 'localhost'
PORT = 50505
SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Dictionary to store stats

STATS = {
    'PUT': {'success': 0, 'error': 0},
    'GET': {'success': 0, 'error': 0},
    'GETLIST': {'success': 0, 'error': 0},
    'PUTLIST': {'success': 0, 'error': 0},
    'INCREMENT': {'success': 0, 'error': 0},
    'APPEND': {'success': 0, 'error': 0},
    'DELETE': {'success': 0, 'error': 0},
    'STATS': {'success': 0, 'error': 0},
}



# Data store
DATA = {}


#
# Command  input examples
# "PUT;foo;1;INT"
#
# "GET;foo;;"
#
# "PUTLIST;bar;a,b,c;LIST"
#
# "APPEND;bar;d;STRING
#
# "GETLIST;bar;;"
#
# "STATS;;;"
#
# "INCREMENT;foo;;"
#
# "DELETE;foo;;"



# command output examples
#
# "True;Key [foo] set to [1]"
#
# "True;1"
#
# "True;Key [bar] set to [['a', 'b', 'c']]"
#
# "True;Key [bar] had value [d] appended"
#
# "True;['a', 'b', 'c', 'd']
#
# "True;{'PUTLIST': {'success': 1, 'error': 0}, 'STATS': {'success': 0, 'error': 0}, 'INCREMENT': {'success': 0, 'error': 0}, 'GET': {'success': 0, 'error': 0}, 'PUT': {'success': 0, 'error': 0}, 'GETLIST': {'success': 1, 'error': 0}, 'APPEND': {'success': 1, 'error': 0}, 'DELETE': {'success': 0, 'error': 0}}"

def parse_message(data):
    # command will return a tupple which contains
    # command,key,value,type
    command,key,value,type = data.strip().split(';')
    if type :
        if type == 'LIST':
            value = value.split(',')
        elif type == 'INT' :
            value = int(value)
        else:
            value = str(value)
    else :
        value = None

    return command,key,value


def update_stats(command, status):
    if status :
        STATS[command]['success'] +=1
    else :
        STATS[command]['error'] +=1


def handle_put(key,value):
    """Return a tuple containing True and the message
        to send back to the client."""
    DATA[key] = value
    return (True,'key [{}] set to value [{}]'.format(key,value))

def handle_get(key):
    if key not in DATA :
        return (False,'ERROR: key [{}] not found'.format(key))
    else:
        return (True,DATA[key])

def handle_putlist(key, value):
    return handle_put(key,value)


def handle_getlist(key):
     return_value = exists,value=handle_get(key)
     if not exists:
         return return_value
     elif not isinstance(value,list):
         return (
             False,
             'Error: key [{}] contains non list value ([{}])'.format(key,value)
         )
     else:
         return return_value

def handle_increment(key):
    return_value = exists,value =handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(value,int):
        return(
            False,
            'ERROR: Key [{}] contains non-int value ([{}])'.format(key, value)
        )
    else:
        DATA[key] = value +1
        return (True, 'Key [{}] incremented'.format(key))


def handle_append(key,value):
    return_value = exists, list_value = handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(list_value,list):
        return (
            False,
            'ERROR: Key [{}] contains non-list value ([{}])'.format(key, value)
        )
    else:
        DATA[key].append(value)
        return (True, 'Key [{}] had value [{}] appended'.format(key, value))


def handle_delete(key):
    if key not in DATA:
        return (
            False,
            'ERROR: Key [{}] not found and could not be deleted'.format(key)
        )
    else:
        del DATA[key]

def handle_stats():
    return (True,str(STATS))


# command dictionaries which contains all comands supported by database
COMMAND_HANDLERS = {
    'PUT': handle_put,
    'GET': handle_get,
    'GETLIST': handle_getlist,
    'PUTLIST': handle_putlist,
    'INCREMENT': handle_increment,
    'APPEND': handle_append,
    'DELETE': handle_delete,
    'STATS': handle_stats,
}

def main():
    # Main entry point for the script
    SOCKET.bind((HOST, PORT))
    SOCKET.listen(1)
    while 1:
        connection, address = SOCKET.accept()
        print 'New connection form at address : [{}]'.format(address)
        data = connection.recv(4096)
        command, key, value = parse_message(data)
        if command == 'STATS':
            respone = handle_stats()
        elif command in (
                'GET',
                'GETLIST',
                'INCREMENT',
                'DELETE'
        ):
            respone = COMMAND_HANDLERS[command](key)
        elif command in (
                'PUT',
                'PUTLIST',
                'APPEND',
        ):
            respone = COMMAND_HANDLERS[command](key, value)
        else :
            respone = (False, 'Unknown command type [{}]'.format(command))

            # All commands type are handled Now send the response
        update_stats(command, respone[0])
        connection.sendall('{};{}'.format(respone[0], respone[1]))
        connection.close()


if __name__ == '__main__':
    main()
